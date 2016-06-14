#include <wdt/Wdt.h>
#include <wdt/util/WdtFlags.h>

using std::string;

namespace facebook {
namespace wdt {

// this must be called first and exactly once:
Wdt &Wdt::initializeWdt(const std::string &appName) {
  Wdt &res = getWdtInternal(appName);
  // TODO this should return the options
  WdtFlags::initializeFromFlags();
  res.initializeWdtInternal(appName);
  // At fb we do this for services - that's floody for cmd line though
  // res.printWdtOptions(WLOG(INFO));
  return res;
}

ErrorCode Wdt::initializeWdtInternal(const std::string &appName) {
  WLOG(INFO) << "One time initialization of WDT for " << appName;
  if (initDone_) {
    WLOG(ERROR) << "Called initializeWdt() more than once";
    return ERROR;
  }
  appName_ = appName;
  initDone_ = true;
  resourceController_.getThrottler()->setThrottlerRates(options_);
  return OK;
}

// this can be called many times after initializeWdt()
Wdt &Wdt::getWdt(const std::string &appName) {
  Wdt &res = getWdtInternal(appName);
  if (!res.initDone_) {
    WLOG(ERROR) << "Called getWdt() before/without initializeWdt()";
    WDT_CHECK(false) << "Must call initializeWdt() once before getWdt()";
  }
  return res;
}

ErrorCode Wdt::printWdtOptions(std::ostream &out) {
  // TODO: should print this object's options instead
  WdtFlags::printOptions(out);
  return OK;
}

ErrorCode Wdt::wdtSend(const std::string &wdtNamespace,
                       const WdtTransferRequest &req,
                       std::shared_ptr<IAbortChecker> abortChecker,
                       bool terminateExistingOne) {
  if (req.errorCode != OK) {
    WLOG(ERROR) << "Transfer request error " << errorCodeToStr(req.errorCode);
    return req.errorCode;
  }
  // Protocol issues will/should be flagged as error when we call createSender

  // try to create sender
  SenderPtr sender;
  // TODO should be using recoverid
  const std::string secondKey = req.hostName;
  ErrorCode errCode =
      resourceController_.createSender(wdtNamespace, secondKey, req, sender);
  if (errCode == ALREADY_EXISTS && terminateExistingOne) {
    WLOG(WARNING) << "Found pre-existing sender for " << wdtNamespace << " "
                  << secondKey << " aborting it and making a new one";
    sender->abort(ABORTED_BY_APPLICATION);
    // This may log an error too
    resourceController_.releaseSender(wdtNamespace, secondKey);
    // Try#2
    errCode =
        resourceController_.createSender(wdtNamespace, secondKey, req, sender);
  }
  if (errCode != OK) {
    WLOG(ERROR) << "Failed to create sender " << errorCodeToStr(errCode) << " "
                << wdtNamespace << " " << secondKey;
    return errCode;
  }
  wdtSetAbortSocketCreatorAndReporter(wdtNamespace, sender.get(), req,
                                      abortChecker);

  auto validatedReq = sender->init();
  if (validatedReq.errorCode != OK) {
    WLOG(ERROR) << "Couldn't init sender with request for " << wdtNamespace
                << " " << secondKey;
    return validatedReq.errorCode;
  }
  auto transferReport = sender->transfer();
  ErrorCode ret = transferReport->getSummary().getErrorCode();
  resourceController_.releaseSender(wdtNamespace, secondKey);
  WLOG(INFO) << "wdtSend for " << wdtNamespace << " " << secondKey << " "
             << " ended with " << errorCodeToStr(ret);
  return ret;
}

ErrorCode Wdt::wdtSetAbortSocketCreatorAndReporter(
    const std::string &, Sender *sender, const WdtTransferRequest &,
    std::shared_ptr<IAbortChecker> abortChecker) {
  if (abortChecker.get() != nullptr) {
    sender->setAbortChecker(abortChecker);
  }
  return OK;
}

WdtOptions &Wdt::getWdtOptions() {
  return options_;
}

// private version
Wdt &Wdt::getWdtInternal(const std::string &appName) {
  static std::unordered_map<std::string, std::unique_ptr<Wdt>> s_wdtMap;
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);
  auto it = s_wdtMap.find(appName);
  if (it != s_wdtMap.end()) {
    return *(it->second);
  }
  Wdt *wdtPtr = new Wdt(WdtOptions::getMutable());
  std::unique_ptr<Wdt> wdt(wdtPtr);
  s_wdtMap.emplace(appName, std::move(wdt));
  return *wdtPtr;
}
}
}  // namespaces
