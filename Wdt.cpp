#include <wdt/Wdt.h>
#include <wdt/util/WdtFlags.h>

using std::string;

namespace facebook {
namespace wdt {

// this must be called first and exactly once:
Wdt &Wdt::initializeWdt(const std::string &appName) {
  static bool doGlobalFlagsInit = true;
  if (doGlobalFlagsInit) {
    WdtFlags::initializeFromFlags(WdtOptions::getMutable());
    doGlobalFlagsInit = false;
  }
  Wdt &res = getWdtInternal(appName, []() -> Wdt * { return new Wdt(); });
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
  resourceController_->getWdtThrottler()->setThrottlerRates(options_);
  return OK;
}

// this can be called many times after initializeWdt()
Wdt &Wdt::getWdt(const std::string &appName) {
  Wdt &res = getWdtInternal(appName, nullptr);
  if (!res.initDone_) {
    WLOG(ERROR) << "Called getWdt() before/without initializeWdt()";
    WDT_CHECK(false) << "Must call initializeWdt() once before getWdt()";
  }
  return res;
}

ErrorCode Wdt::printWdtOptions(std::ostream &out) {
  WdtFlags::printOptions(out, options_);
  return OK;
}

Wdt::Wdt() {
  WdtFlags::initializeFromFlags(options_);
  resourceController_ = folly::make_unique<WdtResourceController>(options_);
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
  // Allow more than 1 destination per host by using the host:firstport
  std::string secondKey;
  folly::toAppend(req.hostName, ":", req.ports[0], &secondKey);
  ErrorCode errCode =
      resourceController_->createSender(wdtNamespace, secondKey, req, sender);
  if (errCode == ALREADY_EXISTS && terminateExistingOne) {
    WLOG(WARNING) << "Found pre-existing sender for " << wdtNamespace << " "
                  << secondKey << " aborting it and making a new one";
    if (sender->getTransferRequest() == req) {
      WLOG(WARNING) << "No need to recreate same sender with key: " << secondKey
                    << " TransferRequest: " << req;
      return errCode;
    }
    sender->abort(ABORTED_BY_APPLICATION);
    // This may log an error too
    resourceController_->releaseSender(wdtNamespace, secondKey);
    // Try#2
    errCode =
        resourceController_->createSender(wdtNamespace, secondKey, req, sender);
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
  resourceController_->releaseSender(wdtNamespace, secondKey);
  WLOG(INFO) << "wdtSend for " << wdtNamespace << " " << secondKey
             << " ended with " << errorCodeToStr(ret);
  return ret;
}

ErrorCode Wdt::wdtReceiveStart(const std::string &wdtNamespace,
                               WdtTransferRequest &req,
                               const std::string &identifier,
                               std::shared_ptr<IAbortChecker> abortChecker) {
  if (req.errorCode != OK) {
    WLOG(ERROR) << "Transfer request namespace:" << wdtNamespace
                << " identifier:" << identifier
                << " error:" << errorCodeToStr(req.errorCode);
    return req.errorCode;
  }

  ReceiverPtr receiver;
  ErrorCode errCode = resourceController_->createReceiver(
      wdtNamespace, identifier, req, receiver);
  if (errCode != OK) {
    WLOG(ERROR) << "Failed to create receiver " << errorCodeToStr(errCode)
                << " " << wdtNamespace << " " << identifier;
    req.errorCode = errCode;
    return errCode;
  }

  wdtSetAbortSocketCreatorAndReporter(wdtNamespace, receiver.get(), req,
                                      abortChecker);

  req = receiver->init();
  if (req.errorCode != OK) {
    WLOG(ERROR) << "Couldn't init receiver with request for " << wdtNamespace
                << " " << identifier;
    return req.errorCode;
  }
  errCode = receiver->transferAsync();
  WLOG(INFO) << "wdtReceiveStart for " << wdtNamespace << " " << identifier
             << " : " << errorCodeToStr(errCode);
  req.errorCode = errCode;
  return errCode;
}

ErrorCode Wdt::wdtReceiveFinish(const std::string &wdtNamespace,
                                const std::string &identifier) {
  ReceiverPtr receiver =
      resourceController_->getReceiver(wdtNamespace, identifier);
  if (receiver == nullptr) {
    WLOG(ERROR) << "Failed to get receiver " << errorCodeToStr(NOT_FOUND) << " "
                << wdtNamespace << " " << identifier;
    return NOT_FOUND;
  }
  auto report = receiver->finish();
  ErrorCode errCode = report->getSummary().getErrorCode();
  WLOG(INFO) << "wdtReceiveFinish for " << wdtNamespace << " " << identifier
             << " ended with " << errorCodeToStr(errCode);
  resourceController_->releaseReceiver(wdtNamespace, identifier);
  return errCode;
}

ErrorCode Wdt::wdtSetAbortSocketCreatorAndReporter(
    const std::string &, WdtBase *target, const WdtTransferRequest &,
    std::shared_ptr<IAbortChecker> abortChecker) {
  if (abortChecker.get() != nullptr) {
    target->setAbortChecker(abortChecker);
  }
  return OK;
}

WdtOptions &Wdt::getWdtOptions() {
  return options_;
}

static std::unordered_map<std::string, std::unique_ptr<Wdt>> s_wdtMap;
static std::mutex s_mutex;

// private version
Wdt &Wdt::getWdtInternal(const std::string &appName,
                         std::function<Wdt *()> factory) {
  std::lock_guard<std::mutex> lock(s_mutex);
  auto it = s_wdtMap.find(appName);
  if (it != s_wdtMap.end()) {
    return *(it->second);
  }
  WDT_CHECK(factory) << "Must call initializeWdt() once before getWdt() "
                     << appName;
  Wdt *wdtPtr = factory();
  std::unique_ptr<Wdt> wdt(wdtPtr);
  s_wdtMap.emplace(appName, std::move(wdt));
  return *wdtPtr;
}

void Wdt::releaseWdt(const std::string &appName) {
  LOG(INFO) << "Releasing WDT for " << appName;
  std::lock_guard<std::mutex> lock(s_mutex);
  auto it = s_wdtMap.find(appName);
  if (it == s_wdtMap.end()) {
    LOG(ERROR) << appName << " not found in releaseWdt";
    return;
  }
  s_wdtMap.erase(it);
}
}
}  // namespaces
