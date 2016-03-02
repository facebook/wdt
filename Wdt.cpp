#include <wdt/Wdt.h>
#include <wdt/util/WdtFlags.h>

#include <wdt/WdtResourceController.h>

using std::string;

namespace facebook {
namespace wdt {

// this must be called first and exactly once:
Wdt &Wdt::initializeWdt(const std::string &appName) {
  Wdt &res = getWdtInternal();
  res.initializeWdtInternal(appName);
  WdtFlags::initializeFromFlags();
  // At fb we do this for services - that's floody for cmd line though
  // res.printWdtOptions(LOG(INFO));
  return res;
}

ErrorCode Wdt::initializeWdtInternal(const std::string &appName) {
  LOG(INFO) << "One time initialization of WDT for " << appName;
  if (initDone_) {
    LOG(ERROR) << "Called initializeWdt() more than once";
    return ERROR;
  }
  appName_ = appName;
  initDone_ = true;
  // TODO this should return the options
  return OK;
}

ErrorCode Wdt::applySettings() {
  WdtResourceController::get()->setThrottler(
      Throttler::makeThrottler(options_));
  settingsApplied_ = true;
  return OK;
}

// this can be called many times after initializeWdt()
Wdt &Wdt::getWdt() {
  Wdt &res = getWdtInternal();
  if (!res.initDone_) {
    LOG(ERROR) << "Called getWdt() before/without initializeWdt()";
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
  if (!settingsApplied_) {
    applySettings();
  }

  if (req.errorCode != OK) {
    LOG(ERROR) << "Transfer request error " << errorCodeToStr(req.errorCode);
    return req.errorCode;
  }
  // Protocol issues will/should be flagged as error when we call createSender

  // try to create sender
  SenderPtr sender;
  auto wdtController = WdtResourceController::get();
  // TODO should be using recoverid
  const std::string secondKey = req.hostName;
  ErrorCode errCode =
      wdtController->createSender(wdtNamespace, secondKey, req, sender);
  if (errCode == ALREADY_EXISTS && terminateExistingOne) {
    LOG(WARNING) << "Found pre-existing sender for " << wdtNamespace << " "
                 << secondKey << " aborting it and making a new one";
    sender->abort(ABORTED_BY_APPLICATION);
    // This may log an error too
    wdtController->releaseSender(wdtNamespace, secondKey);
    // Try#2
    errCode = wdtController->createSender(wdtNamespace, secondKey, req, sender);
  }
  if (errCode != OK) {
    LOG(ERROR) << "Failed to create sender " << errorCodeToStr(errCode) << " "
               << wdtNamespace << " " << secondKey;
    return errCode;
  }
  wdtSetAbortSocketCreatorAndReporter(wdtNamespace, sender.get(), req,
                                      abortChecker);

  auto validatedReq = sender->init();
  if (validatedReq.errorCode != OK) {
    LOG(ERROR) << "Couldn't init sender with request for " << wdtNamespace
               << " " << secondKey;
    return validatedReq.errorCode;
  }
  auto transferReport = sender->transfer();
  ErrorCode ret = transferReport->getSummary().getErrorCode();
  wdtController->releaseSender(wdtNamespace, secondKey);
  LOG(INFO) << "wdtSend for " << wdtNamespace << " " << secondKey << " "
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
Wdt &Wdt::getWdtInternal() {
  static Wdt s_wdtInstance{WdtOptions::getMutable()};
  return s_wdtInstance;
}
}
}  // namespaces
