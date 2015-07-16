#include "WdtBase.h"
namespace facebook {
namespace wdt {
WdtTransferRequest::WdtTransferRequest(int startPort, int numPorts) {
  protocolVersion = Protocol::protocol_version;
  int portNum = startPort;
  for (int i = 0; i < numPorts; i++) {
    ports.push_back(portNum);
    if (startPort) {
      ++portNum;
    }
  }
}

WdtBase::WdtBase() : abortCheckerCallback_(this) {
}

WdtBase::~WdtBase() {
  abortChecker_ = nullptr;
}

void WdtBase::abort() {
  LOG(WARNING) << "Setting the abort flag";
  abortTransfer_.store(true);
}

void WdtBase::setAbortChecker(IAbortChecker const *checker) {
  abortChecker_ = checker;
}

bool WdtBase::wasAbortRequested() const {
  // external check, if any:
  if (abortChecker_ && abortChecker_->shouldAbort()) {
    return true;
  }
  // internal check:
  return abortTransfer_.load();
}

void WdtBase::setProgressReporter(
    std::unique_ptr<ProgressReporter> &progressReporter) {
  progressReporter_ = std::move(progressReporter);
}

void WdtBase::setThrottler(std::shared_ptr<Throttler> throttler) {
  VLOG(2) << "Setting an external throttler";
  throttler_ = throttler;
}

void WdtBase::setTransferId(const std::string &transferId) {
  transferId_ = transferId;
  LOG(INFO) << "Setting transfer id " << transferId_;
}

void WdtBase::setProtocolVersion(int64_t protocolVersion) {
  WDT_CHECK(Protocol::negotiateProtocol(protocolVersion) == protocolVersion)
      << "Can not support wdt version " << protocolVersion;
  protocolVersion_ = protocolVersion;
  LOG(INFO) << "using wdt protocol version " << protocolVersion_;
}

std::string WdtBase::getTransferId() {
  return transferId_;
}

void WdtBase::configureThrottler() {
  WDT_CHECK(!throttler_);
  VLOG(1) << "Configuring throttler options";
  const auto &options = WdtOptions::get();
  double avgRateBytesPerSec = options.avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = options.max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = options.throttler_bucket_limit * kMbToB;
  throttler_ = Throttler::makeThrottler(avgRateBytesPerSec, peakRateBytesPerSec,
                                        bucketLimitBytes,
                                        options.throttler_log_time_millis);
  if (throttler_) {
    LOG(INFO) << "Enabling throttling " << *throttler_;
  } else {
    LOG(INFO) << "Throttling not enabled";
  }
}
}
}  // namespace facebook::wdt
