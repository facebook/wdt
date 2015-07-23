/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
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

void WdtBase::abort(const ErrorCode abortCode) {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode == VERSION_MISMATCH && abortCode_ != OK) {
    // VERSION_MISMATCH is the lowest priority abort code. If the abort code is
    // anything other than OK, we should not override it
    return;
  }
  LOG(WARNING) << "Setting the abort code " << abortCode;
  abortCode_ = abortCode;
}

void WdtBase::clearAbort() {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode_ != VERSION_MISMATCH) {
    // We do no clear abort code unless it is VERSION_MISMATCH
    return;
  }
  LOG(WARNING) << "Clearing the abort code";
  abortCode_ = OK;
}

void WdtBase::setAbortChecker(IAbortChecker const *checker) {
  abortChecker_ = checker;
}

ErrorCode WdtBase::getCurAbortCode() {
  // external check, if any:
  if (abortChecker_ && abortChecker_->shouldAbort()) {
    return ABORTED_BY_APPLICATION;
  }
  folly::RWSpinLock::ReadHolder guard(abortCodeLock_);
  // internal check:
  return abortCode_;
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
