/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtBase.h>
#include <wdt/WdtTransferRequest.h>
#include <random>
using namespace std;

namespace facebook {
namespace wdt {

WdtBase::WdtBase() : abortCheckerCallback_(this), sys_(&System::getDefault()) {
}

WdtBase::~WdtBase() {
  abortChecker_ = nullptr;
  sys_ = nullptr;
  delete threadsController_;
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

void WdtBase::setAbortChecker(const std::shared_ptr<IAbortChecker>& checker) {
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
    std::unique_ptr<ProgressReporter>& progressReporter) {
  progressReporter_ = std::move(progressReporter);
}

void WdtBase::setThrottler(std::shared_ptr<Throttler> throttler) {
  VLOG(2) << "Setting an external throttler";
  throttler_ = throttler;
}

std::shared_ptr<Throttler> WdtBase::getThrottler() const {
  return throttler_;
}

void WdtBase::setTransferId(const std::string& transferId) {
  transferRequest_.transferId = transferId;
  LOG(INFO) << "Setting transfer id " << transferId;
}

void WdtBase::setProtocolVersion(int64_t protocol) {
  WDT_CHECK(protocol > 0) << "Protocol version can't be <= 0 " << protocol;
  int negotiatedPv = Protocol::negotiateProtocol(protocol);
  if (negotiatedPv != protocol) {
    LOG(WARNING) << "Negotiated protocol version " << protocol << " -> "
                 << negotiatedPv;
  }
  protocolVersion_ = negotiatedPv;
  LOG(INFO) << "using wdt protocol version " << protocolVersion_;
}

int WdtBase::getProtocolVersion() const {
  return protocolVersion_;
}

std::string WdtBase::getTransferId() {
  return transferRequest_.transferId;
}

WdtBase::TransferStatus WdtBase::getTransferStatus() {
  std::lock_guard<std::mutex> lock(mutex_);
  return transferStatus_;
}

void WdtBase::setTransferStatus(TransferStatus transferStatus) {
  std::lock_guard<std::mutex> lock(mutex_);
  transferStatus_ = transferStatus;
  if (transferStatus_ == THREADS_JOINED) {
    conditionFinished_.notify_one();
  }
}

bool WdtBase::isStale() {
  TransferStatus status = getTransferStatus();
  return (status == FINISHED || status == THREADS_JOINED);
}

void WdtBase::configureThrottler() {
  WDT_CHECK(!throttler_);
  VLOG(1) << "Configuring throttler options";
  const auto& options = WdtOptions::get();
  throttler_ = Throttler::makeThrottler(options);
  if (throttler_) {
    LOG(INFO) << "Enabling throttling " << *throttler_;
  } else {
    LOG(INFO) << "Throttling not enabled";
  }
}

string WdtBase::generateTransferId() {
  static std::default_random_engine randomEngine{std::random_device()()};
  static std::mutex mutex;
  string transferId;
  {
    std::lock_guard<std::mutex> lock(mutex);
    transferId = to_string(randomEngine());
  }
  VLOG(1) << "Generated a transfer id " << transferId;
  return transferId;
}
}
}  // namespace facebook::wdt
