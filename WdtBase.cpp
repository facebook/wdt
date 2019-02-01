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

// TODO: force callers to pass options in
WdtBase::WdtBase() : abortCheckerCallback_(this) {
  options_.copyInto(WdtOptions::get());
}

void WdtBase::setWdtOptions(const WdtOptions& src) {
  options_.copyInto(src);
}

WdtBase::~WdtBase() {
  abortChecker_ = nullptr;
  delete threadsController_;
}

void WdtBase::abort(const ErrorCode abortCode) {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode == VERSION_MISMATCH && abortCode_ != OK) {
    // VERSION_MISMATCH is the lowest priority abort code. If the abort code is
    // anything other than OK, we should not override it
    return;
  }
  WLOG(WARNING) << "Setting the abort code " << abortCode;
  abortCode_ = abortCode;
}

void WdtBase::clearAbort() {
  folly::RWSpinLock::WriteHolder guard(abortCodeLock_);
  if (abortCode_ != VERSION_MISMATCH) {
    // We do no clear abort code unless it is VERSION_MISMATCH
    return;
  }
  WLOG(WARNING) << "Clearing the abort code";
  abortCode_ = OK;
}

void WdtBase::setAbortChecker(const std::shared_ptr<IAbortChecker>& checker) {
  abortChecker_ = checker;
}

ErrorCode WdtBase::getCurAbortCode() const {
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
  WVLOG(2) << "Setting an external throttler";
  throttler_ = throttler;
}

std::shared_ptr<Throttler> WdtBase::getThrottler() const {
  return throttler_;
}

void WdtBase::setTransferId(const std::string& transferId) {
  transferRequest_.transferId = transferId;
  WLOG(INFO) << "Setting transfer id " << transferId;
}

void WdtBase::negotiateProtocol() {
  int protocol = transferRequest_.protocolVersion;
  WDT_CHECK(protocol > 0) << "Protocol version can't be <= 0 " << protocol;
  int negotiatedPv = Protocol::negotiateProtocol(protocol);
  if (negotiatedPv != protocol) {
    WLOG(WARNING) << "Negotiated protocol version " << protocol << " -> "
                  << negotiatedPv;
  }
  transferRequest_.protocolVersion = negotiatedPv;
  WLOG(INFO) << "using wdt protocol version "
             << transferRequest_.protocolVersion;
}

int WdtBase::getProtocolVersion() const {
  return transferRequest_.protocolVersion;
}

void WdtBase::setProtocolVersion(int protocolVersion) {
  transferRequest_.protocolVersion = protocolVersion;
}

std::string WdtBase::getTransferId() {
  return transferRequest_.transferId;
}

const std::string& WdtBase::getDirectory() const {
  return transferRequest_.directory;
}

WdtTransferRequest& WdtBase::getTransferRequest() {
  return transferRequest_;
}

void WdtBase::checkAndUpdateBufferSize() {
  int64_t bufSize = options_.buffer_size;
  if (bufSize < Protocol::kMaxHeader) {
    bufSize = Protocol::kMaxHeader;
    WLOG(WARNING) << "Specified buffer size " << options_.buffer_size
                  << " less than " << Protocol::kMaxHeader << ", using "
                  << bufSize;
  }
  if (bufSize % kDiskBlockSize != 0) {
    int64_t alignedBufSize =
        ((bufSize + kDiskBlockSize - 1) / kDiskBlockSize) * kDiskBlockSize;
    WLOG(WARNING) << "Buffer size " << bufSize
                  << " not divisible by disk block size " << kDiskBlockSize
                  << ", changing it to " << alignedBufSize;
    bufSize = alignedBufSize;
  }
  options_.buffer_size = bufSize;
}

WdtBase::TransferStatus WdtBase::getTransferStatus() {
  std::lock_guard<std::mutex> lock(mutex_);
  return transferStatus_;
}

ErrorCode WdtBase::validateTransferRequest() {
  ErrorCode code = transferRequest_.errorCode;
  if (code != OK) {
    WLOG(ERROR) << "WDT object initiated with erroneous transfer request "
                << transferRequest_.getLogSafeString();
    return code;
  }
  if (transferRequest_.directory.empty() ||
      (transferRequest_.protocolVersion < 0) ||
      transferRequest_.ports.empty()) {
    WLOG(ERROR) << "Transfer request validation failed for wdt object "
                << transferRequest_.getLogSafeString();
    code = INVALID_REQUEST;
    transferRequest_.errorCode = code;
  }
  return code;
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

bool WdtBase::hasStarted() {
  TransferStatus status = getTransferStatus();
  return (status != NOT_STARTED);
}

void WdtBase::configureThrottler() {
  WDT_CHECK(!throttler_);
  WVLOG(1) << "Configuring throttler options";
  throttler_ = std::make_shared<Throttler>(options_.getThrottlerOptions());
  if (throttler_) {
    WLOG(INFO) << "Enabling throttling " << *throttler_;
  } else {
    WLOG(INFO) << "Throttling not enabled";
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
  WVLOG(1) << "Generated a transfer id " << transferId;
  return transferId;
}
}
}  // namespace facebook::wdt
