/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <folly/lang/Bits.h>
#include <wdt/ErrorCodes.h>
#include <wdt/Protocol.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/util/ThreadsController.h>
#include <wdt/util/WdtSocket.h>
#include <memory>
#include <thread>

namespace facebook {
namespace wdt {

class ThreadsController;

/// Common functionality and settings between SenderThread and ReceiverThread
class WdtThread {
 public:
  /// Constructor for wdt thread
  WdtThread(const WdtOptions &options, int threadIndex, int port,
            int protocolVersion, ThreadsController *controller)
      : options_(options),
        port_(port),
        threadProtocolVersion_(protocolVersion) {
    controller_ = controller;
    threadCtx_ = std::make_unique<ThreadCtx>(
        options, /* allocate buffer */ true, threadIndex);
    const Buffer *buffer = threadCtx_->getBuffer();
    WDT_CHECK(buffer);
    buf_ = buffer->getData();
    bufSize_ = buffer->getSize();
    threadIndex_ = threadCtx_->getThreadIndex();
    lastHeartBeatTime_ = Clock::now();
  }
  /// Starts a thread which runs the wdt functionality
  void startThread();

  /// Get the perf stats of the transfer for this thread
  const PerfStatReport &getPerfReport() const;

  /// Initializes the wdt thread before starting
  virtual ErrorCode init() = 0;

  /// Conclude the thread transfer
  virtual ErrorCode finish();

  /// Moves the local stats into a new instance
  TransferStats moveStats();

  /// Get the transfer stats recorded by this thread
  const TransferStats &getTransferStats() const;

  /// Reset the wdt thread
  virtual void reset() = 0;

  /// Get the port this thread is running on
  virtual int getPort() const = 0;

  // TODO remove this function
  virtual int getNegotiatedProtocol() const {
    return threadProtocolVersion_;
  }

  virtual ~WdtThread();

 protected:
  /// The main entry point of the thread
  virtual void start() = 0;

  std::unique_ptr<ThreadCtx> threadCtx_{nullptr};

  /// buffer pointer. this points to the buffer in threadCtx_
  char *buf_{nullptr};

  /// buffer size. this is the size of buffer in threadCtx_
  int64_t bufSize_{0};

  /// reference to parent options
  const WdtOptions &options_;

  /// Index of this thread with respect to other threads
  int threadIndex_;

  /// port number for this thread
  const int port_;

  /// Copy of the protocol version that might be changed
  int threadProtocolVersion_;

  /// whether heart-beat is enabled
  bool enableHeartBeat_{false};

  Clock::time_point lastHeartBeatTime_;

  /// possible footer types
  enum FooterType {
    NO_FOOTER,
    CHECKSUM_FOOTER,
  };

  FooterType footerType_{NO_FOOTER};

  /// Transfer stats for this thread
  TransferStats threadStats_{true};

  /// Thread controller for all the sender threads
  ThreadsController *controller_{nullptr};

  /// Pointer to the std::thread executing the transfer
  std::unique_ptr<std::thread> threadPtr_{nullptr};
};
}
}
