/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <wdt/Reporting.h>
#include <wdt/ErrorCodes.h>
#include <wdt/util/ThreadsController.h>
#include <thread>
#include <memory>

namespace facebook {
namespace wdt {

class ThreadsController;

/// Common functionality and settings between SenderThread and ReceiverThread
class WdtThread {
 public:
  /// Constructor for wdt thread
  WdtThread(int threadIndex, int protocolVersion, ThreadsController *controller)
      : threadIndex_(threadIndex), threadProtocolVersion_(protocolVersion) {
    controller_ = controller;
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

  /// Index of this thread with respect to other threads
  const int threadIndex_;

  /// Copy of the protocol version that might be changed
  int threadProtocolVersion_;

  /// Transfer stats for this thread
  TransferStats threadStats_{true};

  /// Perf stats report for this thread
  PerfStatReport perfReport_;

  /// Thread controller for all the sender threads
  ThreadsController *controller_{nullptr};

  /// Pointer to the std::thread executing the transfer
  std::unique_ptr<std::thread> threadPtr_{nullptr};
};
}
}
