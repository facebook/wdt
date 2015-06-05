/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "Throttler.h"

#include <memory>
#include <string>

namespace facebook {
namespace wdt {

/**
 * Shared code/functionality between Receiver and Sender
 * TODO: a lot more code from sender/receiver should move here
 */
class WdtBase {
 public:
  /// Interface for external abort checks (pull mode)
  class IAbortChecker {
   public:
    virtual bool shouldAbort() const = 0;
    virtual ~IAbortChecker() {
    }
  };

  /// Constructor
  WdtBase() {
  }
  /// Destructor
  virtual ~WdtBase(){
    abortChecker_ = nullptr;
  }

  /// Transfer can be marked to abort and receiver threads will eventually
  /// get aborted after this method has been called based on
  /// whether they are doing read/write on the socket and the timeout for the
  /// socket. Push mode for abort.
  void abort() {
    LOG(WARNING) << "Setting the abort flag";
    abortTransfer_.store(true);
  }

  /**
   * sets an extra external call back to check for abort
   * can be for instance extending IAbortChecker with
   * bool checkAbort() {return atomicBool->load();}
   * see wdtCmdLine.cpp for an example.
   */
  void setAbortChecker(IAbortChecker const *checker) {
    abortChecker_ = checker;
  }

  /// Receiver threads can call this method to find out
  /// whether transfer has been marked to abort from outside the receiver
  bool wasAbortRequested() const {
    // external check, if any:
    if (abortChecker_ && abortChecker_->shouldAbort()) {
      return true;
    }
    // internal check:
    return abortTransfer_.load();
  }

  /**
   * @param progressReporter    progress reporter to be used. By default, wdt
   *                            uses a progress reporter which shows progress
   *                            percentage and current throughput. It also
   *                            visually shows progress. Sample report:
   *                            [=====>               ] 30% 2500.00 Mbytes/sec
   */
  void setProgressReporter(
      std::unique_ptr<ProgressReporter> &progressReporter) {
    progressReporter_ = std::move(progressReporter);
  }

  /// Set throttler externally. Should be set before any transfer calls
  void setThrottler(std::shared_ptr<Throttler> throttler) {
    VLOG(2) << "Setting an external throttler";
    throttler_ = throttler;
  }

 protected:
  /// Global throttler across all threads
  std::shared_ptr<Throttler> throttler_;

  /// Holds the instance of the progress reporter default or customized
  std::unique_ptr<ProgressReporter> progressReporter_;

 private:
  /// Internal and default abort transfer flag
  std::atomic<bool> abortTransfer_{false};
  /// Additional external source of check for abort requested
  IAbortChecker const *abortChecker_{nullptr};
};
}
}  // namespace facebook::wdt
