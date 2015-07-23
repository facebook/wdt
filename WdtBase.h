/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "Throttler.h"
#include "Protocol.h"
#include "DirectorySourceQueue.h"
#include <memory>
#include <string>
#include <vector>
#include <folly/RWSpinLock.h>

namespace facebook {
namespace wdt {
/**
 * Basic request for creating wdt objects
 * This request can be created for creating receivers
 * Same request can be used to create a counter part sender
 */
struct WdtTransferRequest {
  /// Transfer Id for the transfer should be same on both sender and receiver
  std::string transferId;
  /// Protocol version on sender and receiver
  int64_t protocolVersion;
  /// Ports on which receiver is listening / sender is sending to
  std::vector<int32_t> ports;
  /// Address on which receiver binded the ports / sender is sending data to
  std::string hostName;
  /// Directory to write the data to / read the data from
  std::string directory;
  /// Only required for the sender
  std::vector<FileInfo> fileInfo;
  /// Constructor
  WdtTransferRequest(int startPort, int numPorts);
};

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
  WdtBase();

  /// Destructor
  virtual ~WdtBase();

  /// Transfer can be marked to abort and threads will eventually
  /// get aborted after this method has been called based on
  /// whether they are doing read/write on the socket and the timeout for the
  /// socket. Push mode for abort.
  void abort(const ErrorCode abortCode);

  /// clears abort flag
  void clearAbort();

  /**
   * sets an extra external call back to check for abort
   * can be for instance extending IAbortChecker with
   * bool checkAbort() {return atomicBool->load();}
   * see wdtCmdLine.cpp for an example.
   */
  void setAbortChecker(IAbortChecker const *checker);

  /// threads can call this method to find out
  /// whether transfer has been marked from abort
  ErrorCode getCurAbortCode();

  /// Wdt objects can report progress. Setter for progress reporter
  /// defined in Reporting.h
  void setProgressReporter(std::unique_ptr<ProgressReporter> &progressReporter);

  /// Set throttler externally. Should be set before any transfer calls
  void setThrottler(std::shared_ptr<Throttler> throttler);

  /// Sets the transferId for this transfer
  void setTransferId(const std::string &transferId);

  /// Sets the protocol version for the transfer
  void setProtocolVersion(int64_t protocolVersion);

  /// Get the transfer id of the object
  std::string getTransferId();

  /// Finishes the wdt object and returns a report
  virtual std::unique_ptr<TransferReport> finish() = 0;

  /// Method to transfer the data. Doesn't block and
  /// returns with the status
  virtual ErrorCode transferAsync() = 0;

  /// Basic setup for throttler using options
  void configureThrottler();

 protected:
  /// Global throttler across all threads
  std::shared_ptr<Throttler> throttler_;

  /// Holds the instance of the progress reporter default or customized
  std::unique_ptr<ProgressReporter> progressReporter_;

  /// Unique id for the transfer
  std::string transferId_;

  /// protocol version to use, this is determined by negotiating protocol
  /// version with the other side
  int protocolVersion_{Protocol::protocol_version};

  /// abort checker class passed to socket functions
  class AbortChecker : public IAbortChecker {
   public:
    explicit AbortChecker(WdtBase *wdtBase) : wdtBase_(wdtBase) {
    }

    bool shouldAbort() const {
      return wdtBase_->getCurAbortCode() != OK;
    }

   private:
    WdtBase *wdtBase_;
  };

  /// abort checker passed to socket functions
  AbortChecker abortCheckerCallback_;

 private:
  folly::RWSpinLock abortCodeLock_;
  /// Internal and default abort code
  ErrorCode abortCode_{OK};
  /// Additional external source of check for abort requested
  IAbortChecker const *abortChecker_{nullptr};
};
}
}  // namespace facebook::wdt
