/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <folly/synchronization/RWSpinLock.h>
#include <wdt/AbortChecker.h>
#include <wdt/ErrorCodes.h>
#include <wdt/Protocol.h>
#include <wdt/Reporting.h>
#include <wdt/Throttler.h>
#include <wdt/WdtOptions.h>
#include <wdt/WdtThread.h>
#include <wdt/util/DirectorySourceQueue.h>
#include <wdt/util/EncryptionUtils.h>
#include <wdt/util/ThreadsController.h>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace wdt {

/**
 * Shared code/functionality between Receiver and Sender
 * TODO: check if more of Receiver/Sender should move here
 */
class WdtBase {
 public:
  /// Constructor
  WdtBase();

  /**
   * Does the setup before start, returns the transfer request
   * that corresponds to the information relating to the sender
   * The transfer request has error code set should there be an error
   */
  virtual const WdtTransferRequest& init() = 0;

  /// Sets other options than global/singleton ones - call this before init()
  void setWdtOptions(const WdtOptions& src);

  /// Destructor
  virtual ~WdtBase();

  /// Transfer can be marked to abort and threads will eventually
  /// get aborted after this method has been called based on
  /// whether they are doing read/write on the socket and the timeout for the
  /// socket. Push mode for abort.
  void abort(ErrorCode abortCode);

  /// clears abort flag
  void clearAbort();

  /**
   * Returns a reference to the copy of WdtOptions held by this object.
   * Changes should only be made before init() is called, not after.
   */
  WdtOptions& getWdtOptions() {
    return options_;
  }

  /**
   * sets an extra external call back to check for abort
   * can be for instance extending IAbortChecker with
   * bool checkAbort() {return atomicBool->load();}
   * see wdtCmdLine.cpp for an example.
   */
  void setAbortChecker(const std::shared_ptr<IAbortChecker>& checker);

  /// threads can call this method to find out
  /// whether transfer has been marked from abort
  ErrorCode getCurAbortCode() const;

  /// Wdt objects can report progress. Setter for progress reporter
  /// defined in Reporting.h
  void setProgressReporter(std::unique_ptr<ProgressReporter>& progressReporter);

  /// Set throttler externally. Should be set before any transfer calls
  void setThrottler(std::shared_ptr<Throttler> throttler);

  /// Sets the transferId for this transfer
  void setTransferId(const std::string& transferId);

  ///  Get the protocol version of the transfer
  int getProtocolVersion() const;

  /// Sets protocol version to use
  void setProtocolVersion(int protocolVersion);

  /// Get the transfer id of the object
  std::string getTransferId();

  /// Get the transfer request
  WdtTransferRequest& getTransferRequest();

  /// Finishes the wdt object and returns a report
  virtual std::unique_ptr<TransferReport> finish() = 0;

  /// Method to transfer the data. Doesn't block and
  /// returns with the status
  virtual ErrorCode transferAsync() = 0;

  /// Basic setup for throttler using options
  void configureThrottler();

  /// Utility to generate a random transfer id
  static std::string generateTransferId();

  /// Get the throttler
  std::shared_ptr<Throttler> getThrottler() const;

  /// @return   Root directory
  const std::string& getDirectory() const;

  /// @param      whether the object is stale. If all the transferring threads
  ///             have finished, the object will marked as stale
  bool isStale();

  /// @return       Whether the transfer has started
  bool hasStarted();

  /// abort checker class passed to socket functions
  class AbortChecker : public IAbortChecker {
   public:
    explicit AbortChecker(WdtBase* wdtBase) : wdtBase_(wdtBase) {
    }

    bool shouldAbort() const override {
      return wdtBase_->getCurAbortCode() != OK;
    }

   private:
    WdtBase* wdtBase_;
  };

 protected:
  enum TransferStatus {
    NOT_STARTED,     // threads not started
    ONGOING,         // transfer is ongoing
    FINISHED,        // last running thread finished
    THREADS_JOINED,  // threads joined
  };

  /// Validate the transfer request
  virtual ErrorCode validateTransferRequest();

  /// @return current transfer status
  TransferStatus getTransferStatus();

  /// corrects buffer size if necessary
  void checkAndUpdateBufferSize();

  /// @param transferStatus   current transfer status
  void setTransferStatus(TransferStatus transferStatus);

  /// Sets the protocol version for the transfer
  void negotiateProtocol();

  /// Dumps performance statistics if enable_perf_stat_collection is true.
  virtual void logPerfStats() const = 0;

  /// Input/output transfer request
  WdtTransferRequest transferRequest_;

  /// Global throttler across all threads
  std::shared_ptr<Throttler> throttler_;

  /// Holds the instance of the progress reporter default or customized
  std::unique_ptr<ProgressReporter> progressReporter_;

  /// abort checker passed to socket functions
  AbortChecker abortCheckerCallback_;

  /// current transfer status
  TransferStatus transferStatus_{NOT_STARTED};

  /// Mutex which is shared between the parent thread, transferring threads and
  /// progress reporter thread
  std::mutex mutex_;

  /// Mutex for the management of this instance, specifically to keep the
  /// instance sane for multi threaded public API calls
  std::mutex instanceManagementMutex_;

  /// This condition is notified when the transfer is finished
  std::condition_variable conditionFinished_;

  /// Controller for wdt threads shared between base and threads
  ThreadsController* threadsController_{nullptr};

  /// Dump perf stats if notified
  ReportPerfSignalSubscriber reportPerfSignal_;

  /// Options/config used by this object
  WdtOptions options_;

 private:
  mutable folly::RWSpinLock abortCodeLock_;
  /// Internal and default abort code
  ErrorCode abortCode_{OK};
  /// Additional external source of check for abort requested
  std::shared_ptr<IAbortChecker> abortChecker_{nullptr};
};
}
}  // namespace facebook::wdt
