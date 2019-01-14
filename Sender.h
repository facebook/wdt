/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/WdtBase.h>
#include <wdt/util/ClientSocket.h>
#include <chrono>
#include <iostream>
#include <memory>

namespace facebook {
namespace wdt {

class SenderThread;
class TransferHistoryController;

enum ProtoNegotiationStatus {
  V_MISMATCH_WAIT,      // waiting for version mismatch to be processed
  V_MISMATCH_RESOLVED,  // version mismatch processed and was successful
  V_MISMATCH_FAILED,    // version mismatch processed and it failed
};

/**
 * The sender for the transfer. One instance of sender should only be
 * responsible for one transfer. For a second transfer you should make
 * another instance of the sender.
 * The object will not be destroyed till the transfer finishes. This
 * class is not thread safe.
 */
class Sender : public WdtBase {
 public:
  /// Creates a counter part sender for the receiver according to the details
  explicit Sender(const WdtTransferRequest &transferRequest);

  /// Setup before start (@see WdtBase.h)
  const WdtTransferRequest &init() override;

  /**
   * If the transfer has not finished, then it is aborted. finish() is called to
   * wait for threads to end.
   */
  ~Sender() override;

  /**
   * Joins on the threads spawned by start. This has to
   * be explicitly called when the caller expects to conclude
   * a transfer. This method can be called multiple times and is thread-safe.
   *
   * @return    transfer report
   */
  std::unique_ptr<TransferReport> finish() override;

  /**
   * API to initiate a transfer and return back to the context
   * from where it was called. Caller would have to call finish
   * to get the stats for the transfer
   */
  ErrorCode transferAsync() override;

  /**
   * A blocking call which will initiate a transfer based on
   * the configuration and return back the stats for the transfer
   *
   * @return    transfer report
   */
  std::unique_ptr<TransferReport> transfer();

  /// End time of the transfer
  Clock::time_point getEndTime();

  /// Get the destination sender is sending to
  /// @return     destination host-name
  const std::string &getDestination() const;

  /**
   * @param progressReportIntervalMillis_   interval(ms) between progress
   *                                        reports. A value of 0 indicates no
   *                                        progress reporting
   * TODO: move to base
   */
  void setProgressReportIntervalMillis(const int progressReportIntervalMillis);

  /// @return    minimal transfer report using transfer stats of the thread
  std::unique_ptr<TransferReport> getTransferReport();

  /// Interface to make socket
  class ISocketCreator {
   public:
    virtual std::unique_ptr<IClientSocket> makeClientSocket(
        ThreadCtx &threadCtx, const std::string &dest, const int port,
        const EncryptionParams &encryptionParams, int64_t ivChangeInterval,
        bool tls) = 0;

    virtual ~ISocketCreator() = default;
  };

  /**
   * Sets socket creator
   *
   * @param socketCreator   socket-creator to be used
   */
  void setSocketCreator(ISocketCreator *socketCreator);

 private:
  friend class SenderThread;
  friend class QueueAbortChecker;

  /// Validate the transfer request
  ErrorCode validateTransferRequest() override;

  /// Get the sum of all the thread transfer stats
  TransferStats getGlobalTransferStats() const;

  /// Returns true if file chunks need to be read
  bool isSendFileChunks() const;

  /// Returns true if file chunks been received by a thread
  bool isFileChunksReceived();

  /// Sender thread calls this method to set the file chunks info received
  /// from the receiver
  void setFileChunksInfo(std::vector<FileChunksInfo> &fileChunksInfoList);

  /// Abort checker passed to DirectoryQueue. If all the network threads finish,
  /// directory discovery thread is also aborted
  class QueueAbortChecker : public IAbortChecker {
   public:
    explicit QueueAbortChecker(Sender *sender) : sender_(sender) {
    }

    bool shouldAbort() const override {
      return (sender_->getTransferStatus() == FINISHED);
    }

   private:
    Sender *sender_;
  };

  /// Abort checker shared with the directory queue
  QueueAbortChecker queueAbortChecker_;

  /**
   * Internal API that triggers the directory thread, sets up the sender
   * threads and starts the transfer. Returns after the sender threads
   * have been spawned
   */
  ErrorCode start();

  /**
   * @param transferredSourceStats      Stats for the successfully transmitted
   *                                    sources
   * @param failedSourceStats           Stats for the failed sources
   */
  void validateTransferStats(
      const std::vector<TransferStats> &transferredSourceStats,
      const std::vector<TransferStats> &failedSourceStats);

  /**
   * Responsible for doing a periodic check.
   * 1. Takes a lock on the thread stats to make a summary
   * 2. Sends the progress report with the summary to the progress reporter
   *    which can be provided by the user
   */
  void reportProgress();

  void logPerfStats() const override;

  /// Pointer to DirectorySourceQueue which reads the srcDir and the files
  std::unique_ptr<DirectorySourceQueue> dirQueue_;
  /// Number of active threads, decremented every time a thread is finished
  int32_t numActiveThreads_{0};
  /// The interval at which the progress reporter should check for progress
  int progressReportIntervalMillis_;
  /// Socket creator used to optionally create different kinds of client socket
  ISocketCreator *socketCreator_{nullptr};
  /// Whether download resumption is enabled or not
  bool downloadResumptionEnabled_{false};
  /// Flags representing whether file chunks have been received or not
  bool fileChunksReceived_{false};
  /// Thread that is running the discovery of files using the dirQueue_
  std::thread dirThread_;
  /// Threads which are responsible for transfer of the sources
  std::vector<std::unique_ptr<WdtThread>> senderThreads_;
  /// Thread responsible for doing the progress checks. Uses reportProgress()
  std::thread progressReporterThread_;

  /// Returns the protocol negotiation status of the parent sender
  ProtoNegotiationStatus getNegotiationStatus();

  /// Set the protocol negotiation status, called by sender thread
  void setProtoNegotiationStatus(ProtoNegotiationStatus status);

  /// Things to do before ending the current transfer
  void endCurTransfer();

  /// Initializing the new transfer
  void startNewTransfer();

  /// Returns vector of negotiated protocols set by sender threads
  std::vector<int> getNegotiatedProtocols() const;

  /// Protocol negotiation status, used to co-ordinate processing of version
  /// mismatch. Threads aborted due to version mismatch waits for all threads to
  /// abort and reach PROCESS_VERSION_MISMATCH state. Last thread processes
  /// version mismatch and changes this status variable. Other threads check
  /// this variable to decide when to proceed.
  ProtoNegotiationStatus protoNegotiationStatus_{V_MISMATCH_WAIT};
  /// Time at which the transfer was started
  std::chrono::time_point<Clock> startTime_;
  /// Time at which the transfer finished
  std::chrono::time_point<Clock> endTime_;

  /// Transfer history controller for the sender threads
  std::unique_ptr<TransferHistoryController> transferHistoryController_;
};
}
}  // namespace facebook::wdt
