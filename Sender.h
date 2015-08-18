/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "WdtBase.h"
#include "ClientSocket.h"
#include "WdtOptions.h"

#include <folly/SpinLock.h>

#include <chrono>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <iostream>

namespace facebook {
namespace wdt {

class DirectorySourceQueue;

/// transfer history of a sender thread
class ThreadTransferHistory {
 public:
  /**
   * @param queue        directory queue
   * @param threadStats  stat object of the thread
   */
  ThreadTransferHistory(DirectorySourceQueue &queue,
                        TransferStats &threadStats);

  /**
   * @param             index of the source
   * @return            if index is in bounds, returns the identifier for the
   *                    source, else returns empty string
   */
  std::string getSourceId(int64_t index);

  /**
   * Adds the source to the history. If global checkpoint has already been
   * received, then the source is returned to the queue.
   *
   * @param source      source to be added to history
   * @return            true if added to history, false if not added due to a
   *                    global checkpoint
   */
  bool addSource(std::unique_ptr<ByteSource> &source);

  /**
   * Sets checkpoint. Also, returns unacked sources to queue
   *
   * @param numReceivedSources     number of sources acked by the receiver
   * @param lastBlockReceivedBytes number of bytes received for the last block
   * @param globalCheckpoint       global or local checkpoint
   * @return                       number of sources returned to queue, -1 in
   *                               case of error
   */
  int64_t setCheckpointAndReturnToQueue(int64_t numReceivedSources,
                                        int64_t lastBlockReceivedBytes,
                                        bool globalCheckpoint);

  /**
   * @return            stats for acked sources, must be called after all the
   *                    unacked sources are returned to the queue
   */
  std::vector<TransferStats> popAckedSourceStats();

  /// marks all the sources as acked
  void markAllAcknowledged();

  /**
   * returns all unacked sources to the queue
   * @return            number of sources returned to queue, -1 in case of error
   */
  int64_t returnUnackedSourcesToQueue();

  /**
   * @return    number of sources acked by the receiver
   */
  int64_t getNumAcked() const {
    return numAcknowledged_;
  }

 private:
  void markSourceAsFailed(std::unique_ptr<ByteSource> &source,
                          int64_t receivedBytes);

  /// reference to global queue
  DirectorySourceQueue &queue_;
  /// reference to thread stats
  TransferStats &threadStats_;
  /// history of the thread
  std::vector<std::unique_ptr<ByteSource>> history_;
  /// whether a global error checkpoint has been received or not
  bool globalCheckpoint_{false};
  /// number of sources acked by the receiver thread
  int64_t numAcknowledged_{0};
  /// number of bytes received for the last block
  int64_t lastBlockReceivedBytes_{0};
  folly::SpinLock lock_;
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

  /**
   * @param destHost    destination hostname
   * @param srcDir      source directory
   */
  Sender(const std::string &destHost, const std::string &srcDir);

  /**
   * @param destHost    destination hostname
   * @param srcDir      source directory
   * @param ports       list of destination ports
   * @param srcFileInfo list of (file, size) pair
   */
  Sender(const std::string &destHost, const std::string &srcDir,
         const std::vector<int32_t> &ports,
         const std::vector<FileInfo> &srcFileInfo);

  /// Setup before start (@see WdtBase.h)
  WdtTransferRequest init() override;

  /**
   * If the transfer has not finished, then it is aborted. finish() is called to
   * wait for threads to end.
   */
  virtual ~Sender();

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

  /// TODO: move to base
  /// @return    whether transfer is finished
  bool isTransferFinished();

  /// End time of the transfer
  Clock::time_point getEndTime();

  /// Sets regex representing files to include for transfer
  /// @param includeRegex     regex for files to include for transfer
  void setIncludeRegex(const std::string &includeRegex);

  /// Sets regex representing files to exclude for transfer
  /// @param excludeRegex     regex for files to exclude for transfer
  void setExcludeRegex(const std::string &excludeRegex);

  /// Sets regex representing directories to exclude for transfer
  /// @param pruneDirRegex    regex for directories to exclude
  void setPruneDirRegex(const std::string &pruneDirRegex);

  /// Sets specific files to be transferred
  /// @param setFileInfo      list of (file, size) pair
  void setSrcFileInfo(const std::vector<FileInfo> &srcFileInfo);

  /// Sets whether to follow symlink or not
  /// @param followSymlinks   whether to follow symlinks or not
  void setFollowSymlinks(const bool followSymlinks);

  /// Get the ports sender is operating on
  /// @return     list of destination ports
  const std::vector<int32_t> &getPorts() const;

  /// Get the destination sender is sending to
  /// @return     destination host-name
  const std::string &getDestination() const;

  /// Get the source directory sender is reading from
  /// @return     source directory
  const std::string &getSrcDir() const;

  /**
   * @param progressReportIntervalMillis_   interval(ms) between progress
   *                                        reports. A value of 0 indicates no
   *                                        progress reporting
   * TODO: move to base
   */
  void setProgressReportIntervalMillis(const int progressReportIntervalMillis);

  /// @retun    minimal transfer report using transfer stats of the thread
  std::unique_ptr<TransferReport> getTransferReport();

  typedef std::unique_ptr<ClientSocket> (*SocketCreator)(
      const std::string &dest, const std::string &port,
      IAbortChecker const *abortChecker);

  /**
   * Sets socket creator
   *
   * @param socketCreator   socket-creator to be used
   */
  void setSocketCreator(const SocketCreator socketCreator);

 private:
  /// Abort checker passed to DirectoryQueue. If all the network threads finish,
  /// directory discovery thread is also aborted
  class QueueAbortChecker : public IAbortChecker {
   public:
    explicit QueueAbortChecker(Sender *sender) : sender_(sender) {
    }

    bool shouldAbort() const {
      return sender_->isTransferFinished();
    }

   private:
    Sender *sender_;
  };

  /// state machine states
  enum SenderState {
    CONNECT,
    READ_LOCAL_CHECKPOINT,
    SEND_SETTINGS,
    SEND_BLOCKS,
    SEND_DONE_CMD,
    SEND_SIZE_CMD,
    CHECK_FOR_ABORT,
    READ_FILE_CHUNKS,
    READ_RECEIVER_CMD,
    PROCESS_DONE_CMD,
    PROCESS_WAIT_CMD,
    PROCESS_ERR_CMD,
    PROCESS_ABORT_CMD,
    PROCESS_VERSION_MISMATCH,
    END
  };

  /// structure to share data among different states
  struct ThreadData {
    const int threadIndex_;
    TransferStats &threadStats_;
    std::vector<ThreadTransferHistory> &transferHistories_;
    std::unique_ptr<ClientSocket> socket_;
    char buf_[Protocol::kMinBufLength];
    /// whether total file size has been sent to the receiver
    bool totalSizeSent_{false};
    ThreadData(int threadIndex, TransferStats &threadStats,
               std::vector<ThreadTransferHistory> &transferHistories)
        : threadIndex_(threadIndex),
          threadStats_(threadStats),
          transferHistories_(transferHistories) {
    }

    ThreadTransferHistory &getTransferHistory() {
      return transferHistories_[threadIndex_];
    }
  };

  typedef SenderState (Sender::*StateFunction)(ThreadData &data);

  /**
   * tries to connect to the receiver
   * Previous states : Almost all states(in case of network errors, all states
   *                   move to this state)
   * Next states : SEND_SETTINGS(if there is no previous error)
   *               READ_LOCAL_CHECKPOINT(if there is previous error)
   *               END(failed)
   */
  SenderState connect(ThreadData &data);
  /**
   * tries to read local checkpoint and return unacked sources to queue. If the
   * checkpoint value is -1, then we know previous attempt to send DONE had
   * failed. So, we move to READ_RECEIVER_CMD state.
   * Previous states : CONNECT
   * Next states : CONNECT(read failure),
   *               END(protocol error or global checkpoint found),
   *               READ_RECEIVER_CMD(if checkpoint is -1),
   *               SEND_SETTINGS(success)
   */
  SenderState readLocalCheckPoint(ThreadData &data);
  /**
   * sends sender settings to the receiver
   * Previous states : READ_LOCAL_CHECKPOINT,
   *                   CONNECT
   * Next states : SEND_BLOCKS(success),
   *               CONNECT(failure)
   */
  SenderState sendSettings(ThreadData &data);
  /**
   * sends blocks to receiver till the queue is not empty. After transferring a
   * block, we add it to the history. While adding to history, if it is found
   * that global checkpoint has been received for this thread, we move to END
   * state.
   * Previous states : SEND_SETTINGS,
   *                   PROCESS_ERR_CMD
   * Next states : SEND_BLOCKS(success),
   *               END(global checkpoint received),
   *               CHECK_FOR_ABORT(socket write failure),
   *               SEND_DONE_CMD(no more blocks left to transfer)
   */
  SenderState sendBlocks(ThreadData &data);
  /**
   * sends DONE cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CONNECT(failure),
   *               READ_RECEIVER_CMD(success)
   */
  SenderState sendDoneCmd(ThreadData &data);
  /**
   * sends size cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CHECK_FOR_ABORT(failure),
   *               SEND_BLOCKS(success)
   */
  SenderState sendSizeCmd(ThreadData &data);
  /**
   * checks to see if the receiver has sent ABORT or not
   * Previous states : SEND_BLOCKS,
   *                   SEND_DONE_CMD
   * Next states : CONNECT(no ABORT cmd),
   *               END(protocol error),
   *               PROCESS_ABORT_CMD(read ABORT cmd)
   */
  SenderState checkForAbort(ThreadData &data);
  /**
   * reads previously transferred file chunks list. If it receives an ACK cmd,
   * then it moves on. If wait cmd is received, it waits. Otherwise reads the
   * file chunks and when done starts directory queue thread.
   * Previous states : SEND_SETTINGS,
   * Next states: READ_FILE_CHUNKS(if wait cmd is received),
   *              CHECK_FOR_ABORT(network error),
   *              END(protocol error),
   *              SEND_BLOCKS(success)
   *
   */
  SenderState readFileChunks(ThreadData &data);
  /**
   * reads receiver cmd
   * Previous states : SEND_DONE_CMD
   * Next states : PROCESS_DONE_CMD,
   *               PROCESS_WAIT_CMD,
   *               PROCESS_ERR_CMD,
   *               END(protocol error),
   *               CONNECT(failure)
   */
  SenderState readReceiverCmd(ThreadData &data);
  /**
   * handles DONE cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processDoneCmd(ThreadData &data);
  /**
   * handles WAIT cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : READ_RECEIVER_CMD
   */
  SenderState processWaitCmd(ThreadData &data);
  /**
   * reads list of global checkpoints and returns unacked sources to queue.
   * Previous states : READ_RECEIVER_CMD
   * Next states : CONNECT(socket read failure)
   *               END(checkpoint list decode failure),
   *               SEND_BLOCKS(success)
   */
  SenderState processErrCmd(ThreadData &data);
  /**
   * processes ABORT cmd
   * Previous states : CHECK_FOR_ABORT,
   *                   READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processAbortCmd(ThreadData &data);

  /**
   * waits for all active threads to be aborted, checks to see if the abort was
   * due to version mismatch. Also performs various sanity checks.
   * Previous states : Almost all threads, abort flags is checked between every
   *                   state transition
   * Next states : CONNECT(Abort was due to version kismatch),
   *               END(if abort was not due to version mismatch or some sanity
   *               check failed)
   */
  SenderState processVersionMismatch(ThreadData &data);

  /// mapping from sender states to state functions
  static const StateFunction stateMap_[];

  /// Method responsible for sending one source to the destination
  virtual TransferStats sendOneByteSource(
      const std::unique_ptr<ClientSocket> &socket,
      const std::unique_ptr<ByteSource> &source, ErrorCode transferStatus);

  /// Every sender thread executes this method to send the data
  void sendOne(int threadIndex);

  std::unique_ptr<ClientSocket> connectToReceiver(const int port,
                                                  ErrorCode &errCode);

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
   * @param threadStats                 Stats calculated by each sender thread
   */
  void validateTransferStats(
      const std::vector<TransferStats> &transferredSourceStats,
      const std::vector<TransferStats> &failedSourceStats,
      const std::vector<TransferStats> &threadStats);

  /**
   * Responsible for doing a periodic check.
   * 1. Takes a lock on the thread stats to make a summary
   * 2. Sends the progress report with the summary to the progress reporter
   *    which can be provided by the user
   */
  void reportProgress();

  /// Pointer to DirectorySourceQueue which reads the srcDir and the files
  std::unique_ptr<DirectorySourceQueue> dirQueue_;
  /// List of ports where the receiver threads are running on the destination
  std::vector<int32_t> ports_;
  /// Number of active threads, decremented every time a thread is finished
  int32_t numActiveThreads_{0};
  /// The directory from where the files are read
  std::string srcDir_;
  /// Address of the destination host where the files are sent
  std::string destHost_;
  /// The interval at which the progress reporter should check for progress
  int progressReportIntervalMillis_;
  /// Socket creator used to optionally create different kinds of client socket
  SocketCreator socketCreator_{nullptr};
  /// Whether download resumption is enabled or not
  bool downloadResumptionEnabled_{false};
  /// Flags representing whether file chunks have been received or not
  bool fileChunksReceived_{false};
  /// Thread that is running the discovery of files using the dirQueue_
  std::thread dirThread_;
  /// Threads which are responsible for transfer of the sources
  std::vector<std::thread> senderThreads_;
  /// Thread responsible for doing the progress checks. Uses reportProgress()
  std::thread progressReporterThread_;
  /// Vector of per thread stats, this same instance is used in reporting
  std::vector<TransferStats> globalThreadStats_;
  /// per thread perf report
  std::vector<PerfStatReport> perfReports_;
  /// per thread negotiated protocol versions
  std::vector<int> negotiatedProtocolVersions_;
  /// number of threads waiting in PROCESS_VERSION_MISMATCH state
  int numWaitingWithAbort_{0};
  /// Condition variable used to co-ordinate threads waiting in
  /// PROCESS_VERSION_MISMATCH state
  std::condition_variable conditionAllAborted_;

  enum ProtoNegotiationStatus {
    V_MISMATCH_WAIT,      // waiting for version mismatch to be processed
    V_MISMATCH_RESOLVED,  // version mismatch processed and was successful
    V_MISMATCH_FAILED,    // version mismatch processed and it failed
  };
  /// Protocol negotiation status, used to co-ordinate processing of version
  /// mismatch. Threads aborted due to version mismatch waits for all threads to
  /// abort and reach PROCESS_VERSION_MISMATCH state. Last thread processes
  /// version mismatch and changes this status variable. Other threads check
  /// this variable to decide when to proceed.
  ProtoNegotiationStatus protoNegotiationStatus_{V_MISMATCH_WAIT};
  /// This condition is notified when the transfer is finished
  std::condition_variable conditionFinished_;
  /// Mutex which is shared between the parent thread, sender thread and
  /// progress reporter thread
  std::mutex mutex_;
  /// Set to false when the transfer begins and then turned on when it ends
  bool transferFinished_;
  /// Time at which the transfer was started
  std::chrono::time_point<Clock> startTime_;
  /// Time at which the transfer finished
  std::chrono::time_point<Clock> endTime_;
  /// Per thread transfer history
  std::vector<ThreadTransferHistory> transferHistories_;
  /// Has finished been called and threads joined
  bool areThreadsJoined_{true};
  /// Mutex for the management of this instance, specifically to keep the
  /// instance sane for multi threaded public API calls
  std::mutex instanceManagementMutex_;
};
}
}  // namespace facebook::wdt
