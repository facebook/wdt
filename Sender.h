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

#include "DirectorySourceQueue.h"
#include "ErrorCodes.h"
#include "Throttler.h"
#include "ClientSocket.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "Protocol.h"

#include <folly/SpinLock.h>

#include <chrono>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <iostream>

namespace facebook {
namespace wdt {

class DirectorySourceQueue;

typedef std::chrono::high_resolution_clock Clock;

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
   * @param numReceivedSources    number of sources acked by the receiver
   * @param globalCheckpoint      global or local checkpoint
   * @return                      number of sources returned to queue, -1 in
   *                              case of error
   */
  int64_t setCheckpointAndReturnToQueue(int64_t numReceivedSources,
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

 private:
  void markSourceAsFailed(std::unique_ptr<ByteSource> &source);

  /// reference to global queue
  DirectorySourceQueue &queue_;
  /// refernce to thread stats
  TransferStats &threadStats_;
  /// history of the thread
  std::vector<std::unique_ptr<ByteSource>> history_;
  /// whether a global error checkpoint has been received or not
  bool globalCheckpoint_{false};
  /// number of sources acked by the receiver thread
  int64_t numAcknowledged_{0};
  folly::SpinLock lock_;
};

class Sender {
 public:
  Sender(const std::string &destHost, const std::string &srcDir);

  Sender(const std::string &destHost, const std::string &srcDir,
         const std::vector<int64_t> &ports,
         const std::vector<FileInfo> &srcFileInfo);

  Sender(int port, int numSockets, const std::string &destHost,
         const std::string &srcDir);

  virtual ~Sender() {
  }

  std::unique_ptr<TransferReport> start();

  void setIncludeRegex(const std::string &includeRegex);

  void setExcludeRegex(const std::string &excludeRegex);

  void setPruneDirRegex(const std::string &pruneDirRegex);

  void setNumSockets(const int numSockets);

  void setSrcFileInfo(const std::vector<FileInfo> &srcFileInfo);

  void setFollowSymlinks(const bool followSymlinks);

  /**
   * @param progressReportIntervalMillis_   interval(ms) between progress
   *                                        reports. A value of 0 indicates no
   *                                        progress reporting
   */
  void setProgressReportIntervalMillis(const int progressReportIntervalMillis);

  /**
   * @param progressReporter    progress reporter to be used. By default, wdt
   *                            uses a progress reporter which shows progress
   *                            percentage and current throughput. It also
   *                            visually shows progress. Sample report:
   *                            [=====>               ] 30% 2500.00 Mbytes/sec
   */
  void setProgressReporter(std::unique_ptr<ProgressReporter> &progressReporter);

 private:
  /// state machine states
  enum SenderState {
    CONNECT,
    READ_LOCAL_CHECKPOINT,
    SEND_SETTINGS,
    SEND_BLOCKS,
    SEND_DONE_CMD,
    READ_RECEIVER_CMD,
    PROCESS_DONE_CMD,
    PROCESS_WAIT_CMD,
    PROCESS_ERR_CMD,
    END
  };

  /// structure to share data among different states
  struct ThreadData {
    const int threadIndex_;
    DirectorySourceQueue &queue_;
    TransferStats &threadStats_;
    std::vector<ThreadTransferHistory> &transferHistories_;
    std::unique_ptr<Throttler> throttler_;
    std::unique_ptr<ClientSocket> socket_;
    char buf_[Protocol::kMinBufLength];
    ThreadData(int threadIndex, DirectorySourceQueue &queue,
               TransferStats &threadStats,
               std::vector<ThreadTransferHistory> &transferHistories)
        : threadIndex_(threadIndex),
          queue_(queue),
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
   * Next states : END(global checkpoint received),
   *               CONNECT(socket write failure),
   *               SEND_DONE_CMD(success)
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

  /// mapping from sender states to state functions
  static const StateFunction stateMap_[];

  virtual TransferStats sendOneByteSource(
      const std::unique_ptr<ClientSocket> &socket,
      const std::unique_ptr<Throttler> &throttler,
      const std::unique_ptr<ByteSource> &source, const size_t totalBytes,
      ErrorCode transferStatus);

  /**
   * @param startTime           Time when this thread was spawned
   * @param threadIndex         Index of the thread
   * @param queue               DirectorySourceQueue object for reading files
   * @param avgRateBytes        Average rate of throttler in bytes/sec
   * @param maxRateBytes        Peak rate for Token Bucket algorithm in
   *                            bytes/sec
   * @param bucketLimitBytes    Bucket Limit for Token Bucket algorithm in
   *                            bytes
   * @param threadStats         Per thread statistics
   * @param transferHistories   list of transfer histories of all threads
   */
  void sendOne(Clock::time_point startTime, int threadIndex,
               DirectorySourceQueue &queue, double avgRateBytes,
               double maxRateBytes, double bucketLimitBytes,
               TransferStats &threadStats,
               std::vector<ThreadTransferHistory> &transferHistories);

  virtual std::unique_ptr<ClientSocket> makeSocket(const std::string &destHost,
                                                   int port);
  std::unique_ptr<ClientSocket> connectToReceiver(const int port,
                                                  ErrorCode &errCode);
  void validateTransferStats(
      const std::vector<TransferStats> &transferredSourceStats,
      const std::vector<TransferStats> &failedSourceStats,
      const std::vector<TransferStats> &threadStats);

  void reportProgress(Clock::time_point startTime,
                      std::vector<TransferStats> &threadStats,
                      DirectorySourceQueue &queue);

  std::vector<int64_t> ports_;
  std::string srcDir_;
  std::string destHost_;
  std::string pruneDirRegex_;
  std::string includeRegex_;
  std::string excludeRegex_;
  std::vector<FileInfo> srcFileInfo_;
  bool followSymlinks_;
  int progressReportIntervalMillis_;
  std::unique_ptr<ProgressReporter> progressReporter_;

  std::condition_variable conditionFinished_;
  std::mutex mutex_;
  bool transferFinished_{false};
};
}
}  // namespace facebook::wdt
