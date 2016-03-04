/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <folly/SpinLock.h>
#include <wdt/Protocol.h>
#include <wdt/Reporting.h>
#include <wdt/util/DirectorySourceQueue.h>
#include <vector>

namespace facebook {
namespace wdt {

/// Transfer history of a sender thread
class ThreadTransferHistory {
 public:
  /**
   * @param queue        directory queue
   * @param threadStats  stat object of the thread
   */
  ThreadTransferHistory(DirectorySourceQueue &queue, TransferStats &threadStats,
                        int32_t port);

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
   * @param checkpoint             checkpoint received
   * @param globalCheckpoint       global or local checkpoint
   * @return                       number of sources returned to queue
   */
  ErrorCode setLocalCheckpoint(const Checkpoint &checkpoint);

  /**
   * @return            stats for acked sources, must be called after all the
   *                    unacked sources are returned to the queue
   */
  std::vector<TransferStats> popAckedSourceStats();

  /// marks all the sources as acked
  void markAllAcknowledged();

  /**
   * returns all unacked sources to the queue
   */
  void returnUnackedSourcesToQueue();

  /**
   * @return    number of sources acked by the receiver
   */
  int64_t getNumAcked() const {
    return numAcknowledged_;
  }

  /// @return   whether global checkpoint has been received or not
  bool isGlobalCheckpointReceived();

  /// Clears the inUse_ flag and notifies other waiting threads
  void markNotInUse();

  /// Copy constructor deleted
  ThreadTransferHistory(const ThreadTransferHistory &that) = delete;

  /// Delete the assignment operatory by copy
  ThreadTransferHistory &operator=(const ThreadTransferHistory &that) = delete;

 private:
  friend class TransferHistoryController;
  /// Validates a checkpoint and returns the status
  ErrorCode validateCheckpoint(const Checkpoint &checkpoint,
                               bool globalCheckpoint);
  /**
   * Sets global checkpoint. If the history is still in use, waits for the
   * error thread to add current block to the history.
   *
   * @param checkpoint             checkpoint received
   *
   * @return                       status of the operation
   */
  ErrorCode setGlobalCheckpoint(const Checkpoint &checkpoint);

  void markSourceAsFailed(std::unique_ptr<ByteSource> &source,
                          const Checkpoint *checkpoint);
  /**
   * Sets checkpoint. Also, returns unacked sources to queue
   *
   * @param checkpoint             checkpoint received
   * @param globalCheckpoint       global or local checkpoint
   * @return                       status of sources returned to queue
   */
  ErrorCode setCheckpointAndReturnToQueue(const Checkpoint &checkpoint,
                                          bool globalCheckpoint);

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
  /// last received checkpoint
  std::unique_ptr<Checkpoint> lastCheckpoint_{nullptr};
  /// Port assosciated with the history
  int32_t port_;
  /// whether the owner thread is still using this
  bool inUse_{true};
  /// Mutex used by history internally for synchronization
  std::mutex mutex_;
  /// Condition variable to signify the history being in use
  std::condition_variable conditionInUse_;
};

/// Controller for history across the sender threads
class TransferHistoryController {
 public:
  /**
   * Constructor for the history controller
   * @param dirQueue      Directory queue used by the sender
   */
  explicit TransferHistoryController(DirectorySourceQueue &dirQueue);

  /**
   * Add transfer history for a thread
   * @param port           Port being used by the sender thread
   * @param threadStats    Thread stats for the sender thread
   */
  void addThreadHistory(int32_t port, TransferStats &threadStats);

  /// Get transfer history for a thread using its port number
  ThreadTransferHistory &getTransferHistory(int32_t port);

  /// Handle version mismatch across the histories of all thread
  ErrorCode handleVersionMismatch();

  /// Handle checkpoint that was sent by the receiver
  void handleGlobalCheckpoint(const Checkpoint &checkpoint);

 private:
  /// Reference to the directory queue being used by the sender
  DirectorySourceQueue &dirQueue_;

  /// Map of port (used by sender threads) and transfer history
  std::unordered_map<int32_t, std::unique_ptr<ThreadTransferHistory>>
      threadHistoriesMap_;
};
}
}
