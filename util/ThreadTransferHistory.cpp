/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/ThreadTransferHistory.h>
#include <wdt/Sender.h>

namespace facebook {
namespace wdt {

ThreadTransferHistory::ThreadTransferHistory(DirectorySourceQueue &queue,
                                             TransferStats &threadStats,
                                             int32_t port)
    : queue_(queue), threadStats_(threadStats), port_(port) {
  WVLOG(1) << "Making thread history for port " << port_;
}

std::string ThreadTransferHistory::getSourceId(int64_t index) {
  std::lock_guard<std::mutex> lock(mutex_);
  std::string sourceId;
  const int64_t historySize = history_.size();
  if (index >= 0 && index < historySize) {
    sourceId = history_[index]->getIdentifier();
  } else {
    WLOG(WARNING) << "Trying to read out of bounds data " << index << " "
                  << history_.size();
  }
  return sourceId;
}

bool ThreadTransferHistory::addSource(std::unique_ptr<ByteSource> &source) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (globalCheckpoint_) {
    // already received an error for this thread
    WVLOG(1) << "adding source after global checkpoint is received. returning "
                "the source to the queue";
    markSourceAsFailed(source, lastCheckpoint_.get());
    lastCheckpoint_.reset();
    queue_.returnToQueue(source);
    return false;
  }
  history_.emplace_back(std::move(source));
  return true;
}

ErrorCode ThreadTransferHistory::setLocalCheckpoint(
    const Checkpoint &checkpoint) {
  std::lock_guard<std::mutex> lock(mutex_);
  return setCheckpointAndReturnToQueue(checkpoint, false);
}

ErrorCode ThreadTransferHistory::setGlobalCheckpoint(
    const Checkpoint &checkpoint) {
  std::unique_lock<std::mutex> lock(mutex_);
  ErrorCode status = setCheckpointAndReturnToQueue(checkpoint, true);
  while (inUse_) {
    // have to wait, error thread signalled through globalCheckpoint_ flag
    WLOG(INFO) << "Transfer history still in use, waiting, checkpoint "
               << checkpoint;
    conditionInUse_.wait(lock);
  }
  return status;
}
ErrorCode ThreadTransferHistory::setCheckpointAndReturnToQueue(
    const Checkpoint &checkpoint, bool globalCheckpoint) {
  const int64_t historySize = history_.size();
  int64_t numReceivedSources = checkpoint.numBlocks;
  int64_t lastBlockReceivedBytes = checkpoint.lastBlockReceivedBytes;
  if (numReceivedSources > historySize) {
    WLOG(ERROR)
        << "checkpoint is greater than total number of sources transferred "
        << history_.size() << " " << numReceivedSources;
    return INVALID_CHECKPOINT;
  }
  ErrorCode errCode = validateCheckpoint(checkpoint, globalCheckpoint);
  if (errCode == INVALID_CHECKPOINT) {
    return INVALID_CHECKPOINT;
  }
  globalCheckpoint_ |= globalCheckpoint;
  lastCheckpoint_ = std::make_unique<Checkpoint>(checkpoint);
  int64_t numFailedSources = historySize - numReceivedSources;
  if (numFailedSources == 0 && lastBlockReceivedBytes > 0) {
    if (!globalCheckpoint) {
      // no block to apply checkpoint offset. This can happen if we receive same
      // local checkpoint without adding anything to the history
      WLOG(WARNING)
          << "Local checkpoint has received bytes for last block, but "
             "there are no unacked blocks in the history. Ignoring.";
    }
  }
  numAcknowledged_ = numReceivedSources;
  std::vector<std::unique_ptr<ByteSource>> sourcesToReturn;
  for (int64_t i = 0; i < numFailedSources; i++) {
    std::unique_ptr<ByteSource> source = std::move(history_.back());
    history_.pop_back();
    const Checkpoint *checkpointPtr =
        (i == numFailedSources - 1 ? &checkpoint : nullptr);
    markSourceAsFailed(source, checkpointPtr);
    sourcesToReturn.emplace_back(std::move(source));
  }
  queue_.returnToQueue(sourcesToReturn);
  WLOG(INFO) << numFailedSources
             << " number of sources returned to queue, checkpoint: "
             << checkpoint;
  return errCode;
}

std::vector<TransferStats> ThreadTransferHistory::popAckedSourceStats() {
  std::unique_lock<std::mutex> lock(mutex_);
  const int64_t historySize = history_.size();
  WDT_CHECK(numAcknowledged_ == historySize);
  // no locking needed, as this should be called after transfer has finished
  std::vector<TransferStats> sourceStats;
  while (!history_.empty()) {
    sourceStats.emplace_back(std::move(history_.back()->getTransferStats()));
    history_.pop_back();
  }
  return sourceStats;
}

void ThreadTransferHistory::markAllAcknowledged() {
  std::unique_lock<std::mutex> lock(mutex_);
  numAcknowledged_ = history_.size();
}

void ThreadTransferHistory::returnUnackedSourcesToQueue() {
  std::unique_lock<std::mutex> lock(mutex_);
  Checkpoint checkpoint;
  checkpoint.numBlocks = numAcknowledged_;
  setCheckpointAndReturnToQueue(checkpoint, false);
}

ErrorCode ThreadTransferHistory::validateCheckpoint(
    const Checkpoint &checkpoint, bool globalCheckpoint) {
  if (lastCheckpoint_ == nullptr) {
    return OK;
  }
  if (checkpoint.numBlocks < lastCheckpoint_->numBlocks) {
    WLOG(ERROR)
        << "Current checkpoint must be higher than previous checkpoint, "
           "Last checkpoint: "
        << *lastCheckpoint_ << ", Current checkpoint: " << checkpoint;
    return INVALID_CHECKPOINT;
  }
  if (checkpoint.numBlocks > lastCheckpoint_->numBlocks) {
    return OK;
  }
  bool noProgress = false;
  // numBlocks same
  if (checkpoint.lastBlockSeqId == lastCheckpoint_->lastBlockSeqId &&
      checkpoint.lastBlockOffset == lastCheckpoint_->lastBlockOffset) {
    // same block
    if (checkpoint.lastBlockReceivedBytes !=
        lastCheckpoint_->lastBlockReceivedBytes) {
      WLOG(ERROR) << "Current checkpoint has different received bytes, but all "
                     "other fields are same, Last checkpoint "
                  << *lastCheckpoint_ << ", Current checkpoint: " << checkpoint;
      return INVALID_CHECKPOINT;
    }
    noProgress = true;
  } else {
    // different block
    WDT_CHECK(checkpoint.lastBlockReceivedBytes >= 0);
    if (checkpoint.lastBlockReceivedBytes == 0) {
      noProgress = true;
    }
  }
  if (noProgress && !globalCheckpoint) {
    // we can get same global checkpoint multiple times, so no need to check for
    // progress
    WLOG(WARNING) << "No progress since last checkpoint, Last checkpoint: "
                  << *lastCheckpoint_ << ", Current checkpoint: " << checkpoint;
    return NO_PROGRESS;
  }
  return OK;
}

void ThreadTransferHistory::markSourceAsFailed(
    std::unique_ptr<ByteSource> &source, const Checkpoint *checkpoint) {
  auto &metadata = source->getMetaData();
  bool validCheckpoint = false;
  if (checkpoint != nullptr) {
    if (checkpoint->hasSeqId) {
      if ((checkpoint->lastBlockSeqId == metadata.seqId) &&
          (checkpoint->lastBlockOffset == source->getOffset())) {
        validCheckpoint = true;
      } else {
        WLOG(WARNING)
            << "Checkpoint block does not match history block. Checkpoint: "
            << checkpoint->lastBlockSeqId << ", " << checkpoint->lastBlockOffset
            << " History: " << metadata.seqId << ", " << source->getOffset();
      }
    } else {
      // Receiver at lower version!
      // checkpoint does not have seq-id. We have to blindly trust
      // lastBlockReceivedBytes. If we do not, transfer will fail because of
      // number of bytes mismatch. Even if an error happens because of this,
      // Receiver will fail.
      validCheckpoint = true;
    }
  }
  int64_t receivedBytes =
      (validCheckpoint ? checkpoint->lastBlockReceivedBytes : 0);
  TransferStats &sourceStats = source->getTransferStats();
  if (sourceStats.getLocalErrorCode() != OK) {
    // already marked as failed
    sourceStats.addEffectiveBytes(0, receivedBytes);
    threadStats_.addEffectiveBytes(0, receivedBytes);
  } else {
    auto dataBytes = source->getSize();
    auto headerBytes = sourceStats.getEffectiveHeaderBytes();
    int64_t wastedBytes = dataBytes - receivedBytes;
    sourceStats.subtractEffectiveBytes(headerBytes, wastedBytes);
    sourceStats.decrNumBlocks();
    sourceStats.setLocalErrorCode(SOCKET_WRITE_ERROR);
    sourceStats.incrFailedAttempts();

    threadStats_.subtractEffectiveBytes(headerBytes, wastedBytes);
    threadStats_.decrNumBlocks();
    threadStats_.incrFailedAttempts();
  }
  source->advanceOffset(receivedBytes);
}

bool ThreadTransferHistory::isGlobalCheckpointReceived() {
  std::lock_guard<std::mutex> lock(mutex_);
  return globalCheckpoint_;
}

void ThreadTransferHistory::markNotInUse() {
  std::lock_guard<std::mutex> lock(mutex_);
  inUse_ = false;
  conditionInUse_.notify_all();
}

TransferHistoryController::TransferHistoryController(
    DirectorySourceQueue &dirQueue)
    : dirQueue_(dirQueue) {
}

ThreadTransferHistory &TransferHistoryController::getTransferHistory(
    int32_t port) {
  auto it = threadHistoriesMap_.find(port);
  WDT_CHECK(it != threadHistoriesMap_.end()) << "port not found" << port;
  return *(it->second.get());
}

void TransferHistoryController::addThreadHistory(int32_t port,
                                                 TransferStats &threadStats) {
  WVLOG(1) << "Adding the history for " << port;
  threadHistoriesMap_.emplace(port, std::make_unique<ThreadTransferHistory>(
                                        dirQueue_, threadStats, port));
}

ErrorCode TransferHistoryController::handleVersionMismatch() {
  for (auto &historyPair : threadHistoriesMap_) {
    auto &history = historyPair.second;
    if (history->getNumAcked() > 0) {
      WLOG(ERROR)
          << "Even though the transfer aborted due to VERSION_MISMATCH, "
             "some blocks got acked by the receiver, port "
          << historyPair.first << " numAcked " << history->getNumAcked();
      return ERROR;
    }
    history->returnUnackedSourcesToQueue();
  }
  return OK;
}

void TransferHistoryController::handleGlobalCheckpoint(
    const Checkpoint &checkpoint) {
  auto errPort = checkpoint.port;
  auto it = threadHistoriesMap_.find(errPort);
  if (it == threadHistoriesMap_.end()) {
    WLOG(ERROR) << "Invalid checkpoint " << checkpoint
                << ". No sender thread running on port " << errPort;
    return;
  }
  WVLOG(1) << "received global checkpoint " << checkpoint;
  it->second->setGlobalCheckpoint(checkpoint);
}
}
}
