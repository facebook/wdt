#pragma once

#include "ErrorCodes.h"

#include <vector>
#include <string>

#include <folly/RWSpinLock.h>
#include <folly/Memory.h>

namespace facebook {
namespace wdt {

// TODO: make those non copyable (or at least not copied by accident)

/// class representing statistics related to file transfer
class TransferStats {
 private:
  /// number of header bytes transferred
  size_t headerBytes_ = 0;
  /// number of data bytes transferred
  size_t dataBytes_ = 0;

  /// number of header bytes transferred as part of successful file transfer
  size_t effectiveHeaderBytes_ = 0;
  /// number of data bytes transferred as part of successful file transfer
  size_t effectiveDataBytes_ = 0;

  /// number of files successfully transferred
  size_t numFiles_ = 0;
  /// number of failed transfers
  size_t failedAttempts_ = 0;

  /// status of the transfer
  ErrorCode errCode_ = OK;

  /// id of the owner object
  std::string id_;

  /// mutex to support synchronized access
  std::unique_ptr<folly::RWSpinLock> mutex_{nullptr};

 public:
  // making the object noncopyable
  TransferStats(const TransferStats &stats) = delete;
  TransferStats &operator=(const TransferStats &stats) = delete;
  TransferStats(TransferStats &&stats) = default;
  TransferStats &operator=(TransferStats &&stats) = default;

  explicit TransferStats(bool isLocked = false) {
    if (isLocked) {
      mutex_ = folly::make_unique<folly::RWSpinLock>();
    }
  }

  /// @return number of header bytes transferred
  size_t getHeaderBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return headerBytes_;
  }

  /// @return number of data bytes transferred
  size_t getDataBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return dataBytes_;
  }

  /// @return number of total bytes transferred
  size_t getTotalBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return headerBytes_ + dataBytes_;
  }

  /**
   * @return    number of header bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveHeaderBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveHeaderBytes_;
  }

  /**
   * @return    number of data bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveDataBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveDataBytes_;
  }

  /**
   * @return    number of total bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveTotalBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveHeaderBytes_ + effectiveDataBytes_;
  }

  /// @return number of files successfully transferred
  size_t getNumFiles() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return numFiles_;
  }

  /// @return number of failed transfers
  size_t getFailedAttempts() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return failedAttempts_;
  }

  /// @return status of the transfer
  ErrorCode getErrorCode() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return errCode_;
  }

  const std::string &getId() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return id_;
  }

  /// @param number of additional data bytes transferred
  void addDataBytes(size_t count) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    dataBytes_ += count;
  }

  /// @param number of additional header bytes transferred
  void addHeaderBytes(size_t count) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    headerBytes_ += count;
  }

  /// one more file transfer failed
  void incrFailedAttempts() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    failedAttempts_++;
  }

  /// @param status of the transfer
  void setErrorCode(ErrorCode errCode) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    errCode_ = errCode;
  }

  /// @param id of the corresponding entity
  void setId(const std::string &id) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    id_ = id;
  }

  /// one more file successfully transferred
  void incrNumFiles() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    numFiles_++;
  }

  /**
   * @param headerBytes header bytes transfered part of a successful file
   *                    transfer
   * @param dataBytes   data bytes transferred part of a successful file
   *                    transfer
   */
  void addEffectiveBytes(size_t headerBytes, size_t dataBytes) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    effectiveHeaderBytes_ += headerBytes;
    effectiveDataBytes_ += dataBytes;
  }

  TransferStats &operator+=(const TransferStats &stats);

  friend std::ostream &operator<<(std::ostream &os, const TransferStats &stats);
};

/**
 * class representing entire client transfer report
 */
class TransferReport {
 public:
  TransferReport(std::vector<TransferStats> &transferredSourceStats,
                 std::vector<TransferStats> &failedSourceStats,
                 std::vector<TransferStats> &threadStats);
  /// @return   summary of the report
  const TransferStats &getSummary() const {
    return summary_;
  }
  /// @return   stats for successfully transferred sources
  const std::vector<TransferStats> &getTransferredSourceStats() const {
    return transferredSourceStats_;
  }
  /// @return   stats for failed sources
  const std::vector<TransferStats> &getFailedSourceStats() const {
    return failedSourceStats_;
  }
  /// @return   stats for threads
  const std::vector<TransferStats> &getThreadStats() const {
    return threadStats_;
  }
  /// @param stats  stats to added
  void addTransferStats(const TransferStats &stats) {
    summary_ += stats;
  }
  friend std::ostream &operator<<(std::ostream &os,
                                  const TransferReport &report);

 private:
  TransferStats summary_;
  /// stats for successfully transferred sources
  std::vector<TransferStats> transferredSourceStats_;
  /// stats for failed sources
  std::vector<TransferStats> failedSourceStats_;
  /// stats for client threads
  std::vector<TransferStats> threadStats_;
};
}
}
