#pragma once

#include "ErrorCodes.h"

#include <vector>
#include <string>

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

 public:
  /// @return number of header bytes transferred
  size_t getHeaderBytes() const {
    return headerBytes_;
  }

  /// @return number of data bytes transferred
  size_t getDataBytes() const {
    return dataBytes_;
  }

  /// @return number of total bytes transferred
  size_t getTotalBytes() const {
    return headerBytes_ + dataBytes_;
  }

  /**
   * @return    number of header bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveHeaderBytes() const {
    return effectiveHeaderBytes_;
  }

  /**
   * @return    number of data bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveDataBytes() const {
    return effectiveDataBytes_;
  }

  /**
   * @return    number of total bytes transferred as part of successful file
   *            transfer
   */
  size_t getEffectiveTotalBytes() const {
    return effectiveHeaderBytes_ + effectiveDataBytes_;
  }

  /// @return number of files successfully transferred
  size_t getNumFiles() const {
    return numFiles_;
  }

  /// @return number of failed transfers
  size_t getFailedAttempts() const {
    return failedAttempts_;
  }

  /// @return status of the transfer
  ErrorCode getErrorCode() const {
    return errCode_;
  }

  const std::string &getId() const {
    return id_;
  }

  /// @param number of additional data bytes transferred
  void addDataBytes(size_t count) {
    dataBytes_ += count;
  }

  /// @param number of additional header bytes transferred
  void addHeaderBytes(size_t count) {
    headerBytes_ += count;
  }

  /// one more file transfer failed
  void incrFailedAttempts() {
    failedAttempts_++;
  }

  /// @param status of the transfer
  void setErrorCode(ErrorCode errCode) {
    errCode_ = errCode;
  }

  /// @param id of the corresponding entity
  void setId(const std::string &id) {
    id_ = id;
  }

  /// one more file successfully transferred
  void incrNumFiles() {
    numFiles_++;
  }

  /**
   * @param headerBytes header bytes transfered part of a successful file
   *                    transfer
   * @param dataBytes   data bytes transferred part of a successful file
   *                    transfer
   */
  void addEffectiveBytes(size_t headerBytes, size_t dataBytes) {
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
  TransferReport(const std::vector<TransferStats> &transferredSourceStats,
                 const std::vector<TransferStats> &failedSourceStats,
                 const std::vector<TransferStats> &threadStats)
      : transferredSourceStats_(transferredSourceStats),
        failedSourceStats_(failedSourceStats),
        threadStats_(threadStats) {
    for (const auto &stats : threadStats) {
      summary_ += stats;
    }
    auto errCode = failedSourceStats_.empty() ? OK : ERROR;
    // Global status depends on failed files, not thread statuses
    summary_.setErrorCode(errCode);
  }
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
