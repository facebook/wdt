/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/AbortChecker.h>
#include <wdt/ErrorCodes.h>
#include <wdt/WdtOptions.h>
#include <wdt/WdtTransferRequest.h>
#include <wdt/util/EncryptionUtils.h>

#include <algorithm>
#include <chrono>
#include <iterator>
#include <limits>
#include <string>
#include <unordered_map>
#include <vector>

#include <folly/synchronization/RWSpinLock.h>

namespace facebook {
namespace wdt {

const double kMbToB = 1024 * 1024;
const double kMicroToMilli = 1000;
const double kMicroToSec = 1000 * 1000;
const double kMilliToSec = 1000;

typedef std::chrono::high_resolution_clock Clock;

template <typename T>
int64_t durationMicros(T d) {
  return std::chrono::duration_cast<std::chrono::microseconds>(d).count();
}

template <typename T>
int durationMillis(T d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

template <typename T>
double durationSeconds(T d) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

template <typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}

// TODO rename to ThreadResult
/// class representing statistics related to file transfer
class TransferStats {
 private:
  /// number of header bytes transferred
  int64_t headerBytes_ = 0;
  /// number of data bytes transferred
  int64_t dataBytes_ = 0;

  /// number of header bytes transferred as part of successful file transfer
  int64_t effectiveHeaderBytes_ = 0;
  /// number of data bytes transferred as part of successful file transfer
  int64_t effectiveDataBytes_ = 0;

  /// number of files successfully transferred
  int64_t numFiles_ = 0;

  /// number of blocks successfully transferred
  int64_t numBlocks_ = 0;

  /// number of failed transfers
  int64_t failedAttempts_ = 0;

  /// Total number of blocks sent by sender
  int64_t numBlocksSend_{-1};

  /// Total number of bytes sent by sender
  int64_t totalSenderBytes_{-1};

  /// status of the transfer
  ErrorCode localErrCode_ = OK;

  /// status of the remote
  ErrorCode remoteErrCode_ = OK;

  /// id of the owner object
  std::string id_;

  /// encryption type used
  EncryptionType encryptionType_{ENC_NONE};

  /// is tls enabled?
  bool tls_{false};

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
      mutex_ = std::make_unique<folly::RWSpinLock>();
    }
  }

  explicit TransferStats(const std::string &id, bool isLocked = false)
      : TransferStats(isLocked) {
    id_ = id;
  }

  void reset() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    headerBytes_ = dataBytes_ = 0;
    effectiveHeaderBytes_ = effectiveDataBytes_ = 0;
    numFiles_ = numBlocks_ = 0;
    failedAttempts_ = 0;
    localErrCode_ = remoteErrCode_ = OK;
  }

  /// @return the number of blocks sent by sender
  int64_t getNumBlocksSend() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return numBlocksSend_;
  }

  /// @return the total sender bytes
  int64_t getTotalSenderBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return totalSenderBytes_;
  }

  /// @return number of header bytes transferred
  int64_t getHeaderBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return headerBytes_;
  }

  /// @return number of data bytes transferred
  int64_t getDataBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return dataBytes_;
  }

  /**
   * @param needLocking     specifies whether we need to lock or not. this is
   *                        for performance improvement. in sender, we do not
   *                        need locking for this call, even though the other
   *                        calls have to be locked
   *
   * @return                number of total bytes transferred
   */
  int64_t getTotalBytes(bool needLocking = true) const {
    if (needLocking) {
      folly::RWSpinLock::ReadHolder lock(mutex_.get());
      return headerBytes_ + dataBytes_;
    }
    return headerBytes_ + dataBytes_;
  }

  /**
   * @return    number of header bytes transferred as part of successful file
   *            transfer
   */
  int64_t getEffectiveHeaderBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveHeaderBytes_;
  }

  /**
   * @return    number of data bytes transferred as part of successful file
   *            transfer
   */
  int64_t getEffectiveDataBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveDataBytes_;
  }

  /**
   * @return    number of total bytes transferred as part of successful file
   *            transfer
   */
  int64_t getEffectiveTotalBytes() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return effectiveHeaderBytes_ + effectiveDataBytes_;
  }

  /// @return number of files successfully transferred
  int64_t getNumFiles() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return numFiles_;
  }

  /// @return number of blocks successfully transferred
  int64_t getNumBlocks() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return numBlocks_;
  }

  /// @return number of failed transfers
  int64_t getFailedAttempts() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return failedAttempts_;
  }

  /// @return error code based on combinator of local and remote error
  ErrorCode getErrorCode() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return getMoreInterestingError(localErrCode_, remoteErrCode_);
  }

  /// @return status of the transfer on this side
  ErrorCode getLocalErrorCode() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return localErrCode_;
  }

  /// @return status of the transfer on the remote end
  ErrorCode getRemoteErrorCode() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return remoteErrCode_;
  }

  const std::string &getId() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return id_;
  }

  /// @param number of additional data bytes transferred
  void addDataBytes(int64_t count) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    dataBytes_ += count;
  }

  /// @param number of additional header bytes transferred
  void addHeaderBytes(int64_t count) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    headerBytes_ += count;
  }

  /// @param set num blocks send
  void setNumBlocksSend(int64_t numBlocksSend) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    numBlocksSend_ = numBlocksSend;
  }

  /// @param set total sender bytes
  void setTotalSenderBytes(int64_t totalSenderBytes) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    totalSenderBytes_ = totalSenderBytes;
  }

  /// one more file transfer failed
  void incrFailedAttempts() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    failedAttempts_++;
  }

  /// @param status of the transfer
  void setLocalErrorCode(ErrorCode errCode) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    localErrCode_ = errCode;
  }

  /// @param status of the transfer on the remote end
  void setRemoteErrorCode(ErrorCode remoteErrCode) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    remoteErrCode_ = remoteErrCode;
  }

  /// @param id of the corresponding entity
  void setId(const std::string &id) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    id_ = id;
  }

  /// @param numFiles number of files successfully send
  void setNumFiles(int64_t numFiles) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    numFiles_ = numFiles;
  }

  /// one more block successfully transferred
  void incrNumBlocks() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    numBlocks_++;
  }

  void decrNumBlocks() {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    numBlocks_--;
  }

  /**
   * @param headerBytes header bytes transfered part of a successful file
   *                    transfer
   * @param dataBytes   data bytes transferred part of a successful file
   *                    transfer
   */
  void addEffectiveBytes(int64_t headerBytes, int64_t dataBytes) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    effectiveHeaderBytes_ += headerBytes;
    effectiveDataBytes_ += dataBytes;
  }

  void subtractEffectiveBytes(int64_t headerBytes, int64_t dataBytes) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    effectiveHeaderBytes_ -= headerBytes;
    effectiveDataBytes_ -= dataBytes;
  }

  void setEncryptionType(EncryptionType encryptionType) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    encryptionType_ = encryptionType;
  }

  EncryptionType getEncryptionType() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return encryptionType_;
  }

  void setTls(bool tls) {
    folly::RWSpinLock::WriteHolder lock(mutex_.get());
    tls_ = tls;
  }

  bool getTls() const {
    folly::RWSpinLock::ReadHolder lock(mutex_.get());
    return tls_;
  }

  TransferStats &operator+=(const TransferStats &stats);

  friend std::ostream &operator<<(std::ostream &os, const TransferStats &stats);
};

/**
 * Class representing entire client transfer report.
 * Unit are mebibyte (MiB), ie 1048576 bytes which we call "Mbytes"
 * for familiarity
 */
class TransferReport {
 public:
  // TODO: too many constructor parameters, needs to clean-up
  /**
   * This constructor moves all the stat objects to member variables. This is
   * only called at the end of transfer by the sender
   */
  TransferReport(std::vector<TransferStats> &transferredSourceStats,
                 std::vector<TransferStats> &failedSourceStats,
                 std::vector<TransferStats> &threadStats,
                 std::vector<std::string> &failedDirectories, double totalTime,
                 int64_t totalFileSize, int64_t numDiscoveredFiles,
                 int64_t previouslySentBytes, bool fileDiscoveryFinished);

  /**
   * This function does not move the thread stats passed to it. This is called
   * by the progress reporter thread.
   */
  TransferReport(const std::vector<TransferStats> &threadStats,
                 double totalTime, int64_t totalFileSize,
                 int64_t numDiscoveredFiles, bool fileDiscoveryFinished);

  TransferReport(TransferStats &&stats, double totalTime, int64_t totalFileSize,
                 int64_t numDiscoveredFiles, bool fileDiscoveryFinished);
  /// constructor used by receiver, does move the stats
  explicit TransferReport(TransferStats &&globalStats);
  /// @return   summary of the report
  const TransferStats &getSummary() const {
    return summary_;
  }
  /// @return   transfer throughput in Mbytes/sec
  double getThroughputMBps() const {
    return summary_.getEffectiveTotalBytes() / totalTime_ / kMbToB;
  }
  /// @return total time taken in transfer
  double getTotalTime() const {
    return totalTime_;
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
  const std::vector<std::string> &getFailedDirectories() const {
    return failedDirectories_;
  }
  int64_t getTotalFileSize() const {
    return totalFileSize_;
  }
  /// @return   recent throughput in Mbytes/sec
  double getCurrentThroughputMBps() const {
    return currentThroughput_ / kMbToB;
  }
  /// @param stats  stats to added
  void addTransferStats(const TransferStats &stats) {
    summary_ += stats;
  }
  /// @param currentThroughput  current throughput
  void setCurrentThroughput(double currentThroughput) {
    currentThroughput_ = currentThroughput;
  }
  void setTotalTime(double totalTime) {
    totalTime_ = totalTime;
  }
  void setTotalFileSize(int64_t totalFileSize) {
    totalFileSize_ = totalFileSize;
  }
  void setErrorCode(const ErrorCode errCode) {
    summary_.setLocalErrorCode(errCode);
    summary_.setRemoteErrorCode(errCode);
  }
  int64_t getNumDiscoveredFiles() const {
    return numDiscoveredFiles_;
  }
  bool fileDiscoveryFinished() const {
    return fileDiscoveryFinished_;
  }
  int64_t getPreviouslySentBytes() const {
    return previouslySentBytes_;
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
  /// directories which could not be opened
  std::vector<std::string> failedDirectories_;
  /// total transfer time
  double totalTime_{0};
  /// sum of all the file sizes
  int64_t totalFileSize_{0};
  /// recent throughput in bytes/sec
  double currentThroughput_{0};
  /// Count of all files discovered so far
  int64_t numDiscoveredFiles_{0};
  /// Number of bytes sent in previous transfers
  int64_t previouslySentBytes_{0};
  /// Is file discovery finished?
  bool fileDiscoveryFinished_{false};
};

/**
 * This class represents interface and default implementation of progress
 * reporting
 */
class ProgressReporter {
 public:
  explicit ProgressReporter(const WdtTransferRequest &transferRequest)
      : transferRequest_(transferRequest) {
    isTty_ = isatty(STDOUT_FILENO);
  }

  /// this method is called before the transfer starts
  virtual void start() {
  }

  /**
   * This method gets called repeatedly with interval defined by
   * progress_report_interval. If stdout is a terminal, then it displays
   * transfer progress in stdout. Example output [===>    ] 30% 5.00 Mbytes/sec.
   * Else, it prints progress details in stdout.
   *
   * @param report                current transfer report
   */
  virtual void progress(const std::unique_ptr<TransferReport> &report);

  /**
   * This method gets called after the transfer ends
   *
   * @param report                final transfer report
   */
  virtual void end(const std::unique_ptr<TransferReport> &report);

  virtual ~ProgressReporter() {
  }

 protected:
  /// Reference to the wdt transfer request for the wdt base
  /// object using the progress reporter
  const WdtTransferRequest &transferRequest_;

 private:
  /**
   * Displays progress of the transfer in stdout
   *
   * @param progress              progress percentage
   * @param throughput            average throughput
   * @param currentThroughput     recent throughput
   * @param numDiscoveredFiles    number of files discovered so far
   * @param fileDiscoveryFinished true once file discovery has compeleted
   */
  void displayProgress(int progress, double averageThroughput,
                       double currentThroughput, int64_t numDiscoveredFiles,
                       bool fileDiscoveryFinished);

  /**
   * logs progress details
   *
   * @param effectiveDataBytes    number of bytes sent
   * @param progress              progress percentage
   * @param throughput            average throughput
   * @param currentThroughput     recent throughput
   * @param numDiscoveredFiles    number of files discovered so far
   * @param fileDiscoveryFinished true once file discovery has compeleted
   */
  void logProgress(int64_t effectiveDataBytes, int progress,
                   double averageThroughput, double currentThroughput,
                   int64_t numDiscoveredFiles, bool fileDiscoveryFinished);

  /// whether stdout is redirected to a terminal or not
  bool isTty_;
};

/// class representing perf stat collection
class PerfStatReport {
 public:
  enum StatType {
    SOCKET_READ,
    SOCKET_WRITE,
    FILE_OPEN,
    FILE_CLOSE,
    FILE_READ,
    FILE_WRITE,
    SYNC_FILE_RANGE,
    FSYNC_STATS,  // just 'FSYNC' is defined on Windows/conflicts
    FILE_SEEK,
    THROTTLER_SLEEP,
    RECEIVER_WAIT_SLEEP,  // receiver sleep duration between sending wait cmd to
                          // sender. A high sum for this suggests threads
                          // were not properly load balanced
    DIRECTORY_CREATE,
    IOCTL,
    UNLINK,
    FADVISE,
    END
  };

  explicit PerfStatReport(const WdtOptions &options);

  /**
   * @param statType      stat-type
   * @param timeInMicros  time taken by the operation in microseconds
   */
  void addPerfStat(StatType statType, int64_t timeInMicros);

  friend std::ostream &operator<<(std::ostream &os,
                                  const PerfStatReport &statReport);
  PerfStatReport &operator+=(const PerfStatReport &statReport);

 private:
  const static int kNumTypes_ = PerfStatReport::END;
  const static std::string statTypeDescription_[];
  const static int32_t kHistogramBuckets[];
  /// mapping from time to number of entries
  std::unordered_map<int64_t, int64_t> perfStats_[kNumTypes_];
  /// max time for different stat types
  int64_t maxValueMicros_[kNumTypes_] = {0};
  /// min time for different stat types
  int64_t minValueMicros_[kNumTypes_] = {std::numeric_limits<int64_t>::max()};
  /// number of records for different stat types
  int64_t count_[kNumTypes_] = {0};
  /// sum of all records for different stat types
  int64_t sumMicros_[kNumTypes_] = {0};
  /// network timeout in milliseconds
  int networkTimeoutMillis_;
  /// mutex to support synchronized access
  mutable folly::RWSpinLock mutex_;
};
}
}
