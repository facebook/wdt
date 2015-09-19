/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Reporting.h"
#include "WdtOptions.h"
#include <folly/String.h>

#include <iostream>
#include <iomanip>
#include <set>
#include <algorithm>

namespace facebook {
namespace wdt {

const static int64_t kMaxEntriesToPrint = 10;

TransferStats& TransferStats::operator+=(const TransferStats& stats) {
  folly::RWSpinLock::WriteHolder writeLock(mutex_.get());
  folly::RWSpinLock::ReadHolder readLock(stats.mutex_.get());
  headerBytes_ += stats.headerBytes_;
  dataBytes_ += stats.dataBytes_;
  effectiveHeaderBytes_ += stats.effectiveHeaderBytes_;
  effectiveDataBytes_ += stats.effectiveDataBytes_;
  numFiles_ += stats.numFiles_;
  numBlocks_ += stats.numBlocks_;
  failedAttempts_ += stats.failedAttempts_;
  if (stats.errCode_ != OK) {
    if (errCode_ == OK) {
      // First error. Setting this as the error code
      errCode_ = stats.errCode_;
    } else if (stats.errCode_ != errCode_) {
      // Different error than the previous one. Setting error code as generic
      // ERROR
      errCode_ = ERROR;
    }
  }
  if (stats.remoteErrCode_ != OK) {
    if (remoteErrCode_ == OK) {
      remoteErrCode_ = stats.remoteErrCode_;
    } else if (stats.remoteErrCode_ != remoteErrCode_) {
      remoteErrCode_ = ERROR;
    }
  }
  return *this;
}

std::ostream& operator<<(std::ostream& os, const TransferStats& stats) {
  folly::RWSpinLock::ReadHolder lock(stats.mutex_.get());
  double headerOverhead = 100;
  double failureOverhead = 100;

  if (stats.effectiveDataBytes_ > 0) {
    headerOverhead = 100.0 * stats.headerBytes_ / stats.effectiveDataBytes_;
    failureOverhead = 100.0 * (stats.dataBytes_ - stats.effectiveDataBytes_) /
                      stats.effectiveDataBytes_;
  }

  if (stats.errCode_ == OK && stats.remoteErrCode_ == OK) {
    os << "Transfer status = OK.";
  } else {
    os << "Transfer status (local) = " << errorCodeToStr(stats.errCode_)
       << ", (remote) = " << errorCodeToStr(stats.remoteErrCode_) << ".";
  }

  if (stats.numFiles_ > 0) {
    os << " Number of files transferred = " << stats.numFiles_ << ".";
  } else {
    os << " Number of blocks transferred = " << stats.numBlocks_ << ".";
  }
  os << " Data Mbytes = " << stats.effectiveDataBytes_ / kMbToB
     << ". Header kBytes = " << stats.headerBytes_ / 1024. << " ("
     << headerOverhead << "% overhead)"
     << ". Total bytes = " << (stats.dataBytes_ + stats.headerBytes_)
     << ". Wasted bytes due to failure = "
     << (stats.dataBytes_ - stats.effectiveDataBytes_) << " ("
     << failureOverhead << "% overhead).";
  return os;
}

TransferReport::TransferReport(
    std::vector<TransferStats>& transferredSourceStats,
    std::vector<TransferStats>& failedSourceStats,
    std::vector<TransferStats>& threadStats,
    std::vector<std::string>& failedDirectories, double totalTime,
    int64_t totalFileSize, int64_t numDiscoveredFiles)
    : transferredSourceStats_(std::move(transferredSourceStats)),
      failedSourceStats_(std::move(failedSourceStats)),
      threadStats_(std::move(threadStats)),
      failedDirectories_(std::move(failedDirectories)),
      totalTime_(totalTime),
      totalFileSize_(totalFileSize) {
  for (const auto& stats : threadStats_) {
    summary_ += stats;
  }
  summary_.setErrorCode(ERROR);
  if (failedSourceStats_.empty() && failedDirectories_.empty()) {
    // none of the files or directories failed
    for (auto& stats : threadStats_) {
      if (stats.getCombinedErrorCode() == OK) {
        // one thread finished successfully
        summary_.setErrorCode(OK);
        break;
      }
    }
  }
  if (summary_.getEffectiveDataBytes() != totalFileSize_) {
    // sender did not send all the bytes
    LOG(INFO) << "Could not send all the bytes " << totalFileSize_ << " "
              << summary_.getEffectiveDataBytes();
    summary_.setErrorCode(ERROR);
  }
  std::set<std::string> failedFilesSet;
  for (auto& stats : failedSourceStats_) {
    failedFilesSet.insert(stats.getId());
  }
  int64_t numTransferredFiles = numDiscoveredFiles - failedFilesSet.size();
  summary_.setNumFiles(numTransferredFiles);
}

TransferReport::TransferReport(const std::vector<TransferStats>& threadStats,
                               double totalTime, int64_t totalFileSize)
    : totalTime_(totalTime), totalFileSize_(totalFileSize) {
  for (const auto& stats : threadStats) {
    summary_ += stats;
  }
}

TransferReport::TransferReport(std::vector<TransferStats>& threadStats)
    : threadStats_(std::move(threadStats)) {
  for (const auto& stats : threadStats_) {
    summary_ += stats;
  }
}

std::ostream& operator<<(std::ostream& os, const TransferReport& report) {
  os << report.getSummary();
  if (!report.failedSourceStats_.empty()) {
    if (report.summary_.getNumFiles() == 0) {
      os << " All files failed.";
    } else {
      os << "\n"
         << "Failed files :\n";
      std::set<std::string> failedFilesSet;
      for (auto& stats : report.getFailedSourceStats()) {
        failedFilesSet.insert(stats.getId());
      }
      int64_t numFailedFiles = failedFilesSet.size();
      int64_t numOfFilesToPrint =
          std::min<int64_t>(kMaxEntriesToPrint, numFailedFiles);

      int64_t displayCount = 0;
      for (auto& fileName : failedFilesSet) {
        if (displayCount >= numOfFilesToPrint) {
          break;
        }
        os << fileName << "\n";
        displayCount++;
      }

      if (numOfFilesToPrint < numFailedFiles) {
        os << "more...(" << numFailedFiles - numOfFilesToPrint << " files)";
      }
    }
  }
  if (!report.failedDirectories_.empty()) {
    os << "\n"
       << "Failed directories :\n";
    int64_t numFailedDirectories = report.failedDirectories_.size();
    int64_t numOfDirToPrint =
        std::min<int64_t>(kMaxEntriesToPrint, numFailedDirectories);
    for (int64_t i = 0; i < numOfDirToPrint; i++) {
      os << report.failedDirectories_[i] << "\n";
    }
    if (numOfDirToPrint < numFailedDirectories) {
      os << "more...(" << numFailedDirectories - numOfDirToPrint
         << " directories)";
    }
  }
  return os;
}

void ProgressReporter::progress(const std::unique_ptr<TransferReport>& report) {
  const TransferStats& stats = report->getSummary();
  int64_t totalDiscoveredSize = report->getTotalFileSize();
  int progress = 0;
  if (totalDiscoveredSize > 0) {
    progress = stats.getEffectiveDataBytes() * 100 / totalDiscoveredSize;
  }
  if (isTty_) {
    displayProgress(progress, report->getThroughputMBps(),
                    report->getCurrentThroughputMBps());
  } else {
    logProgress(stats.getEffectiveDataBytes(), progress,
                report->getThroughputMBps(),
                report->getCurrentThroughputMBps());
  }
}

void ProgressReporter::end(const std::unique_ptr<TransferReport>& report) {
  progress(report);
  if (isTty_) {
    std::cout << '\n';
    std::cout.flush();
  }
}

void ProgressReporter::displayProgress(int progress, double averageThroughput,
                                       double currentThroughput) {
  int scaledProgress = progress / 2;
  std::cout << '\r';
  std::cout << '[';
  for (int i = 0; i < scaledProgress - 1; i++) {
    std::cout << '=';
  }
  if (scaledProgress != 0 && scaledProgress != 50) {
    std::cout << '>';
  }
  for (int i = 0; i < 50 - scaledProgress - 1; i++) {
    std::cout << ' ';
  }
  std::cout << "] " << progress << "% " << std::setprecision(1) << std::fixed
            << averageThroughput;
  if (progress < 100) {
    std::cout << " " << currentThroughput << " Mbytes/s  ";
  } else {
    std::cout << " Mbytes/s          ";
  }
  std::cout.flush();
}

void ProgressReporter::logProgress(int64_t effectiveDataBytes, int progress,
                                   double averageThroughput,
                                   double currentThroughput) {
  LOG(INFO) << "wdt transfer progress " << (effectiveDataBytes / kMbToB)
            << " Mb, completed " << progress << "%, Average throughput "
            << averageThroughput << " Mbps, Recent throughput "
            << currentThroughput << "Mbps.";
}

folly::ThreadLocalPtr<PerfStatReport> perfStatReport;

const std::string PerfStatReport::statTypeDescription_[] = {
    "Socket Read", "Socket Write",    "File Open",          "File Close",
    "File Read",   "File Write",      "Sync File Range",    "fsync",
    "File Seek",   "Throttler Sleep", "Receiver Wait Sleep"};

PerfStatReport::PerfStatReport() {
  static_assert(
      sizeof(statTypeDescription_) / sizeof(statTypeDescription_[0]) ==
          PerfStatReport::END,
      "Mismatch between number of stat types and number of descriptions");
  const auto& options = WdtOptions::get();
  networkTimeoutMillis_ =
      std::min<int>(options.read_timeout_millis, options.write_timeout_millis);
}

/**
 *  Semi log bucket definitions covering 5 order of magnitude (more
 *  could be added) with high resolution in small numbers and relatively
 *  small number of total buckets
 *  For efficiency a look up table is created so the last value shouldn't
 *  be too large (or will incur large memory overhead)
 *  value between   [ bucket(i-1), bucket(i) [ go in slot i
 *  plus every value > bucket(last) in last bucket
 */
const int32_t PerfStatReport::kHistogramBuckets[] = {
    1,     2,     3,     4,     5,     6,
    7,     8,     9,     10,    11,          // by 1
    12,    14,    16,    18,    20,          // by 2
    25,    30,    35,    40,    45,    50,   // by 5
    60,    70,    80,    90,    100,         // by 10
    120,   140,   160,   180,   200,         // line2 *10
    250,   300,   350,   400,   450,   500,  // line3 *10
    600,   700,   800,   900,   1000,        // line4 *10
    2000,  3000,  4000,  5000,  7500,  10000,
    20000, 30000, 40000, 50000, 75000, 100000};

void PerfStatReport::addPerfStat(StatType statType, int64_t timeInMicros) {
  int64_t timeInMillis = timeInMicros / kMicroToMilli;
  if (timeInMicros >= networkTimeoutMillis_ * 750) {
    LOG(WARNING) << statTypeDescription_[statType] << " system call took "
                 << timeInMillis << " ms";
  }
  perfStats_[statType][timeInMillis]++;
  maxValueMicros_[statType] =
      std::max<int64_t>(maxValueMicros_[statType], timeInMicros);
  minValueMicros_[statType] =
      std::min<int64_t>(minValueMicros_[statType], timeInMicros);
  count_[statType]++;
  sumMicros_[statType] += timeInMicros;
}

PerfStatReport& PerfStatReport::operator+=(const PerfStatReport& statReport) {
  for (int i = 0; i < kNumTypes_; i++) {
    for (const auto& pair : statReport.perfStats_[i]) {
      int64_t key = pair.first;
      int64_t value = pair.second;
      perfStats_[i][key] += value;
    }
    maxValueMicros_[i] =
        std::max<int64_t>(maxValueMicros_[i], statReport.maxValueMicros_[i]);
    minValueMicros_[i] =
        std::min<int64_t>(minValueMicros_[i], statReport.minValueMicros_[i]);
    count_[i] += statReport.count_[i];
    sumMicros_[i] += statReport.sumMicros_[i];
  }
  return *this;
}

std::ostream& operator<<(std::ostream& os, const PerfStatReport& statReport) {
  const auto& options = WdtOptions::get();
  os << "\n***** PERF STATS *****\n";
  for (int i = 0; i < PerfStatReport::kNumTypes_; i++) {
    if (statReport.count_[i] == 0) {
      continue;
    }
    double max = statReport.maxValueMicros_[i] / kMicroToMilli;
    double min = statReport.minValueMicros_[i] / kMicroToMilli;
    double sumPerThread =
        (statReport.sumMicros_[i] / kMicroToMilli / options.num_ports);
    double avg = (((double)statReport.sumMicros_[i]) / statReport.count_[i] /
                  kMicroToMilli);

    os << std::fixed << std::setprecision(2);
    os << statReport.statTypeDescription_[i] << " : ";
    os << "Ncalls " << statReport.count_[i] << " Stats in ms : SumPerThread "
       << sumPerThread << " Min " << min << " Max " << max << " Avg " << avg
       << " ";

    // One extra bucket for values extending beyond last bucket
    int numBuckets = 1 +
                     sizeof(PerfStatReport::kHistogramBuckets) /
                         sizeof(PerfStatReport::kHistogramBuckets[0]);
    std::vector<int64_t> buckets(numBuckets);

    auto& perfStatMap = statReport.perfStats_[i];
    std::vector<int64_t> timesInMillis;
    for (const auto& pair : perfStatMap) {
      timesInMillis.emplace_back(pair.first);
    }
    std::sort(timesInMillis.begin(), timesInMillis.end());
    int currentBucketIndex = 0;

    int64_t runningCount = 0;
    int64_t p50Count = statReport.count_[i] * 0.50;
    int64_t p95Count = statReport.count_[i] * 0.95;
    int64_t p99Count = statReport.count_[i] * 0.99;

    for (auto time : timesInMillis) {
      WDT_CHECK(time >= 0) << time;
      int64_t count = perfStatMap.find(time)->second;

      if (p50Count > runningCount && p50Count <= runningCount + count) {
        os << "p50 " << time << " ";
      }
      if (p95Count > runningCount && p95Count <= runningCount + count) {
        os << "p95 " << time << " ";
      }
      if (p99Count > runningCount && p99Count <= runningCount + count) {
        os << "p99 " << time << "\n";
      }
      runningCount += count;

      while (currentBucketIndex < numBuckets - 1 &&
             time >= PerfStatReport::kHistogramBuckets[currentBucketIndex]) {
        currentBucketIndex++;
      }
      buckets[currentBucketIndex] += count;
    }

    for (int i = 0; i < numBuckets; i++) {
      if (buckets[i] == 0) {
        continue;
      }
      int64_t bucketStart =
          (i == 0 ? 0 : PerfStatReport::kHistogramBuckets[i - 1]);
      int64_t bucketEnd =
          (i < numBuckets - 1 ? PerfStatReport::kHistogramBuckets[i]
                              : std::numeric_limits<int64_t>::max());
      os << "[" << bucketStart << ", " << bucketEnd << ") --> " << buckets[i]
         << '\n';
    }
  }
  return os;
}
}
}
