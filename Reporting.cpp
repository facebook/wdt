#include "Reporting.h"
#include "WdtOptions.h"
#include <folly/String.h>
#include <folly/stats/Histogram.h>
#include <folly/stats/Histogram-defs.h>

#include <iostream>
#include <iomanip>
#include <set>
#include <algorithm>

namespace facebook {
namespace wdt {

const static size_t kMaxEntriesToPrint = 10;

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
    size_t totalFileSize, size_t numDiscoveredFiles)
    : transferredSourceStats_(std::move(transferredSourceStats)),
      failedSourceStats_(std::move(failedSourceStats)),
      threadStats_(std::move(threadStats)),
      failedDirectories_(std::move(failedDirectories)),
      totalTime_(totalTime),
      totalFileSize_(totalFileSize) {
  for (const auto& stats : threadStats_) {
    summary_ += stats;
  }
  if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
    summary_.setErrorCode(ERROR);
  } else {
    summary_.setErrorCode(OK);
  }
  std::set<std::string> failedFilesSet;
  for (auto& stats : failedSourceStats_) {
    failedFilesSet.insert(stats.getId());
  }
  size_t numTransferredFiles = numDiscoveredFiles - failedFilesSet.size();
  summary_.setNumFiles(numTransferredFiles);
}

TransferReport::TransferReport(const std::vector<TransferStats>& threadStats,
                               double totalTime, size_t totalFileSize)
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
      int numOfFilesToPrint =
          std::min(kMaxEntriesToPrint, failedFilesSet.size());

      int displayCount = 0;
      for (auto& fileName : failedFilesSet) {
        if (displayCount >= numOfFilesToPrint) {
          break;
        }
        os << fileName << "\n";
        displayCount++;
      }

      if (numOfFilesToPrint < failedFilesSet.size()) {
        os << "more...(" << failedFilesSet.size() - numOfFilesToPrint
           << " files)";
      }
    }
  }
  if (!report.failedDirectories_.empty()) {
    os << "\n"
       << "Failed directories :\n";
    int numOfDirToPrint =
        std::min(kMaxEntriesToPrint, report.failedDirectories_.size());
    for (int i = 0; i < numOfDirToPrint; i++) {
      os << report.failedDirectories_[i] << "\n";
    }
    if (numOfDirToPrint < report.failedDirectories_.size()) {
      os << "more...(" << report.failedDirectories_.size() - numOfDirToPrint
         << " directories)";
    }
  }
  return os;
}

void ProgressReporter::progress(const std::unique_ptr<TransferReport>& report) {
  const TransferStats& stats = report->getSummary();
  size_t totalDiscoveredSize = report->getTotalFileSize();
  int progress = 0;
  if (totalDiscoveredSize > 0) {
    progress = stats.getEffectiveDataBytes() * 100 / totalDiscoveredSize;
  }
  displayProgress(progress, report->getThroughputMBps(),
                  report->getCurrentThroughputMBps());
}

void ProgressReporter::end(const std::unique_ptr<TransferReport>& report) {
  progress(report);
  std::cout << '\n';
  std::cout.flush();
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

folly::ThreadLocalPtr<PerfStatReport> perfStatReport;

const std::string PerfStatReport::statTypeDescription_[] = {
    "Socket Read",     "Socket Write", "File Open",
    "File Close",      "File Read",    "File Write",
    "Sync File Range", "File Seek",    "Throttler Sleep"};

void PerfStatReport::addPerfStat(StatType statType, int64_t timeInMicros) {
  int64_t timeInMillis = timeInMicros / kMicroToMilli;
  perfStats_[statType][timeInMillis]++;
  maxValueMillis_[statType] =
      std::max<int64_t>(maxValueMillis_[statType], timeInMillis);
  minValueMillis_[statType] =
      std::min<int64_t>(minValueMillis_[statType], timeInMillis);
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
    maxValueMillis_[i] =
        std::max<int64_t>(maxValueMillis_[i], statReport.maxValueMillis_[i]);
    minValueMillis_[i] =
        std::min<int64_t>(minValueMillis_[i], statReport.minValueMillis_[i]);
    count_[i] += statReport.count_[i];
    sumMicros_[i] += statReport.sumMicros_[i];
  }
  return *this;
}

std::ostream& operator<<(std::ostream& os, const PerfStatReport& statReport) {
  const auto& options = WdtOptions::get();
  const int numBuckets = options.perf_stat_num_buckets;
  WDT_CHECK(numBuckets > 0) << "Number of buckets must be greater than zero";
  os << "\n***** PERF STATS *****\n";
  for (int i = 0; i < PerfStatReport::kNumTypes_; i++) {
    if (statReport.count_[i] == 0) {
      continue;
    }
    const auto& perfStatMap = statReport.perfStats_[i];
    os << statReport.statTypeDescription_[i] << '\n';
    os << "Number of calls " << statReport.count_[i] << '\n';
    os << "Total time spent per thread "
       << (statReport.sumMicros_[i] / kMicroToMilli / options.num_ports)
       << " ms\n";
    os << "Avg " << (((double)statReport.sumMicros_[i]) / statReport.count_[i] /
                     kMicroToMilli) << " ms\n";
    int64_t max = statReport.maxValueMillis_[i];
    int64_t min = statReport.minValueMillis_[i];
    if (min == max) {
      // only one value
      auto it = perfStatMap.find(max);
      WDT_CHECK(it != perfStatMap.end());
      os << "[" << min << ", " << max << "] ==> " << it->second << "\n";
      continue;
    }
    int64_t range = max - min + 1;
    if (range % numBuckets != 0) {
      range = (range / numBuckets + 1) * numBuckets;
    }
    int64_t bucketSize = range / numBuckets;
    folly::Histogram<int64_t> hist(bucketSize, min, max + 1);
    for (const auto& pair : perfStatMap) {
      hist.addRepeatedValue(pair.first, pair.second);
    }
    os << "p99 time " << hist.getPercentileEstimate(0.99) << " ms\n";
    for (int bucketId = 0; bucketId < hist.getNumBuckets(); bucketId++) {
      int64_t numSamplesInBucket = hist.getBucketByIndex(bucketId).count;
      if (numSamplesInBucket == 0) {
        continue;
      }
      os << "(" << hist.getBucketMin(bucketId) << ", "
         << hist.getBucketMax(bucketId) << "] --> " << numSamplesInBucket
         << "\n";
    }
  }
  return os;
}
}
}
