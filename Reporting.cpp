#include "Reporting.h"
#include <folly/String.h>

#include <iostream>
#include <iomanip>

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
  const double kMbToB = 1024 * 1024;
  double headerOverhead = 0;
  size_t effectiveTotalBytes =
      stats.effectiveHeaderBytes_ + stats.effectiveDataBytes_;
  if (effectiveTotalBytes) {
    headerOverhead = 100.0 * stats.effectiveHeaderBytes_ / effectiveTotalBytes;
  }
  double failureOverhead = 0;
  size_t totalBytes = stats.headerBytes_ + stats.dataBytes_;
  if (totalBytes) {
    failureOverhead = 100.0 * (totalBytes - effectiveTotalBytes) / totalBytes;
  }
  os << "Transfer Status (local) = " << kErrorToStr[stats.errCode_]
     << ", (remote) = " << kErrorToStr[stats.remoteErrCode_]
     << ". Number of files transferred = " << stats.numFiles_
     << ". Data Mbytes = " << stats.effectiveDataBytes_ / kMbToB
     << ". Header kBytes = " << stats.effectiveHeaderBytes_ / 1024. << " ("
     << headerOverhead << "% overhead)"
     << ". Total bytes = " << effectiveTotalBytes
     << ". Wasted bytes due to failure = " << totalBytes - effectiveTotalBytes
     << " (" << failureOverhead << "% overhead).";
  return os;
}

TransferReport::TransferReport(
    std::vector<TransferStats>& transferredSourceStats,
    std::vector<TransferStats>& failedSourceStats,
    std::vector<TransferStats>& threadStats,
    std::vector<std::string>& failedDirectories, double totalTime,
    size_t totalFileSize)
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
  }
}

TransferReport::TransferReport(const std::vector<TransferStats>& threadStats,
                               double totalTime, size_t totalFileSize)
    : totalTime_(totalTime), totalFileSize_(totalFileSize) {
  for (const auto& stats : threadStats) {
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
      int numOfFilesToPrint =
          std::min(kMaxEntriesToPrint, report.failedSourceStats_.size());
      for (int i = 0; i < numOfFilesToPrint; i++) {
        os << report.failedSourceStats_[i].getId() << "\n";
      }
      if (numOfFilesToPrint < report.failedSourceStats_.size()) {
        os << "more...(" << report.failedSourceStats_.size() - numOfFilesToPrint
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
      ;
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
  displayProgress(progress, report->getThroughputMBps());
}

void ProgressReporter::end(const std::unique_ptr<TransferReport>& report) {
  progress(report);
  std::cout << '\n';
  std::cout.flush();
}

void ProgressReporter::displayProgress(int progress, double throughput) {
  int scaledProgress = progress / 2;
  std::cout << '\r';
  std::cout << '[';
  for (int i = 0; i < scaledProgress - 1; i++) {
    std::cout << '=';
  }
  if (scaledProgress != 0) {
    std::cout << (scaledProgress == 50 ? '=' : '>');
  }
  for (int i = 0; i < 50 - scaledProgress - 1; i++) {
    std::cout << ' ';
  }
  std::cout << "] " << progress << "% " << std::setprecision(2) << std::fixed
            << throughput << " Mbytes/sec";
  std::cout.flush();
}
}
}
