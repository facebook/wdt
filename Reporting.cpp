#include "Reporting.h"
#include <folly/String.h>

namespace facebook {
namespace wdt {

TransferStats& TransferStats::operator+=(const TransferStats& stats) {
  headerBytes_ += stats.getHeaderBytes();
  dataBytes_ += stats.getDataBytes();
  effectiveHeaderBytes_ += stats.getEffectiveHeaderBytes();
  effectiveDataBytes_ += stats.getEffectiveDataBytes();
  numFiles_ += stats.getNumFiles();
  failedAttempts_ += stats.getFailedAttempts();
  if (stats.getErrorCode() != OK) {
    if (errCode_ == OK) {
      // First error. Setting this as the error code
      errCode_ = stats.getErrorCode();
    } else if (stats.getErrorCode() != errCode_) {
      // Different error than the previous one. Setting error code as generic
      // ERROR
      errCode_ = ERROR;
    }
  }
  return *this;
}

std::ostream& operator<<(std::ostream& os, const TransferStats& stats) {
  const double kMbToB = 1024 * 1024;
  double headerOverhead = 0;
  if (stats.getEffectiveTotalBytes()) {
    headerOverhead = 100.0 * stats.getEffectiveHeaderBytes() /
                     stats.getEffectiveTotalBytes();
  }
  double failureOverhead = 0;
  if (stats.getTotalBytes()) {
    failureOverhead = 100.0 *
                      (stats.getTotalBytes() - stats.getEffectiveTotalBytes()) /
                      stats.getTotalBytes();
  }
  os << "Transfer Status = " << kErrorToStr[stats.getErrorCode()]
     << ". Number of files = " << stats.getNumFiles()
     << ". Data Mbytes = " << stats.getEffectiveDataBytes() / kMbToB
     << ". Header kBytes = " << stats.getEffectiveHeaderBytes() / 1024. << " ("
     << headerOverhead << "% overhead)"
     << ". Total bytes = " << stats.getEffectiveTotalBytes()
     << ". Wasted bytes due to failure = "
     << stats.getTotalBytes() - stats.getEffectiveTotalBytes() << " ("
     << failureOverhead << "% overhead).";
  return os;
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
          std::min(size_t(10), report.failedSourceStats_.size());
      for (int i = 0; i < numOfFilesToPrint; i++) {
        os << report.failedSourceStats_[i].getId() << "\n";
      }
      if (numOfFilesToPrint < report.failedSourceStats_.size()) {
        os << "more...";
      }
    }
  }
  return os;
}
}
}
