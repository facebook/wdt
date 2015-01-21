#include "Sender.h"

#include "ClientSocket.h"
#include "Protocol.h"
#include "Throttler.h"

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>

#include <thread>

// Constants for different calculations
/*
 * If you change any of the multipliers be
 * sure to replace them in the description above.
 */
const double kMbToB = 1024 * 1024;
const double kPeakMultiplier = 1.2;
const int kBucketMultiplier = 2;
const double kTimeMultiplier = 0.25;
namespace {

template <typename T>
double durationSeconds(T d) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

}  // anonymous namespace

namespace facebook {
namespace wdt {

/// default progress reporter
static void progressReporter(const TransferStats &stats,
                             size_t numDiscoveredSources,
                             size_t totalDiscoveredSize, double throughput) {
  int progress = 0;
  if (totalDiscoveredSize > 0) {
    progress = stats.getEffectiveDataBytes() * 100 / totalDiscoveredSize;
  }
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
  if (progress == 100) {
    // transfer finished
    std::cout << '\n';
  }
  std::cout.flush();
}

Sender::Sender(int port, int numSockets, const std::string &destHost,
               const std::string &srcDir)
    : Sender(destHost, srcDir) {
  this->port_ = port;
  this->numSockets_ = numSockets;
}

Sender::Sender(const std::string &destHost, const std::string &srcDir)
    : progressReporter_(&progressReporter) {
  this->destHost_ = destHost;
  this->srcDir_ = srcDir;
  const auto &options = WdtOptions::get();
  this->port_ = options.port_;
  this->numSockets_ = options.numSockets_;
  this->followSymlinks_ = options.followSymlinks_;
  this->includeRegex_ = options.includeRegex_;
  this->excludeRegex_ = options.excludeRegex_;
  this->pruneDirRegex_ = options.pruneDirRegex_;
  this->progressReportIntervalMillis_ = options.progressReportIntervalMillis_;
}

void Sender::setIncludeRegex(const std::string &includeRegex) {
  includeRegex_ = includeRegex;
}

void Sender::setExcludeRegex(const std::string &excludeRegex) {
  excludeRegex_ = excludeRegex;
}

void Sender::setPruneDirRegex(const std::string &pruneDirRegex) {
  pruneDirRegex_ = pruneDirRegex;
}

void Sender::setPort(const int port) {
  port_ = port;
}

void Sender::setNumSockets(const int numSockets) {
  numSockets_ = numSockets;
}

void Sender::setSrcFileInfo(const std::vector<FileInfo> &srcFileInfo) {
  srcFileInfo_ = srcFileInfo;
}

void Sender::setFollowSymlinks(const bool followSymlinks) {
  followSymlinks_ = followSymlinks;
}

void Sender::setProgressReportIntervalMillis(
    const int progressReportIntervalMillis) {
  progressReportIntervalMillis_ = progressReportIntervalMillis;
}

void Sender::setProgressReporter(const ProgressReporter &progressReporter) {
  progressReporter_ = progressReporter;
}

std::unique_ptr<TransferReport> Sender::start() {
  const auto &options = WdtOptions::get();
  const bool twoPhases = options.twoPhases_;
  LOG(INFO) << "Client (sending) to " << destHost_ << " port " << port_ << " : "
            << numSockets_ << " sockets, source dir " << srcDir_;
  auto startTime = Clock::now();
  DirectorySourceQueue queue(srcDir_);
  queue.setIncludePattern(includeRegex_);
  queue.setExcludePattern(excludeRegex_);
  queue.setPruneDirPattern(pruneDirRegex_);
  queue.setFileInfo(srcFileInfo_);
  queue.setFollowSymlinks(followSymlinks_);
  std::thread dirThread = queue.buildQueueAsynchronously();
  std::thread progressReporterThread;
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  double directoryTime;
  if (twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  std::vector<std::thread> vt;
  std::vector<TransferStats> threadStats;
  for (int i = 0; i < numSockets_; i++) {
    threadStats.emplace_back(true);
  }
  std::vector<std::vector<TransferStats>> sourceStats(numSockets_);
  double avgRateBytesPerSec = options.avgMbytesPerSec_ * kMbToB;
  double peakRateBytesPerSec = options.maxMbytesPerSec_ * kMbToB;
  double bucketLimitBytes = options.throttlerBucketLimit_ * kMbToB;
  double perThreadAvgRateBytesPerSec = avgRateBytesPerSec / numSockets_;
  double perThreadPeakRateBytesPerSec = peakRateBytesPerSec / numSockets_;
  double perThreadBucketLimit = bucketLimitBytes / numSockets_;
  if (avgRateBytesPerSec < 1.0 && avgRateBytesPerSec >= 0) {
    LOG(FATAL) << "Realistic average rate"
                  " should be greater than 1.0 bytes/sec";
  }
  if (perThreadPeakRateBytesPerSec < perThreadAvgRateBytesPerSec &&
      perThreadPeakRateBytesPerSec >= 0) {
    LOG(WARNING) << "Per thread peak rate should be greater "
                 << "than per thread average rate. "
                 << "Making peak rate 1.2 times the average rate";
    perThreadPeakRateBytesPerSec =
        kPeakMultiplier * perThreadAvgRateBytesPerSec;
  }
  if (perThreadBucketLimit <= 0 && perThreadPeakRateBytesPerSec > 0) {
    perThreadBucketLimit =
        kTimeMultiplier * kBucketMultiplier * perThreadPeakRateBytesPerSec;
    LOG(INFO) << "Burst limit not specified but peak "
              << "rate is configured. Auto configuring to "
              << perThreadBucketLimit / kMbToB << " mbytes";
  }
  VLOG(1) << "Per thread (Avg Rate, Peak Rate) = "
          << "(" << perThreadAvgRateBytesPerSec << ", "
          << perThreadPeakRateBytesPerSec << ")";
  for (int i = 0; i < numSockets_; i++) {
    threadStats[i].setId(folly::to<std::string>(i));
    vt.emplace_back(&Sender::sendOne, this, startTime, std::ref(destHost_),
                    port_ + i, std::ref(queue), perThreadAvgRateBytesPerSec,
                    perThreadPeakRateBytesPerSec, perThreadBucketLimit,
                    std::ref(threadStats[i]), std::ref(sourceStats[i]));
  }
  if (progressReportEnabled) {
    std::thread reporterThread(&Sender::reportProgress, this, startTime,
                               std::ref(threadStats), std::ref(queue));
    progressReporterThread = std::move(reporterThread);
  }
  if (!twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
  }
  if (progressReportEnabled) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      transferFinished_ = true;
      conditionFinished_.notify_all();
    }
    progressReporterThread.join();
  }

  std::vector<TransferStats> transferredSourceStats;
  if (WdtOptions::get().fullReporting_) {
    for (auto &stats : sourceStats) {
      transferredSourceStats.insert(transferredSourceStats.end(),
                                    std::make_move_iterator(stats.begin()),
                                    std::make_move_iterator(stats.end()));
    }
    validateTransferStats(transferredSourceStats, queue.getFailedSourceStats(),
                          threadStats);
  }
  std::unique_ptr<TransferReport> report = folly::make_unique<TransferReport>(
      transferredSourceStats, queue.getFailedSourceStats(), threadStats);
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Total sender time = " << totalTime << " seconds ("
            << directoryTime << " dirTime)"
            << ". Transfer summary : " << *report
            << "\nTotal sender throughput = "
            << report->getSummary().getEffectiveTotalBytes() / totalTime /
                   kMbToB << " Mbytes/sec ("
            << report->getSummary().getEffectiveTotalBytes() /
                   (totalTime - directoryTime) / kMbToB
            << " Mbytes/sec pure transf rate)";
  return report;
}

void Sender::validateTransferStats(
    const std::vector<TransferStats> &transferredSourceStats,
    const std::vector<TransferStats> &failedSourceStats,
    const std::vector<TransferStats> &threadStats) {
  size_t sourceFailedAttempts = 0;
  size_t sourceNumFiles = transferredSourceStats.size();
  size_t sourceDataBytes = 0;
  size_t sourceEffectiveDataBytes = 0;

  size_t threadFailedAttempts = 0;
  size_t threadNumFiles = 0;
  size_t threadDataBytes = 0;
  size_t threadEffectiveDataBytes = 0;

  for (const auto &stat : transferredSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    WDT_CHECK(stat.getNumFiles() == 1);
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
  }
  for (const auto &stat : failedSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    WDT_CHECK(stat.getNumFiles() == 0);
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
  }
  for (const auto &stat : threadStats) {
    threadFailedAttempts += stat.getFailedAttempts();
    threadNumFiles += stat.getNumFiles();
    threadDataBytes += stat.getDataBytes();
    threadEffectiveDataBytes += stat.getEffectiveDataBytes();
  }

  WDT_CHECK(sourceFailedAttempts == threadFailedAttempts);
  WDT_CHECK(sourceNumFiles == threadNumFiles);
  WDT_CHECK(sourceDataBytes == threadDataBytes);
  WDT_CHECK(sourceEffectiveDataBytes == threadEffectiveDataBytes);
}

std::unique_ptr<ClientSocket> Sender::makeSocket(const std::string &destHost,
                                                 int port) {
  return folly::make_unique<ClientSocket>(destHost,
                                          folly::to<std::string>(port));
}

std::unique_ptr<ClientSocket> Sender::connectToReceiver(
    const std::string &destHost, const int port, ErrorCode &errCode,
    Clock::time_point startTime) {
  const auto &options = WdtOptions::get();
  int connectAttempts = 0;
  std::unique_ptr<ClientSocket> socket = makeSocket(destHost, port);
  for (int i = 1; i <= options.maxRetries_; ++i) {
    ++connectAttempts;
    errCode = socket->connect();
    if (errCode == OK) {
      break;
    } else if (errCode == CONN_ERROR) {
      return nullptr;
    }
    if (i != options.maxRetries_) {
      // sleep between attempts but not after the last
      VLOG(1) << "Sleeping after failed attempt " << i;
      usleep(options.sleepMillis_ * 1000);
    }
  }
  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  if (errCode != OK) {
    LOG(ERROR) << "Unable to connect despite " << connectAttempts
               << " retries in " << elapsedSecsConn << " seconds.";
    errCode = CONN_ERROR;
    return nullptr;
  }
  ((connectAttempts > 1) ? LOG(WARNING) : LOG(INFO))
      << "Connection took " << connectAttempts << " attempt(s) and "
      << elapsedSecsConn << " seconds.";
  return socket;
}

/**
 * @param startTime              Time when this thread was spawned
 * @param destHost               Address to the destination, see ClientSocket
 * @param port                   Port to establish connect
 * @param queue                  DirectorySourceQueue object for reading files
 * @param avgRateBytes           Average rate of throttler in bytes/sec
 * @param maxRateBytes           Peak rate for Token Bucket algorithm in
 bytes/sec
                                 Checkout Throttler.h for Token Bucket
 * @param bucketLimitBytes       Bucket Limit for Token Bucket algorithm in
 bytes
 * @param threadStats            Per thread statistics
 * @param transferredFileStats   Stats for successfully transferred files
 */
void Sender::sendOne(Clock::time_point startTime, const std::string &destHost,
                     int port, DirectorySourceQueue &queue, double avgRateBytes,
                     double maxRateBytes, double bucketLimitBytes,
                     TransferStats &threadStats,
                     std::vector<TransferStats> &transferredFileStats) {
  std::unique_ptr<Throttler> throttler;
  const auto &options = WdtOptions::get();
  const bool doThrottling = (avgRateBytes > 0 || maxRateBytes > 0);
  if (doThrottling) {
    throttler = folly::make_unique<Throttler>(startTime, avgRateBytes,
                                              maxRateBytes, bucketLimitBytes,
                                              options.throttlerLogTimeMillis_);
  } else {
    VLOG(1) << "No throttling in effect";
  }

  ErrorCode code;
  std::unique_ptr<ClientSocket> socket =
      connectToReceiver(destHost, port, code, startTime);
  if (code != OK) {
    threadStats.setErrorCode(code);
    return;
  }
  char headerBuf[Protocol::kMaxHeader];

  while (true) {
    auto source = queue.getNextSource();
    if (!source) {
      break;
    }
    source->open();
    if (options.ignoreOpenErrors_ && source->hasError()) {
      continue;
    }
    TransferStats transferStats;
    size_t totalBytes = threadStats.getTotalBytes();
    transferStats =
        sendOneByteSource(socket, throttler, source, doThrottling, totalBytes);
    threadStats += transferStats;
    source->addTransferStats(transferStats);
    if (transferStats.getErrorCode() == OK) {
      if (options.fullReporting_) {
        transferredFileStats.emplace_back(
            std::move(source->getTransferStats()));
      }
    } else {
      queue.returnToQueue(source);
      if (transferStats.getErrorCode() == SOCKET_WRITE_ERROR) {
        socket = connectToReceiver(destHost, port, code, Clock::now());
        if (code != OK) {
          threadStats.setErrorCode(code);
          return;
        }
      }
    }
  }
  headerBuf[0] = Protocol::DONE_CMD;
  ssize_t written = socket->write(headerBuf, 1);
  if (written != 1) {
    LOG(ERROR) << "Socket write failure " << written;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return;
  }
  threadStats.addHeaderBytes(1);
  threadStats.addEffectiveBytes(1, 0);
  socket->shutdown();
  VLOG(1) << "Wrote done cmd on " << socket->getFd() << " waiting for reply...";
  ssize_t numRead = socket->read(headerBuf, 1);  // TODO: returns 0 on disk full
  if (numRead != 1) {
    LOG(ERROR) << "READ unexpected " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return;
  }
  if (headerBuf[0] != Protocol::DONE_CMD) {
    LOG(ERROR) << "Unexpected reply " << headerBuf[0];
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return;
  }
  numRead = socket->read(headerBuf, Protocol::kMaxHeader);
  if (numRead != 0) {
    LOG(ERROR) << "EOF not found when expected "
               << ((numRead < 0) ? "-1" : folly::humanify(
                                              std::string(headerBuf, numRead)));
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return;
  }
  threadStats.setErrorCode(OK);
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Got reply - all done for fd:" << socket->getFd() << ". "
            << "Transfer stat : " << threadStats << " Total throughput = "
            << threadStats.getEffectiveTotalBytes() / totalTime / kMbToB
            << " Mbytes/sec";
  return;
}

TransferStats Sender::sendOneByteSource(
    const std::unique_ptr<ClientSocket> &socket,
    const std::unique_ptr<Throttler> &throttler,
    const std::unique_ptr<ByteSource> &source, const bool doThrottling,
    const size_t totalBytes) {
  TransferStats stats;
  char headerBuf[Protocol::kMaxHeader];
  size_t off = 0;
  headerBuf[off++] = Protocol::FILE_CMD;
  const size_t expectedSize = source->getSize();
  size_t actualSize = 0;
  Protocol::encode(headerBuf, off, Protocol::kMaxHeader,
                   source->getIdentifier(), expectedSize);
  ssize_t written = socket->write(headerBuf, off);
  if (written != off) {
    LOG(ERROR) << "Write error/mismatch " << written << " " << off;
    stats.setErrorCode(SOCKET_WRITE_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  stats.addHeaderBytes(written);
  VLOG(3) << "Sent " << written << " on " << socket->getFd() << " : "
          << folly::humanify(std::string(headerBuf, off));
  while (!source->finished()) {
    size_t size;
    char *buffer = source->read(size);
    if (source->hasError()) {
      LOG(ERROR) << "Failed reading file " << source->getIdentifier()
                 << " for fd " << socket->getFd();
      break;
    }
    WDT_CHECK(buffer && size > 0);
    written = 0;
    if (doThrottling) {
      /**
       * If throttling is enabled we call limit(totalBytes) which
       * used both the methods of throttling peak and average.
       * Always call it with totalBytes written till now, throttler
       * will do the rest. Total bytes includes header and the data bytes.
       * The throttler was constructed at the time when the header
       * was being written and it is okay to start throttling with the
       * next expected write.
       */
      throttler->limit(totalBytes + stats.getTotalBytes() + size);
    }
    do {
      ssize_t w = socket->write(buffer + written, size - written);
      if (w < 0) {
        // TODO: retries, close connection etc...
        LOG(ERROR) << "Write error " << written << " (" << size << ")";
        stats.setErrorCode(SOCKET_WRITE_ERROR);
        stats.incrFailedAttempts();
        return stats;
      }
      stats.addDataBytes(w);
      written += w;
      if (w != size) {
        VLOG(1) << "Short write " << w << " sub total now " << written << " on "
                << socket->getFd() << " out of " << size;
      } else {
        VLOG(3) << "Wrote all of " << size << " on " << socket->getFd();
      }
    } while (written < size);
    if (written > size) {
      LOG(ERROR) << "Write error " << written << " > " << size;
      stats.setErrorCode(SOCKET_WRITE_ERROR);
      stats.incrFailedAttempts();
      return stats;
    }
    actualSize += written;
  }
  if (actualSize != expectedSize) {
    LOG(ERROR) << "UGH " << source->getIdentifier() << " " << expectedSize
               << " " << actualSize;
    // Can only happen if sender thread can not read complete source byte
    // stream
    stats.setErrorCode(BYTE_SOURCE_READ_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  stats.setErrorCode(OK);
  stats.incrNumFiles();
  stats.addEffectiveBytes(stats.getHeaderBytes(), stats.getDataBytes());
  return stats;
}

void Sender::reportProgress(Clock::time_point startTime,
                            std::vector<TransferStats> &threadStats,
                            DirectorySourceQueue &queue) {
  WDT_CHECK(progressReportIntervalMillis_ > 0);
  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis_);
  bool done = false;
  do {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionFinished_.wait_for(lock, waitingTime);
      done = transferFinished_;
    }
    if (!queue.fileDiscoveryFinished()) {
      continue;
    }
    TransferStats stats;
    for (int index = 0; index < threadStats.size(); index++) {
      stats += threadStats[index];
    }
    auto pair = queue.getCountAndSize();
    double totalTime = durationSeconds(Clock::now() - startTime);
    double throughput = stats.getEffectiveTotalBytes() / totalTime / kMbToB;
    (*progressReporter_)(stats, pair.first, pair.second, throughput);
    if (stats.getEffectiveDataBytes() == pair.second) {
      // transfer finished. this check is needed to ensure progress report
      // callback does not get called with 100% done multiple time. In our
      // default implementation of progress reporter, we print a newline after
      // transfer completion.
      done = true;
    }
  } while (!done);
}
}
}  // namespace facebook::wdt
