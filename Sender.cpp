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

Sender::Sender(int port, int numSockets, const std::string &destHost,
               const std::string &srcDir)
    : Sender(destHost, srcDir) {
  this->port_ = port;
  this->numSockets_ = numSockets;
}
Sender::Sender(const std::string &destHost, const std::string &srcDir) {
  this->destHost_ = destHost;
  this->srcDir_ = srcDir;
  const auto &options = WdtOptions::get();
  this->port_ = options.port_;
  this->numSockets_ = options.numSockets_;
  this->followSymlinks_ = options.followSymlinks_;
  this->includeRegex_ = options.includeRegex_;
  this->excludeRegex_ = options.excludeRegex_;
  this->pruneDirRegex_ = options.pruneDirRegex_;
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

std::vector<ErrorCode> Sender::start() {
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
  double directoryTime;
  if (twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  std::vector<std::thread> vt;
  std::vector<SendStats> stats(numSockets_);
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
    vt.emplace_back(&Sender::sendOne, this, startTime, std::ref(destHost_),
                    port_ + i, std::ref(queue), perThreadAvgRateBytesPerSec,
                    perThreadPeakRateBytesPerSec, perThreadBucketLimit,
                    std::ref(stats[i]));
  }
  if (!twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  size_t totalDataBytes = 0;
  size_t totalHeaderBytes = 0;
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
    totalHeaderBytes += stats[i].headerBytes;
    totalDataBytes += stats[i].dataBytes;
  }
  size_t totalBytes = totalDataBytes + totalHeaderBytes;
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(WARNING) << "All data (" << queue.count() << " files) transfered in "
               << totalTime << " seconds (" << directoryTime
               << " dirtime). Data Mbytes = " << totalDataBytes / kMbToB
               << ". Header kbytes = " << totalHeaderBytes / 1024. << " ("
               << 100. * totalHeaderBytes / totalBytes << "% overhead)"
               << ". Total bytes = " << totalBytes
               << ". Total throughput = " << totalBytes / totalTime / kMbToB
               << " Mbytes/sec ("
               << totalBytes / (totalTime - directoryTime) / kMbToB
               << " Mbytes/sec pure transf rate)";
  std::vector<ErrorCode> errCodes;
  for (auto stat : stats) {
    errCodes.emplace_back(stat.errCode);
  }
  return errCodes;
}

std::unique_ptr<ClientSocket> Sender::makeSocket(const std::string &destHost,
                                                 int port) {
  return folly::make_unique<ClientSocket>(destHost,
                                          folly::to<std::string>(port));
}

/**
 * @param startTime         Time when this thread was spawned
 * @param destHost          Address to the destination, see ClientSocket
 * @param port              Port to establish connect
 * @param queue             DirectorySourceQueue object for reading files
 * @param avgRateBytes      Average rate of throttler in bytes/sec
 * @param maxRateBytes      Peak rate for Token Bucket algorithm in bytes/sec
                            Checkout Throttler.h for Token Bucket
 * @param bucketLimitBytes  Bucket Limit for Token Bucket algorithm in bytes
 * @param stat              Pointer to return send statistics
 */
void Sender::sendOne(Clock::time_point startTime, const std::string &destHost,
                     int port, DirectorySourceQueue &queue, double avgRateBytes,
                     double maxRateBytes, double bucketLimitBytes,
                     SendStats &stat) {
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
  std::unique_ptr<ClientSocket> socket = makeSocket(destHost, port);
  size_t headerBytes = 0, dataBytes = 0, totalBytes = 0;
  size_t numFiles = 0;
  ErrorCode code;
  int connectAttempts = 0;
  for (int i = 1; i < options.maxRetries_; ++i) {
    ++connectAttempts;
    code = socket->connect();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      stat.errCode = code;
      return;
    }
    VLOG(1) << "Sleeping after failed attempt " << i;
    usleep(options.sleepMillis_ * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (code != OK) {
    ++connectAttempts;
    if (socket->connect() != OK) {
      LOG(ERROR) << "Unable to connect despite retries";
      stat.errCode = CONN_ERROR;
      return;
    }
  }
  char headerBuf[Protocol::kMaxHeader];
  std::unique_ptr<ByteSource> source;

  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  ((connectAttempts>1) ? LOG(WARNING) : LOG(INFO))
    << "Connection took " << connectAttempts << " attempt(s) and "
    << elapsedSecsConn << " seconds.";
  while ((source = queue.getNextSource())) {
    if (options.ignoreOpenErrors_ && source->hasError()) {
      continue;
    }
    ++numFiles;
    auto sendStats =
        sendOneByteSource(socket, throttler, source, doThrottling, totalBytes);
    if (sendStats.errCode != OK) {
      stat.errCode = sendStats.errCode;
      return;
    }
    totalBytes = sendStats.totalBytes;
    headerBytes += sendStats.headerBytes;
    dataBytes += sendStats.dataBytes;
  }
  WDT_CHECK(totalBytes == headerBytes + dataBytes);
  headerBuf[0] = Protocol::DONE_CMD;
  socket->write(headerBuf, 1);  //< TODO check for status/succes
  ++headerBytes;
  socket->shutdown();
  VLOG(1) << "Wrote done cmd on " << socket->getFd() << " waiting for reply...";
  ssize_t numRead = socket->read(headerBuf, 1);  // TODO: returns 0 on disk full
  if (numRead != 1) {
    LOG(ERROR) << "READ unexpected " << numRead << ":"
               << folly::humanify(std::string(headerBuf, numRead));
    stat.errCode = SOCKET_READ_ERROR;
    return;
  }
  if (headerBuf[0] != Protocol::DONE_CMD) {
    LOG(ERROR) << "Unexpected reply " << headerBuf[0];
    stat.errCode = PROTOCOL_ERROR;
    return;
  }
  numRead = socket->read(headerBuf, Protocol::kMaxHeader);
  if (numRead != 0) {
    LOG(ERROR) << "EOF not found when expected " << numRead << ":"
               << folly::humanify(std::string(headerBuf, numRead));
    stat.errCode = SOCKET_READ_ERROR;
    return;
  }
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Got reply - all done for fd:" << socket->getFd()
            << ". Number of files = " << numFiles
            << ". Total time = " << totalTime << " (" << elapsedSecsConn
            << " in connection)"
            << ". Avg file size = " << 1. * dataBytes / numFiles
            << ". Data bytes = " << dataBytes
            << ". Header bytes = " << headerBytes << " ( "
            << 100. * headerBytes / totalBytes << " % overhead)"
            << ". Total bytes = " << totalBytes
            << ". Total throughput = " << totalBytes / totalTime / kMbToB
            << " Mbytes/sec";
  stat.headerBytes = headerBytes;
  stat.dataBytes = dataBytes;
  stat.errCode = OK;
  return;
}

Sender::SendStats Sender::sendOneByteSource(
    const std::unique_ptr<ClientSocket> &socket,
    const std::unique_ptr<Throttler> &throttler,
    const std::unique_ptr<ByteSource> &source, const bool doThrottling,
    const size_t totalBytes) {
  SendStats stats;
  stats.totalBytes = totalBytes;
  char headerBuf[Protocol::kMaxHeader];
  size_t off = 0;
  headerBuf[off++] = Protocol::FILE_CMD;
  const size_t expectedSize = source->getSize();
  size_t actualSize = 0;
  Protocol::encode(headerBuf, off, Protocol::kMaxHeader,
                   source->getIdentifier(), expectedSize);
  ssize_t written = socket->write(headerBuf, off);
  stats.headerBytes += written;
  stats.totalBytes += written;
  if (written != off) {
    LOG(ERROR) << "Write error/mismatch " << written << " " << off;
    stats.errCode = SOCKET_WRITE_ERROR;
    return stats;
  }
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
    stats.totalBytes += size;
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
      throttler->limit(stats.totalBytes);
    }
    do {
      ssize_t w = socket->write(buffer + written, size - written);
      if (w < 0) {
        // TODO: retries, close connection etc...
        LOG(ERROR) << "Write error " << written << " (" << size << ")";
        stats.errCode = SOCKET_WRITE_ERROR;
        return stats;
      }
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
      stats.errCode = SOCKET_WRITE_ERROR;
      return stats;
    }
    actualSize += written;
  }
  stats.dataBytes += actualSize;
  if (actualSize != expectedSize) {
    LOG(ERROR) << "UGH " << source->getIdentifier() << " " << expectedSize
               << " " << actualSize;
    // Can only happen if sender thread can not read complete source byte
    // stream
    stats.errCode = BYTE_SOURCE_READ_ERROR;
    return stats;
  }
  stats.errCode = OK;
  return stats;
}
}
}  // namespace facebook::wdt
