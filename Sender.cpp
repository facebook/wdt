#include "Sender.h"
#include "ClientSocket.h"
#include "Protocol.h"
#include "Throttler.h"

#include <folly/Conv.h>
#include <folly/String.h>
#include <thread>
DECLARE_int32(buffer_size);
DECLARE_int32(max_retries);
DECLARE_int32(sleep_ms);
DEFINE_double(avg_mbytes_per_sec, -1,
              "Target transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited");
DEFINE_double(max_mbytes_per_sec, 0,
              "Peak transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited and 0 for auto configure. "
              "In auto configure mode peak rate will be 1.2 "
              "times average rate");
DEFINE_double(bucket_limit, 0,
              "Limit of burst in mbytes to control how "
              "much data you can send at unlimited speed. Unless "
              "you specify a peak rate of -1, wdt will either use "
              "your burst limit (if not 0) or max burst possible at a time "
              "will be 2 times the data allowed in "
              "1/4th seconds at peak rate");
DEFINE_bool(ignore_open_errors, false, "will continue despite open errors");
DEFINE_bool(two_phases, false, "do directory discovery first/separately");
// Constants for different calculations
/*
 * If you change any of the multipliers be
 * sure to replace them in the description above.
 */
const double kMbToB = 1024 * 1024;
const double kPeakMultiplier = 1.2;
const int kBucketMultiplier = 2;
const double kTimeMultiplier = 0.25;
/**
 * avg_mbytes_per_sec Rate at which we would like data to be transferred,
 *                    specifying this as < 0 makes it unlimited
 * max_mbytes_per_sec Rate at which tokens will be generated in TB algorithm
 *                    When we lag behind, this will allow us to go faster,
 *                    specify as < 0 to make it unlimited. Specify as 0
 *                    for auto configuring.
 *                    auto conf as (kPeakMultiplier * avg_mbytes_per_sec)
 * bucket_limit       Maximum bucket size of TB algorithm in mbytes
 *                    This together with max_bytes_per_sec will
 *                    make for maximum burst rate. If specified as 0 it is
 *                    kBucketMultiplier * kTimeMultiplier * max_mbytes_per_sec
 */
namespace {

template <typename T>
double durationSeconds(T d) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

}  // anonymous namespace

namespace facebook {
namespace wdt {

Sender::Sender(const std::string &destHost, int port, int numSockets,
               const std::string &srcDir,
               const std::vector<FileInfo> &srcFileInfo)
    : destHost_(destHost),
      port_(port),
      numSockets_(numSockets),
      srcDir_(srcDir),
      srcFileInfo_(srcFileInfo) {
}

void Sender::start() {
  const bool twoPhases = FLAGS_two_phases;
  const size_t bufferSize = FLAGS_buffer_size;
  LOG(INFO) << "Client (sending) to " << destHost_ << " port " << port_ << " : "
            << numSockets_ << " sockets, source dir " << srcDir_;
  auto startTime = Clock::now();
  DirectorySourceQueue queue(srcDir_, bufferSize, srcFileInfo_);
  std::thread dirThread = queue.buildQueueAsynchronously();
  double directoryTime;
  if (twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  std::vector<std::thread> vt;
  size_t headerBytes[numSockets_];
  size_t dataBytes[numSockets_];
  double avgRateBytesPerSec = FLAGS_avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = FLAGS_max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = FLAGS_bucket_limit * kMbToB;
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
    dataBytes[i] = 0;
    headerBytes[i] = 0;
    vt.emplace_back(&Sender::sendOne, this, startTime, destHost_, port_ + i,
                    &queue, &headerBytes[i], &dataBytes[i],
                    perThreadAvgRateBytesPerSec, perThreadPeakRateBytesPerSec,
                    perThreadBucketLimit);
  }
  if (!twoPhases) {
    dirThread.join();
    directoryTime = durationSeconds(Clock::now() - startTime);
  }
  size_t totalDataBytes = 0;
  size_t totalHeaderBytes = 0;
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
    totalHeaderBytes += headerBytes[i];
    totalDataBytes += dataBytes[i];
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
}
/**
 * @param startTime         Time when this thread was spawned
 * @param destHost          Address to the destination, see ClientSocket
 * @param port              Port to establish connect
 * @param queue             DirectorySourceQueue object for reading files
 * @param pHeaderBytes      Pointer to the size_t which will reflect the
 *                          total bytes sent in the header
 * @param pDataBytes        Pointer to the size_t which will reflect the actual
 *                          data sent in bytes
 * @param avgRateBytes      Average rate of throttler in bytes/sec
 * @param maxRateBytes      Peak rate for Token Bucket algorithm in bytes/sec
                            Checkout Throttler.h for Token Bucket
 * @param bucketLimitBytes  Bucket Limit for Token Bucket algorithm in bytes
 */
void Sender::sendOne(Clock::time_point startTime, const std::string &destHost,
                     int port, DirectorySourceQueue *queue,
                     size_t *pHeaderBytes, size_t *pDataBytes,
                     double avgRateBytes, double maxRateBytes,
                     double bucketLimitBytes) {
  Throttler throttler(startTime, avgRateBytes, maxRateBytes, bucketLimitBytes);
  const bool doThrottling = (avgRateBytes > 0 || maxRateBytes > 0);
  if (!doThrottling) {
    LOG(INFO) << "No throttling in effect";
  }
  size_t headerBytes = 0, dataBytes = 0, totalBytes = 0;
  size_t numFiles = 0;
  ClientSocket s(destHost, folly::to<std::string>(port));
  for (int i = 1; i < FLAGS_max_retries; ++i) {
    if (s.connect()) {
      break;
    }
    VLOG(1) << "Sleeping after failed attempt " << i;
    usleep(FLAGS_sleep_ms * 1000);
  }
  // one more/last try (stays true if it worked above)
  CHECK(s.connect()) << "Unable to connect despite retries";
  int fd = s.getFd();
  char headerBuf[Protocol::kMaxHeader];
  std::unique_ptr<ByteSource> source;

  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  VLOG(1) << "Connect took " << elapsedSecsConn;
  while ((source = queue->getNextSource())) {
    if (FLAGS_ignore_open_errors && source->hasError()) {
      continue;
    }
    ++numFiles;
    size_t off = 0;
    headerBuf[off++] = Protocol::FILE_CMD;
    const size_t expectedSize = source->getSize();
    size_t actualSize = 0;
    bool success = Protocol::encode(headerBuf, off, Protocol::kMaxHeader,
                                    source->getIdentifier(), expectedSize);
    CHECK(success);
    ssize_t written = write(fd, headerBuf, off);
    headerBytes += written;
    totalBytes += written;
    if (written != off) {
      PLOG(FATAL) << "Write error/mismatch " << written << " " << off;
    }
    VLOG(3) << "Sent " << written << " on " << fd << " : "
            << folly::humanify(std::string(headerBuf, off));
    while (!source->finished()) {
      size_t size;
      char *buffer = source->read(size);
      if (source->hasError()) {
        LOG(ERROR) << "Failed reading file " << source->getIdentifier()
                   << " for fd " << fd;
        break;
      }
      CHECK(buffer && size > 0);
      written = 0;
      totalBytes += size;
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
        throttler.limit(totalBytes);
      }
      do {
        ssize_t w = write(fd, buffer + written, size - written);
        if (w < 0) {
          // TODO: retries, close connection etc...
          PLOG(FATAL) << "Write error " << written << " (" << size << ")";
        }
        written += w;
        if (w != size) {
          VLOG(1) << "Short write " << w << " sub total now " << written
                  << " on " << fd << " out of " << size;
        } else {
          VLOG(3) << "Wrote all of " << size << " on " << fd;
        }
      } while (written < size);
      if (written > size) {
        PLOG(FATAL) << "Write error " << written << " > " << size;
      }
      actualSize += written;
    }
    dataBytes += actualSize;
    if (actualSize != expectedSize) {
      LOG(FATAL) << "UGH " << source->getIdentifier() << " " << expectedSize
                 << " " << actualSize;
    }
  }
  CHECK(totalBytes == headerBytes + dataBytes);
  headerBuf[0] = Protocol::DONE_CMD;
  write(fd, headerBuf, 1);  //< TODO check for status/succes
  ++headerBytes;
  shutdown(fd, SHUT_WR);  //< TODO check for status/succes
  VLOG(1) << "Wrote done cmd on " << fd << " waiting for reply...";
  ssize_t numRead = read(fd, headerBuf, 1);  // TODO: returns 0 on disk full
  CHECK(numRead == 1) << "READ unexpected " << numRead << ":"
                      << folly::humanify(std::string(headerBuf, numRead));
  CHECK(headerBuf[0] == Protocol::DONE_CMD) << "Unexpected reply "
                                            << headerBuf[0];
  numRead = read(fd, headerBuf, Protocol::kMaxHeader);
  CHECK(numRead == 0) << "EOF not found when expected " << numRead << ":"
                      << folly::humanify(std::string(headerBuf, numRead));
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Got reply - all done for fd:" << fd
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
  *pHeaderBytes = headerBytes;
  *pDataBytes = dataBytes;
}
}
}  // namespace facebook::wdt
