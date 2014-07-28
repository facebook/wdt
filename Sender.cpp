#include "Sender.h"

#include "ClientSocket.h"
#include "DirectorySourceQueue.h"
#include "Protocol.h"

#include <folly/Conv.h>
#include <folly/String.h>

#include <thread>

DECLARE_int32(buffer_size);
DECLARE_int32(max_retries);
DECLARE_int32(sleep_ms);

namespace {

template <typename T>
double durationSeconds(T d) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

} // anonymous namespace

namespace facebook {
namespace wdt {

Sender::Sender(const std::string& destHost,
               int port,
               int numSockets,
               const std::string& srcDir)
  : destHost_(destHost), port_(port), numSockets_(numSockets), srcDir_(srcDir) {
}

void Sender::start() {
  auto startTime = Clock::now();
  size_t bufferSize = FLAGS_buffer_size;
  LOG(INFO) << "Client (sending) to " << destHost_ << " port " << port_ << " : "
            << numSockets_ << " sockets, source dir " << srcDir_;
  DirectorySourceQueue queue(srcDir_, bufferSize);
  std::vector<std::thread> vt;
  size_t headerBytes[numSockets_];
  size_t dataBytes[numSockets_];
  for (int i = 0; i < numSockets_; i++) {
    dataBytes[i] = 0;
    headerBytes[i] = 0;
    vt.emplace_back(&Sender::sendOne,
                    this,
                    startTime,
                    destHost_,
                    port_ + i,
                    &queue,
                    &headerBytes[i],
                    &dataBytes[i]);
  }
  queue.init();
  size_t totalDataBytes = 0;
  size_t totalHeaderBytes = 0;
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
    totalHeaderBytes += headerBytes[i];
    totalDataBytes += dataBytes[i];
  }
  size_t totalBytes = totalDataBytes + totalHeaderBytes;
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(WARNING) << "All data transfered in " << totalTime
               << " seconds. Data Mbytes = " << totalDataBytes / 1024. / 1024.
               << ". Header kbytes = " << totalHeaderBytes / 1024. << " ( "
               << 100. * totalHeaderBytes / totalBytes << " % overhead)"
               << ". Total bytes = " << totalBytes << ". Total throughput = "
               << totalBytes / totalTime / 1024. / 1024. << " Mbytes/sec";
}

void Sender::sendOne(Clock::time_point startTime,
                     const std::string& destHost,
                     int port,
                     DirectorySourceQueue* queue,
                     size_t* pHeaderBytes,
                     size_t* pDataBytes) {
  size_t headerBytes = 0, dataBytes = 0;
  size_t numFiles = 0;
  ClientSocket s(destHost, folly::to<std::string>(port));
  for (int i = 1; i < FLAGS_max_retries; ++i) {
    if (s.connect()) {
      break;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(FLAGS_sleep_ms * 1000);
  }
  // one more/last try (stays true if it worked above)
  CHECK(s.connect()) << "Unable to connect despite retries";
  int fd = s.getFd();
  char headerBuf[Protocol::kMaxHeader];
  std::unique_ptr<ByteSource> source;

  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Connect took " << elapsedSecsConn;

  while ((source = queue->getNextSource())) {
    ++numFiles;
    size_t off = 0;
    headerBuf[off++] = Protocol::FILE_CMD;
    const size_t expectedSize = source->getSize();
    size_t actualSize = 0;
    bool success = Protocol::encode(headerBuf,
                                    off,
                                    Protocol::kMaxHeader,
                                    source->getIdentifier(),
                                    expectedSize);
    CHECK(success);
    ssize_t written = write(fd, headerBuf, off);
    headerBytes += written;
    if (written != off) {
      PLOG(FATAL) << "Write error/mismatch " << written << " " << off;
    }
    VLOG(1) << "Sent " << written << " on " << fd << " : "
            << folly::humanify(std::string(headerBuf, off));
    while (!source->finished()) {
      size_t size;
      char* buffer = source->read(size);
      if (source->hasError()) {
        LOG(ERROR) << "failed reading file";
        break;
      }
      CHECK(buffer && size > 0);
      written = write(fd, buffer, size);
      actualSize += written;
      VLOG(1) << "wrote " << written << " on " << fd;
      if (written != size) {
        PLOG(FATAL) << "Write error/mismatch " << written << " " << size;
      }
    }
    dataBytes += actualSize;
    if (actualSize != expectedSize) {
      LOG(FATAL) << "UGH " << source->getIdentifier() << " " << expectedSize
                 << " " << actualSize;
    }
  }
  headerBuf[0] = Protocol::DONE_CMD;
  write(fd, headerBuf, 1); //< TODO check for status/succes
  ++headerBytes;
  shutdown(fd, SHUT_WR); //< TODO check for status/succes
  LOG(INFO) << "Wrote done cmd on " << fd << " waiting for reply...";
  ssize_t numRead = read(fd, headerBuf, 1);
  CHECK(numRead == 1) << "READ unexpected " << numRead << ":"
                      << folly::humanify(std::string(headerBuf, numRead));
  CHECK(headerBuf[0] == Protocol::DONE_CMD) << "Unexpected reply "
                                            << headerBuf[0];
  numRead = read(fd, headerBuf, Protocol::kMaxHeader);
  CHECK(numRead == 0) << "EOF not found when expected " << numRead << ":"
                      << folly::humanify(std::string(headerBuf, numRead));
  double totalTime = durationSeconds(Clock::now() - startTime);
  size_t totalBytes = headerBytes + dataBytes;
  LOG(WARNING)
    << "Got reply - all done for fd:" << fd
    << ". Number of files = " << numFiles << ". Total time = " << totalTime
    << " (" << elapsedSecsConn << " in connection)"
    << ". Avg file size = " << 1. * dataBytes / numFiles
    << ". Data bytes = " << dataBytes << ". Header bytes = " << headerBytes
    << " ( " << 100. * headerBytes / totalBytes << " % overhead)"
    << ". Total bytes = " << totalBytes
    << ". Total throughput = " << totalBytes / totalTime / 1024. / 1024.
    << " Mbytes/sec";
  *pHeaderBytes = headerBytes;
  *pDataBytes = dataBytes;
}
}
} // namespace facebook::wdt
