#include "Receiver.h"

#include "Protocol.h"
#include "ServerSocket.h"
#include "SocketUtils.h"

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <chrono>
using std::vector;
namespace facebook {
namespace wdt {
/// len is initial/already read len
size_t readAtLeast(ServerSocket &s, char *buf, size_t max, ssize_t atLeast,
                   ssize_t len) {
  VLOG(4) << "readAtLeast len " << len << " max " << max << " atLeast "
          << atLeast << " from " << s.getFd();
  CHECK(len >= 0) << "negative len " << len;
  CHECK(atLeast >= 0) << "negative atLeast " << atLeast;
  int count = 0;
  while (len < atLeast) {
    ssize_t n = s.read(buf + len, max - len);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << s.getFd() << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      VLOG(2) << "Eof on " << s.getFd() << " after " << count << " read "
              << len;
      return len;
    }
    len += n;
    count++;
  }
  VLOG(3) << "took " << count << " read to get " << len << " from "
          << s.getFd();
  return len;
}

size_t readAtMost(ServerSocket &s, char *buf, size_t max, size_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  VLOG(3) << "readAtMost target " << target;
  ssize_t n = s.read(buf, target);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << s.getFd() << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(WARNING) << "Eof on " << s.getFd();
    return n;
  }
  VLOG(3) << "readAtMost " << n << " / " << atMost << " from " << s.getFd();
  return n;
}

Receiver::Receiver(int port, int numSockets)
    : port_(port), numSockets_(numSockets) {
  isJoinable_ = false;
  transferFinished_ = true;
}

Receiver::Receiver(int port, int numSockets, std::string destDir)
    : Receiver(port, numSockets) {
  this->destDir_ = destDir;
}

void Receiver::setDir(const std::string &destDir) {
  this->destDir_ = destDir;
}

Receiver::~Receiver() {
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an ongoing transfer and the destructor"
                 << " is being called. Trying to finish the transfer";
    finish();
  }
}

bool Receiver::hasPendingTransfer() {
  std::unique_lock<std::mutex> lock(transferInstanceMutex_);
  return !transferFinished_;
}

void Receiver::markTransferFinished(bool isFinished) {
  std::unique_lock<std::mutex> lock(transferInstanceMutex_);
  transferFinished_ = isFinished;
  if (isFinished) {
    conditionRecvFinished_.notify_all();
  }
}

std::unique_ptr<TransferReport> Receiver::finish() {
  const auto &options = WdtOptions::get();

  if (!isJoinable_) {
    LOG(WARNING) << "The receiver is not joinable. The threads will never"
                 << " finish and this method will never return";
  }
  for (int i = 0; i < numSockets_; i++) {
    receiverThreads_[i].join();
  }

  // A very important step to mark the transfer finished
  // No other transferAsync, or runForever can be called on this
  // instance unless the current transfer has finished
  markTransferFinished(true);

  // Make sure to join the progress thread.
  progressTrackerThread_.join();

  std::vector<TransferStats> transferredSourceStats;
  // All the following parameters for the report are useless for
  // receiver
  std::vector<TransferStats> failedSourceStats;
  std::vector<std::string> failedDirectories;
  double totalTime = -1;
  size_t totalFileSize = -1;
  if (options.fullReporting_) {
    // This will only be true if the receiver is joinable
    WDT_CHECK(isJoinable_);
    for (auto &stats : receivedFilesStats_) {
      transferredSourceStats.insert(transferredSourceStats.end(),
                                    std::make_move_iterator(stats.begin()),
                                    std::make_move_iterator(stats.end()));
    }
  }
  // TODO: failed source stats are intentionally kept empty but could
  // potentially be changed in case of disk write errors.
  std::unique_ptr<TransferReport> report = folly::make_unique<TransferReport>(
      transferredSourceStats, failedSourceStats, threadStats_,
      failedDirectories, totalTime, totalFileSize);
  LOG(WARNING) << "WDT receiver's transfer has been finished";
  LOG(INFO) << *report;
  receiverThreads_.clear();
  receivedFilesStats_.clear();
  threadServerSockets_.clear();
  threadStats_.clear();
  return report;
}

ErrorCode Receiver::transferAsync() {
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }
  const auto &options = WdtOptions::get();
  isJoinable_ = true;
  start();
  return OK;
}

ErrorCode Receiver::runForever() {
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }

  // Enforce the full reporting to be false in the daemon mode.
  // These statistics are expensive, and useless as they will never
  // be received/reviewed in a forever running process.
  auto &options = WdtOptions::getMutable();
  options.fullReporting_ = false;
  start();
  finish();
  // This method should never finish
  return ERROR;
}

void Receiver::progressTracker() {
  const auto &options = WdtOptions::get();
  // Progress tracker will check for progress after the time specified
  // in milliseconds.
  int progressTrackIntervalMillis = options.timeoutCheckIntervalMillis_;
  // The number of failed progress checks after which the threads
  // should be stopped
  int numFailedProgressChecks = options.failedTimeoutChecks_;
  if (progressTrackIntervalMillis < 0 || !isJoinable_) {
    return;
  }
  LOG(INFO) << "Progress tracker started. Will check every"
            << " " << progressTrackIntervalMillis << " ms"
            << " and fail after " << numFailedProgressChecks << " checks";
  auto waitingTime = std::chrono::milliseconds(progressTrackIntervalMillis);
  size_t totalBytes = 0;
  int64_t zeroProgressCount = 0;
  bool done = false;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(transferInstanceMutex_);
      conditionRecvFinished_.wait_for(lock, waitingTime);
      done = transferFinished_;
    }
    if (done) {
      break;
    }
    size_t currentTotalBytes = 0;
    for (int i = 0; i < threadStats_.size(); i++) {
      currentTotalBytes += threadStats_[i].getTotalBytes();
    }
    size_t deltaBytes = currentTotalBytes - totalBytes;
    totalBytes = currentTotalBytes;
    if (deltaBytes == 0) {
      zeroProgressCount++;
    } else {
      zeroProgressCount = 0;
    }
    VLOG(2) << "Progress Tracker : Number of bytes received since last call "
            << deltaBytes;
    if (zeroProgressCount > numFailedProgressChecks) {
      LOG(INFO) << "No progress for the last " << numFailedProgressChecks
                << " checks.";
      for (int i = 0; i < numSockets_; i++) {
        int listenFd = threadServerSockets_[i].getListenFd();
        if (shutdown(listenFd, SHUT_RDWR) < 0) {
          int port = port_ + i;
          LOG(WARNING) << "Progress tracker could not shut down listening "
                       << " file descriptor for the thread with port " << port;
        }
      }
      for (int i = 0; i < numSockets_; i++) {
        int fd = threadServerSockets_[i].getFd();
        if (shutdown(fd, SHUT_RDWR) < 0) {
          int port = port_ + i;
          LOG(WARNING) << "Progress tracker could not shut down file "
                       << "descriptor for the thread " << port;
        }
      }
      return;
    }
  }
}

void Receiver::start() {
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an existing transfer in progress on this object";
  }
  LOG(INFO) << "Starting (receiving) server on " << port_ << " : "
            << numSockets_ << " sockets, target dir " << destDir_;
  markTransferFinished(false);
  const auto &options = WdtOptions::get();
  size_t bufferSize = options.bufferSize_;
  if (bufferSize < Protocol::kMaxHeader) {
    // round up to even k
    bufferSize = 2 * 1024 * ((Protocol::kMaxHeader - 1) / (2 * 1024) + 1);
    LOG(INFO) << "Specified -buffer_size " << options.bufferSize_
              << " smaller than " << Protocol::kMaxHeader << " using "
              << bufferSize << " instead";
  }
  fileCreator_.reset(new FileCreator(destDir_));
  for (int i = 0; i < numSockets_; i++) {
    threadStats_.emplace_back(true);
    threadServerSockets_.emplace_back(folly::to<std::string>(port_ + i),
                                      options.backlog_);
  }
  receivedFilesStats_.resize(numSockets_);
  for (int i = 0; i < numSockets_; i++) {
    receiverThreads_.emplace_back(
        &Receiver::receiveOne, this, std::ref(threadServerSockets_[i]),
        std::ref(destDir_), bufferSize, std::ref(threadStats_[i]),
        std::ref(receivedFilesStats_[i]));
  }
  if (isJoinable_) {
    std::thread trackerThread(&Receiver::progressTracker, this);
    progressTrackerThread_ = std::move(trackerThread);
  }
}

void Receiver::receiveOne(ServerSocket &socket, const std::string &destDir,
                          size_t bufferSize, TransferStats &threadStats,
                          std::vector<TransferStats> &receivedFilesStats) {
  const auto &options = WdtOptions::get();
  const bool doActualWrites = !options.skipWrites_;
  std::string port = socket.getPort();
  VLOG(1) << "Server Thread for port " << port << " with backlog "
          << socket.getBackLog() << " on " << destDir
          << " writes= " << doActualWrites;
  for (int i = 1; i < options.maxRetries_; ++i) {
    ErrorCode code = socket.listen();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      threadStats.setErrorCode(code);
      return;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(options.sleepMillis_ * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (socket.listen() != OK) {
    LOG(ERROR) << "Unable to listen/bind despite retries";
    threadStats.setErrorCode(CONN_ERROR);
    return;
  }
  char *buf = (char *)malloc(bufferSize);
  if (!buf) {
    LOG(ERROR) << "error allocating " << bufferSize;
    threadStats.setErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }
  threadStats.setErrorCode(OK);
  while (true) {
    ErrorCode code = socket.acceptNextConnection();
    if (code != OK) {
      threadStats.setErrorCode(code);
      free(buf);
      return;
    }
    // TODO test with sending bytes 1 by 1 and id len at max
    ssize_t numRead = 0;
    size_t off = 0;
    int dest = -1;
    LOG(INFO) << "New socket on " << socket.getFd() << " socket buffer is "
              << SocketUtils::getReceiveBufferSize(socket.getFd());
    while (true) {
      numRead = readAtLeast(socket, buf + off, bufferSize - off,
                            Protocol::kMaxHeader, numRead);
      if (numRead <= 0) {
        break;
      }
      std::string id;
      int64_t size;
      const ssize_t oldOffset = off;
      Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[off++];
      if (cmd == Protocol::EXIT_CMD) {
        if (numRead != 1) {
          LOG(ERROR) << "Unexpected state for exit command. probably junk "
                        "content. ignoring...";
          threadStats.setErrorCode(PROTOCOL_ERROR);
          break;
        }
        LOG(ERROR) << "Got exit command in port " << port << " - exiting";
        exit(0);
      }
      ErrorCode transferStatus = (ErrorCode)buf[off++];
      if (cmd == Protocol::DONE_CMD) {
        VLOG(1) << "Got done command for " << socket.getFd();
        if (numRead != 2) {
          LOG(ERROR) << "Unexpected state for done command"
                     << " off: " << off << " numRead: " << numRead;
          threadStats.setErrorCode(PROTOCOL_ERROR);
          break;
        }
        buf[off - 1] = threadStats.getErrorCode();
        if (transferStatus != OK) {
          LOG(ERROR) << "Errors transmitted by the sender side.\n"
                     << "Final transfer status " << kErrorToStr[transferStatus]
                     << "\nCurrent receiver status "
                     << kErrorToStr[threadStats.getErrorCode()];
          threadStats.setErrorCode(transferStatus);
        }
        socket.write(buf + off - 2, 2);
        threadStats.addHeaderBytes(2);
        threadStats.addEffectiveBytes(2, 0);
        if (isJoinable_) {
          LOG(INFO) << "Receiver thread done. " << threadStats;
          free(buf);
          return;
        }
        break;
      }
      if (cmd != Protocol::FILE_CMD) {
        LOG(ERROR) << "Unexpected magic/cmd byte " << cmd
                   << ". numRead = " << numRead << ". port = " << port
                   << ". offset = " << oldOffset;
        threadStats.setErrorCode(PROTOCOL_ERROR);
        break;
      }
      if (transferStatus != OK) {
        // TODO: use this status information to implement fail fast mode
        VLOG(1) << "sender entered into error state "
                << kErrorToStr[transferStatus];
      }
      bool success = Protocol::decode(buf, off, numRead + oldOffset, id, size);
      ssize_t headerBytes = off - oldOffset;
      threadStats.addHeaderBytes(headerBytes);
      if (!success) {
        LOG(ERROR) << "Error decoding at"
                   << " ooff:" << oldOffset << " off: " << off
                   << " numRead: " << numRead;
        threadStats.setErrorCode(PROTOCOL_ERROR);
        threadStats.incrFailedAttempts();
        break;
      }
      VLOG(1) << "Read id:" << id << " size:" << size << " ooff:" << oldOffset
              << " off: " << off << " numRead: " << numRead;

      if (doActualWrites) {
        dest = fileCreator_->createFile(id);
        if (dest == -1) {
          LOG(ERROR) << "Unable to open " << id << " in " << destDir;
          threadStats.setErrorCode(FILE_WRITE_ERROR);
        }
      }
      ssize_t remainingData = numRead + oldOffset - off;
      ssize_t toWrite = remainingData;
      if (remainingData >= size) {
        toWrite = size;
      }
      threadStats.addDataBytes(toWrite);
      // write rest of stuff
      int64_t wres = toWrite;
      int64_t written;
      if (dest >= 0) {
        written = write(dest, buf + off, toWrite);
        if (written != toWrite) {
          PLOG(ERROR) << "Write error/mismatch " << written << " " << off << " "
                      << toWrite;
          threadStats.setErrorCode(FILE_WRITE_ERROR);
          close(dest);
          dest = -1;
        } else {
          VLOG(3) << "Wrote intial " << toWrite << " / " << size
                  << " off: " << off << " numRead: " << numRead << " on "
                  << dest;
        }
      }
      off += wres;
      remainingData -= wres;
      // also means no leftOver so it's ok we use buf from start
      while (wres < size) {
        int64_t nres = readAtMost(socket, buf, bufferSize, size - wres);
        if (nres <= 0) {
          break;
        }
        threadStats.addDataBytes(nres);
        if (dest >= 0) {
          written = write(dest, buf, nres);
          if (written != nres) {
            PLOG(ERROR) << "Write error/mismatch " << written << " " << nres;
            threadStats.setErrorCode(FILE_WRITE_ERROR);
            close(dest);
            dest = -1;
          }
        }
        wres += nres;
      }
      if (wres != size) {
        threadStats.incrFailedAttempts();
        break;
      }
      VLOG(1) << "completed " << id << " off: " << off
              << " numRead: " << numRead << " on " << dest;
      if (dest >= 0) {
        close(dest);
        dest = -1;
      }
      // Transfer of the file is complete here, mark the bytes effective
      threadStats.addEffectiveBytes(headerBytes, size);
      threadStats.incrNumFiles();
      if (options.fullReporting_) {
        WDT_CHECK(isJoinable_);
        TransferStats fileStats;
        fileStats.setErrorCode(OK);
        fileStats.setId(id);
        fileStats.addHeaderBytes(headerBytes);
        fileStats.addDataBytes(size);
        fileStats.addEffectiveBytes(headerBytes, size);
        receivedFilesStats.emplace_back(std::move(fileStats));
      }
      WDT_CHECK(remainingData >= 0) << "Negative remainingData "
                                    << remainingData;
      if (remainingData > 0) {
        // if we need to read more anyway, let's move the data
        numRead = remainingData;
        if ((remainingData < Protocol::kMaxHeader) &&
            (off > (bufferSize / 2))) {
          // rare so inneficient is ok
          VLOG(3) << "copying extra " << remainingData << " leftover bytes @ "
                  << off;
          memmove(/* dst      */ buf,
                  /* from     */ buf + off,
                  /* how much */ remainingData);
          off = 0;
        } else {
          // otherwise just change the offset
          VLOG(3) << "will use remaining extra " << remainingData
                  << " leftover bytes @ " << off;
        }
      } else {
        numRead = off = 0;
      }
    }
    if (dest >= 0) {
      VLOG(2) << "closing file writer fd " << dest;
      close(dest);
    }
    VLOG(1) << "Done with " << socket.getFd();
    socket.closeCurrentConnection();
  }
  free(buf);
  threadStats.setErrorCode(OK);
}
}
}  // namespace facebook::wdt
