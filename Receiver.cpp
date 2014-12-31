#include "Receiver.h"

#include "Protocol.h"
#include "ServerSocket.h"
#include "SocketUtils.h"

#include <folly/Conv.h>
#include <folly/String.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>

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

Receiver::Receiver(int port, int numSockets, std::string destDir)
    : port_(port), numSockets_(numSockets), destDir_(destDir) {
}

std::vector<ErrorCode> Receiver::start() {
  LOG(INFO) << "Starting (receiving) server on " << port_ << " : "
            << numSockets_ << " sockets, target dir " << destDir_;
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
  std::vector<std::thread> vt;
  ErrorCode errCodes[numSockets_];
  for (int i = 0; i < numSockets_; i++) {
    vt.emplace_back(&Receiver::receiveOne, this, port_ + i, options.backlog_,
                    std::ref(destDir_), bufferSize, std::ref(errCodes[i]));
  }
  // will never exit
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
  }
  return std::vector<ErrorCode>(errCodes, errCodes + numSockets_);
}

void Receiver::receiveOne(int port, int backlog, const std::string &destDir,
                          size_t bufferSize, ErrorCode &errCode) {
  const auto &options = WdtOptions::get();
  const bool doActualWrites = !options.skipWrites_;

  VLOG(1) << "Server Thread for port " << port << " with backlog " << backlog
          << " on " << destDir << " writes= " << doActualWrites;

  ServerSocket s(folly::to<std::string>(port), backlog);
  for (int i = 1; i < options.maxRetries_; ++i) {
    ErrorCode code = s.listen();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      errCode = code;
      return;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(options.sleepMillis_ * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (s.listen() != OK) {
    LOG(ERROR) << "Unable to listen/bind despite retries";
    errCode = CONN_ERROR;
    return;
  }
  char *buf = (char *)malloc(bufferSize);
  if (!buf) {
    LOG(ERROR) << "error allocating " << bufferSize;
    errCode = MEMORY_ALLOCATION_ERROR;
    return;
  }
  while (true) {
    ErrorCode code = s.acceptNextConnection();
    if (code != OK) {
      errCode = code;
      return;
    }
    // TODO test with sending bytes 1 by 1 and id len at max
    ssize_t numRead = 0;
    size_t off = 0;
    LOG(INFO) << "New socket on " << s.getFd() << " socket buffer is "
              << SocketUtils::getReceiveBufferSize(s.getFd());
    while (true) {
      numRead = readAtLeast(s, buf + off, bufferSize - off,
                            Protocol::kMaxHeader, numRead);
      if (numRead <= 0) {
        break;
      }
      std::string id;
      int64_t size;
      const ssize_t oldOffset = off;
      Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[off++];
      if (cmd == Protocol::EXIT_CMD) {
        LOG(CRITICAL) << "Got exit command - exiting";
        exit(0);
      }
      if (cmd == Protocol::DONE_CMD) {
        VLOG(1) << "Got done command for " << s.getFd();
        if (numRead != 1) {
          LOG(ERROR) << "Unexpected state for done command"
                     << " off: " << off << " numRead: " << numRead;
          errCode = PROTOCOL_ERROR;
          return;
        }
        s.write(buf + off - 1, 1);
        break;
      }
      if (cmd != Protocol::FILE_CMD) {
        LOG(ERROR) << "Unexpected magic/cmd byte " << cmd;
        errCode = PROTOCOL_ERROR;
        return;
      }
      bool success = Protocol::decode(buf, off, numRead + oldOffset, id, size);
      WDT_CHECK(success) << "Error decoding at"
                         << " ooff:" << oldOffset << " off: " << off
                         << " numRead: " << numRead;
      VLOG(1) << "Read id:" << id << " size:" << size << " ooff:" << oldOffset
              << " off: " << off << " numRead: " << numRead;

      int dest = -1;
      if (doActualWrites) {
        dest = fileCreator_->createFile(id);
        if (dest == -1) {
          LOG(ERROR) << "Unable to open " << id << " in " << destDir;
        }
      }
      ssize_t remainingData = numRead + oldOffset - off;
      ssize_t toWrite = remainingData;
      if (remainingData >= size) {
        toWrite = size;
      }
      // write rest of stuff
      int64_t wres = toWrite;
      if (doActualWrites) {
        wres = write(dest, buf + off, toWrite);
      }
      if (wres != toWrite) {
        PLOG(ERROR) << "Write error/mismatch " << wres << " " << off << " "
                    << toWrite;
        break;
      } else {
        VLOG(3) << "Wrote intial " << wres << " / " << size << " off: " << off
                << " numRead: " << numRead << " on " << dest;
      }
      off += wres;
      remainingData -= wres;
      // also means no leftOver so it's ok we use buf from start
      while (wres < size) {
        int64_t nres = readAtMost(s, buf, bufferSize, size - wres);
        if (nres <= 0) {
          break;
        }
        int64_t nwres = nres;
        if (doActualWrites) {
          nwres = write(dest, buf, nres);
        }
        if (nwres != nres) {
          PLOG(ERROR) << "Write error/mismatch " << nwres << " " << nres;
          break;
        }
        wres += nwres;
      }
      VLOG(1) << "completed " << id << " off: " << off
              << " numRead: " << numRead << " on " << dest;
      if (doActualWrites) {
        close(dest);
      }
      WDT_CHECK(remainingData >= 0) "Negative remainingData " << remainingData;
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
    VLOG(1) << "Done with " << s.getFd();
    s.closeCurrentConnection();
  }
  free(buf);
  errCode = OK;
}
}
}  // namespace facebook::wdt
