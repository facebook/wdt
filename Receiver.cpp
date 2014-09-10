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

DECLARE_int32(buffer_size);
DECLARE_int32(max_retries);
DECLARE_int32(sleep_ms);
DECLARE_int32(backlog);

namespace {

/// len is initial/already read len
size_t readAtLeast(int fd, char *buf, size_t max, ssize_t atLeast,
                   ssize_t len = 0) {
  VLOG(1) << "readAtLeast len " << len << " max " << max << " atLeast "
          << atLeast << " from " << fd;
  CHECK(len >= 0) << "negative len " << len;
  CHECK(atLeast >= 0) << "negative atLeast " << atLeast;
  int count = 0;
  while (len < atLeast) {
    ssize_t n = read(fd, buf + len, max - len);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << fd << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      VLOG(1) << "Eof on " << fd << " after " << count << " read " << len;
      return len;
    }
    len += n;
    count++;
  }
  VLOG(1) << "took " << count << " read to get " << len << " from " << fd;
  return len;
}

size_t readAtMost(int fd, char *buf, size_t max, size_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  VLOG(1) << "readAtMost target " << target;
  ssize_t n = read(fd, buf, target);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << fd << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(WARNING) << "Eof on " << fd;
    return n;
  }
  VLOG(1) << "readAtMost " << n << " / " << atMost << " from " << fd;
  return n;
}

}  // anonymous namespace

namespace facebook {
namespace wdt {

Receiver::Receiver(int port, int numSockets, std::string destDir)
    : port_(port), numSockets_(numSockets), destDir_(destDir) {
}

void Receiver::start() {
  LOG(INFO) << "Starting (receiving) server on " << port_ << " : "
            << numSockets_ << " sockets, target dir " << destDir_;
  size_t bufferSize = FLAGS_buffer_size;
  if (bufferSize < Protocol::kMaxHeader) {
    // round up to even k
    bufferSize = 2 * 1024 * ((Protocol::kMaxHeader - 1) / (2 * 1024) + 1);
    LOG(INFO) << "Specified -buffer_size " << FLAGS_buffer_size
              << " smaller than " << Protocol::kMaxHeader << " using "
              << bufferSize << " instead";
  }
  fileCreator_.reset(new FileCreator(destDir_));
  std::vector<std::thread> vt;
  for (int i = 0; i < numSockets_; i++) {
    vt.emplace_back(&Receiver::receiveOne, this, port_ + i, FLAGS_backlog,
                    destDir_, bufferSize);
  }
  // will never exit
  for (int i = 0; i < numSockets_; i++) {
    vt[i].join();
  }
}

void Receiver::receiveOne(int port, int backlog, const std::string &destDir,
                          size_t bufferSize) {
  LOG(INFO) << "Server Thread for port " << port << " with backlog " << backlog
            << " on " << destDir;

  ServerSocket s(folly::to<std::string>(port), backlog);
  for (int i = 1; i < FLAGS_max_retries; ++i) {
    if (s.listen()) {
      break;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(FLAGS_sleep_ms * 1000);
  }
  // one more/last try (stays true if it worked above)
  CHECK(s.listen()) << "Unable to listen/bind despite retries";
  char *buf = (char *)malloc(bufferSize);
  CHECK(buf) << "error allocating " << bufferSize;
  while (true) {
    int fd = s.getNextFd();
    // TODO test with sending bytes 1 by 1 and id len at max
    ssize_t numRead = 0;
    size_t off = 0;
    LOG(INFO) << "New socket on " << fd << " socket buffer is "
              << SocketUtils::getReceiveBufferSize(fd);
    while (true) {
      numRead = readAtLeast(fd, buf + off, bufferSize - off,
                            Protocol::kMaxHeader, numRead);
      if (numRead <= 0) {
        break;
      }
      std::string id;
      int64_t size;
      const ssize_t oldOffset = off;
      Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[off++];
      if (cmd == Protocol::DONE_CMD) {
        LOG(INFO) << "Got done command for " << fd;
        CHECK(numRead == 1) << "Unexpected state for done command"
                            << " off: " << off << " numRead: " << numRead;
        write(fd, buf + off - 1, 1);
        break;
      }
      CHECK(cmd == Protocol::FILE_CMD) << "Unexpected magic/cmd byte " << cmd;
      bool success = Protocol::decode(buf, off, numRead + oldOffset, id, size);
      CHECK(success) << "Error decoding at"
                     << " ooff:" << oldOffset << " off: " << off
                     << " numRead: " << numRead;
      LOG(INFO) << "Read id:" << id << " size:" << size << " ooff:" << oldOffset
                << " off: " << off << " numRead: " << numRead;
      int dest = fileCreator_->createFile(id);
      if (dest == -1) {
        LOG(ERROR) << "Unable to open " << id << " in " << destDir;
      }
      ssize_t remainingData = numRead + oldOffset - off;
      ssize_t toWrite = remainingData;
      if (remainingData >= size) {
        toWrite = size;
      }
      // write rest of stuff
      int64_t wres = write(dest, buf + off, toWrite);
      if (wres != toWrite) {
        PLOG(ERROR) << "Write error/mismatch " << wres << " " << off << " "
                    << toWrite;
      } else {
        VLOG(1) << "Wrote intial " << wres << " / " << size << " off: " << off
                << " numRead: " << numRead << " on " << dest;
      }
      off += wres;
      remainingData -= wres;
      // also means no leftOver so it's ok we use buf from start
      while (wres < size) {
        int64_t nres = readAtMost(fd, buf, bufferSize, size - wres);
        if (nres <= 0) {
          break;
        }
        int64_t nwres = write(dest, buf, nres);
        if (nwres != nres) {
          PLOG(ERROR) << "Write error/mismatch " << nwres << " " << nres;
        }
        wres += nwres;
      }
      LOG(INFO) << "completed " << id << " off: " << off
                << " numRead: " << numRead << " on " << dest;
      close(dest);
      CHECK(remainingData >= 0) "Negative remainingData " << remainingData;
      if (remainingData > 0) {
        // if we need to read more anyway, let's move the data
        numRead = remainingData;
        if ((remainingData < Protocol::kMaxHeader) &&
            (off > (bufferSize / 2))) {
          // rare so inneficient is ok
          VLOG(1) << "copying extra " << remainingData << " leftover bytes @ "
                  << off;
          memmove(/* dst      */ buf,
                  /* from     */ buf + off,
                  /* how much */ remainingData);
          off = 0;
        } else {
          // otherwise just change the offset
          VLOG(1) << "will use remaining extra " << remainingData
                  << " leftover bytes @ " << off;
        }
      } else {
        numRead = off = 0;
      }
    }
    LOG(INFO) << "Done with " << fd;
    close(fd);
  }
  free(buf);
}
}
}  // namespace facebook::wdt
