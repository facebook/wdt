/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/SocketUtils.h>

#include <wdt/WdtOptions.h>
#include <wdt/Reporting.h>

#include <glog/logging.h>
#include <folly/Conv.h>
#include <sys/types.h>
#include <sys/ioctl.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <algorithm>
#ifdef WDT_HAS_SOCKIOS_H
#include <linux/sockios.h>
#endif

namespace facebook {
namespace wdt {

/* static */
int SocketUtils::getReceiveBufferSize(int fd) {
  int size;
  socklen_t sizeSize = sizeof(size);
  getsockopt(fd, SOL_SOCKET, SO_RCVBUF, (void *)&size, &sizeSize);
  return size;
}

/* static */
bool SocketUtils::getNameInfo(const struct sockaddr *sa, socklen_t salen,
                              std::string &host, std::string &port) {
  char hostBuf[NI_MAXHOST], portBuf[NI_MAXSERV];
  int res = getnameinfo(sa, salen, hostBuf, sizeof(hostBuf), portBuf,
                        sizeof(portBuf), NI_NUMERICHOST | NI_NUMERICSERV);
  if (res) {
    LOG(ERROR) << "getnameinfo failed " << gai_strerror(res);
    return false;
  }
  host = std::string(hostBuf);
  port = std::string(portBuf);
  return true;
}

/* static */
void SocketUtils::setReadTimeout(int fd) {
  const auto &options = WdtOptions::get();
  int timeout = getTimeout(options.read_timeout_millis);
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;            // milli to sec
    tv.tv_usec = (timeout % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}

/* static */
void SocketUtils::setWriteTimeout(int fd) {
  const auto &options = WdtOptions::get();
  int timeout = getTimeout(options.write_timeout_millis);
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;            // milli to sec
    tv.tv_usec = (timeout % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}

/* static */
int SocketUtils::getTimeout(int networkTimeout) {
  const auto &options = WdtOptions::get();
  int abortInterval = options.abort_check_interval_millis;
  if (abortInterval <= 0) {
    return networkTimeout;
  }
  if (networkTimeout <= 0) {
    return abortInterval;
  }
  return std::min(networkTimeout, abortInterval);
}

/* static */
int SocketUtils::getUnackedBytes(int fd) {
#ifdef WDT_HAS_SOCKIOS_H
  int numUnackedBytes;
  START_PERF_TIMER
  if (::ioctl(fd, SIOCOUTQ, &numUnackedBytes) != 0) {
    PLOG(ERROR) << "Failed to get unacked bytes for socket " << fd;
    numUnackedBytes = -1;
  }
  RECORD_PERF_RESULT(PerfStatReport::IOCTL)
  return numUnackedBytes;
#else
  LOG(WARNING) << "Wdt has no way to determine unacked bytes for socket";
  return -1;
#endif
}

/* static */
int64_t SocketUtils::readWithAbortCheck(int fd, char *buf, int64_t nbyte,
                                        IAbortChecker const *abortChecker,
                                        int timeoutMs, bool tryFull) {
  START_PERF_TIMER
  int64_t numRead =
      ioWithAbortCheck(read, fd, buf, nbyte, abortChecker, timeoutMs, tryFull);
  RECORD_PERF_RESULT(PerfStatReport::SOCKET_READ);
  return numRead;
}

/* static */
int64_t SocketUtils::writeWithAbortCheck(int fd, const char *buf, int64_t nbyte,
                                         IAbortChecker const *abortChecker,
                                         int timeoutMs, bool tryFull) {
  START_PERF_TIMER
  int64_t written =
      ioWithAbortCheck(write, fd, buf, nbyte, abortChecker, timeoutMs, tryFull);
  RECORD_PERF_RESULT(PerfStatReport::SOCKET_WRITE)
  return written;
}

template <typename F, typename T>
int64_t SocketUtils::ioWithAbortCheck(F readOrWrite, int fd, T tbuf,
                                      int64_t numBytes,
                                      IAbortChecker const *abortChecker,
                                      int timeoutMs, bool tryFull) {
  WDT_CHECK(abortChecker != nullptr) << "abort checker can not be null";
  const auto &options = WdtOptions::get();
  bool checkAbort = (options.abort_check_interval_millis > 0);
  auto startTime = Clock::now();
  int64_t doneBytes = 0;
  int retries = 0;
  while (doneBytes < numBytes) {
    const int64_t ret = readOrWrite(fd, tbuf + doneBytes, numBytes - doneBytes);
    if (ret < 0) {
      // error
      if (errno != EINTR && errno != EAGAIN) {
        PLOG(ERROR) << "non-retryable error encountered during socket io " << fd
                    << " " << doneBytes << " " << retries;
        return (doneBytes > 0 ? doneBytes : ret);
      }
    } else if (ret == 0) {
      // eof
      VLOG(1) << "EOF received during socket io. fd : " << fd
              << ", finished bytes : " << doneBytes
              << ", retries : " << retries;
      return doneBytes;
    } else {
      // success
      doneBytes += ret;
      if (!tryFull) {
        // do not have to read/write entire data
        return doneBytes;
      }
    }
    if (checkAbort && abortChecker->shouldAbort()) {
      LOG(ERROR) << "transfer aborted during socket io " << fd << " "
                 << doneBytes << " " << retries;
      return (doneBytes > 0 ? doneBytes : -1);
    }
    if (timeoutMs > 0) {
      int duration = durationMillis(Clock::now() - startTime);
      if (duration >= timeoutMs) {
        LOG(INFO) << "socket io timed out after " << duration << " ms, retries "
                  << retries << " fd " << fd << " doneBytes " << doneBytes;
        return (doneBytes > 0 ? doneBytes : -1);
      }
    }
    retries++;
  }
  VLOG_IF(1, retries > 1) << "socket io for " << doneBytes << " bytes took "
                          << retries << " retries";
  return doneBytes;
}
}
}
