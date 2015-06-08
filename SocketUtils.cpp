#include "SocketUtils.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "ErrorCodes.h"

#include <glog/logging.h>
#include <folly/Conv.h>
#include <sys/types.h>
#include <netdb.h>

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
std::string SocketUtils::getNameInfo(const struct sockaddr *sa,
                                     socklen_t salen) {
  char host[NI_MAXHOST], service[NI_MAXSERV];
  int res = getnameinfo(sa, salen, host, sizeof(host), service, sizeof(service),
                        NI_NUMERICHOST | NI_NUMERICSERV);
  if (res) {
    LOG(ERROR) << "getnameinfo failed " << gai_strerror(res);
  }
  return folly::to<std::string>(host, " ", service);
}

/* static */
void SocketUtils::setReadTimeout(int fd) {
  const auto &options = WdtOptions::get();
  int timeout = options.read_timeout_millis;
  if (options.abort_check_interval_millis > 0) {
    timeout = options.abort_check_interval_millis;
  }
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;            // milli to sec
    tv.tv_usec = (timeout % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}

/* statis */
void SocketUtils::setWriteTimeout(int fd) {
  const auto &options = WdtOptions::get();
  int timeout = options.write_timeout_millis;
  if (options.abort_check_interval_millis > 0) {
    timeout = options.abort_check_interval_millis;
  }
  if (timeout > 0) {
    struct timeval tv;
    tv.tv_sec = timeout / 1000;            // milli to sec
    tv.tv_usec = (timeout % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}

ssize_t SocketUtils::readWithAbortCheck(
    int fd, char *buf, size_t nbyte, WdtBase::IAbortChecker const *abortChecker,
    bool tryFull) {
  const auto &options = WdtOptions::get();
  START_PERF_TIMER
  ssize_t numRead = ioWithAbortCheck(read, fd, buf, nbyte, abortChecker,
                                     options.read_timeout_millis, tryFull);
  RECORD_PERF_RESULT(PerfStatReport::SOCKET_READ);
  return numRead;
}

ssize_t SocketUtils::writeWithAbortCheck(
    int fd, const char *buf, size_t nbyte,
    WdtBase::IAbortChecker const *abortChecker, bool tryFull) {
  const auto &options = WdtOptions::get();
  START_PERF_TIMER
  ssize_t written = ioWithAbortCheck(write, fd, buf, nbyte, abortChecker,
                                     options.write_timeout_millis, tryFull);
  RECORD_PERF_RESULT(PerfStatReport::SOCKET_WRITE)
  return written;
}

template <typename F, typename T>
ssize_t SocketUtils::ioWithAbortCheck(
    F readOrWrite, int fd, T tbuf, size_t numBytes,
    WdtBase::IAbortChecker const *abortChecker, int timeoutMs, bool tryFull) {
  WDT_CHECK(abortChecker != nullptr) << "abort checker can not be null";
  const auto &options = WdtOptions::get();
  bool checkAbort = (options.abort_check_interval_millis > 0);
  auto startTime = Clock::now();
  ssize_t doneBytes = 0;
  int retries = 0;
  while (doneBytes < numBytes) {
    ssize_t ret = readOrWrite(fd, tbuf + doneBytes, numBytes - doneBytes);
    if (ret < 0) {
      // error
      if (errno != EINTR && errno != EAGAIN) {
        PLOG(ERROR) << "non-retryable error encountered during socket io " << fd
                    << " " << doneBytes << " " << retries;
        return (doneBytes > 0 ? doneBytes : ret);
      }
    } else if (ret == 0) {
      // eof
      VLOG(1) << "EOF received during socket io " << fd << " " << doneBytes
              << " " << retries;
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
