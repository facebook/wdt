/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/ClientSocket.h>
#include <wdt/Reporting.h>

#include <fcntl.h>
#include <glog/logging.h>
#include <poll.h>
#include <sys/socket.h>

#include <folly/Conv.h>
#include <folly/ScopeGuard.h>

namespace facebook {
namespace wdt {

using std::string;

ClientSocket::ClientSocket(ThreadCtx &threadCtx, const string &dest,
                           const int port,
                           const EncryptionParams &encryptionParams,
                           int64_t ivChangeInterval)
    : dest_(dest), threadCtx_(threadCtx) {
  memset(&sa_, 0, sizeof(sa_));
  socket_ = std::make_unique<WdtSocket>(threadCtx, port, encryptionParams,
                                        ivChangeInterval, nullptr);
  if (threadCtx_.getOptions().ipv6) {
    sa_.ai_family = AF_INET6;
  }
  if (threadCtx_.getOptions().ipv4) {
    sa_.ai_family = AF_INET;
  }
  sa_.ai_socktype = SOCK_STREAM;
}

ErrorCode ClientSocket::connect() {
  auto fd = socket_->getFd();
  auto port = socket_->getPort();
  WDT_CHECK(fd < 0) << "Previous connection not closed " << fd << " "
                     << port;
  // Lookup
  struct addrinfo *infoList = nullptr;
  auto guard = folly::makeGuard([&] {
    if (infoList) {
      freeaddrinfo(infoList);
    }
  });
  string portStr = folly::to<string>(port);
  int res = getaddrinfo(dest_.c_str(), portStr.c_str(), &sa_, &infoList);
  if (res) {
    // not errno, can't use WPLOG (perror)
    WLOG(ERROR) << "Failed getaddrinfo " << dest_ << " , " << port << " : "
                << res << " : " << gai_strerror(res);
    return CONN_ERROR;
  }
  int count = 0;
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    ++count;
    std::string host, port;
    WdtSocket::getNameInfo(info->ai_addr, info->ai_addrlen, host, port);
    WVLOG(2) << "will connect to " << host << " " << port;
    fd = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (fd == -1) {
      WPLOG(WARNING) << "Error making socket for port " << port;
      continue;
    }
    WVLOG(1) << "new socket " << fd << " for port " << port;
    socket_->setFd(fd);

    setSendBufferSize();

    // make the socket non blocking
    int sockArg = fcntl(fd, F_GETFL, nullptr);
    sockArg |= O_NONBLOCK;
    res = fcntl(fd, F_SETFL, sockArg);
    if (res < 0) {
      WPLOG(ERROR) << "Failed to make the socket non-blocking " << port
                   << " sock " << sockArg << " res " << res;
      closeConnection();
      continue;
    }

    if (::connect(fd, info->ai_addr, info->ai_addrlen) != 0) {
      if (errno != EINPROGRESS) {
        WPLOG(INFO) << "Error connecting on " << host << " " << port;
        closeConnection();
        continue;
      }
      auto startTime = Clock::now();
      int connectTimeout = threadCtx_.getOptions().connect_timeout_millis;

      while (true) {
        // check for abort
        if (threadCtx_.getAbortChecker()->shouldAbort()) {
          WLOG(ERROR) << "Transfer aborted during connect " << port << " "
                      << fd;
          closeConnection();
          return ABORT;
        }
        // we need this loop because poll() can return before any file handles
        // have changes or before timing out. In that case, we check whether it
        // is because of EINTR or not. If true, we have to try poll with
        // reduced timeout. Also we set the poll timeout to be at max equal to
        // abort check interval. This allows us to check for abort regularly.
        int timeElapsed = durationMillis(Clock::now() - startTime);
        if (timeElapsed >= connectTimeout) {
          WVLOG(1) << "connect() timed out" << host << " " << port;
          closeConnection();
          return CONN_ERROR_RETRYABLE;
        }
        int pollTimeout =
            std::min(connectTimeout - timeElapsed,
                     threadCtx_.getOptions().abort_check_interval_millis);
        struct pollfd pollFds[] = {{fd, POLLOUT, 0}};

        if ((res = poll(pollFds, 1, pollTimeout)) <= 0) {
          if (errno == EINTR) {
            WVLOG(1) << "poll() call interrupted. retrying... " << port;
            continue;
          }
          if (res == 0) {
            WVLOG(1) << "poll() timed out " << host << " " << port;
            continue;
          }
          WPLOG(ERROR) << "poll() failed " << host << " " << port << " " << fd;
          closeConnection();
          return CONN_ERROR;
        }
        break;
      }

      // have to check whether the connection attempt succeeded
      int connectResult;
      socklen_t len = sizeof(connectResult);
      if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &connectResult, &len) < 0) {
        WPLOG(WARNING) << "getsockopt() failed";
        closeConnection();
        continue;
      }
      if (connectResult != 0) {
        WLOG(WARNING) << "connect did not succeed on " << host << " " << port
                      << " : " << strerrorStr(connectResult);
        closeConnection();
        continue;
      }
    }

    // Set to blocking mode again
    sockArg = fcntl(fd, F_GETFL, nullptr);
    sockArg &= (~O_NONBLOCK);
    res = fcntl(fd, F_SETFL, sockArg);
    if (res == -1) {
      WPLOG(ERROR) << "Could not make the socket blocking " << port;
      closeConnection();
      continue;
    }
    WVLOG(1) << "Successful connect on " << fd;
    peerIp_ = host;
    sa_ = *info;
    break;
  }
  if (socket_->getFd() < 0) {
    if (count > 1) {
      // Only log this if not redundant with log above (ie --ipv6=false)
      WLOG(INFO) << "Unable to connect to either of the " << count << " addrs";
    }
    return CONN_ERROR_RETRYABLE;
  }
  socket_->setSocketTimeouts();
  socket_->setDscp(threadCtx_.getOptions().dscp);
  return OK;
}

const std::string &ClientSocket::getPeerIp() const {
  return peerIp_;
}

void ClientSocket::setSendBufferSize() {
  int bufSize = threadCtx_.getOptions().send_buffer_size;
  auto fd = socket_->getFd();
  auto port = socket_->getPort();
  if (bufSize <= 0) {
    return;
  }
  int status =
      ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, &bufSize, sizeof(bufSize));
  if (status != 0) {
    WPLOG(ERROR) << "Failed to set send buffer " << port << " size " << bufSize
                 << " fd " << fd;
    return;
  }
  WVLOG(1) << "Send buffer size set to " << bufSize << " port " << port;
}

ClientSocket::~ClientSocket() {
}
}
}  // end namespace facebook::wdt
