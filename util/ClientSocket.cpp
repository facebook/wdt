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

#include <glog/logging.h>
#include <sys/socket.h>
#include <poll.h>
#include <fcntl.h>

namespace facebook {
namespace wdt {

using std::string;

ClientSocket::ClientSocket(ThreadCtx &threadCtx, const string &dest,
                           const int port,
                           const EncryptionParams &encryptionParams)
    : WdtSocket(threadCtx, port, encryptionParams), dest_(dest) {
  memset(&sa_, 0, sizeof(sa_));
  if (threadCtx_.getOptions().ipv6) {
    sa_.ai_family = AF_INET6;
  }
  if (threadCtx_.getOptions().ipv4) {
    sa_.ai_family = AF_INET;
  }
  sa_.ai_socktype = SOCK_STREAM;
}

ErrorCode ClientSocket::connect() {
  WDT_CHECK(fd_ < 0) << "Previous connection not closed " << fd_ << " "
                     << port_;
  // Lookup
  struct addrinfo *infoList = nullptr;
  auto guard = folly::makeGuard([&] {
    if (infoList) {
      freeaddrinfo(infoList);
    }
  });
  string portStr = folly::to<string>(port_);
  int res = getaddrinfo(dest_.c_str(), portStr.c_str(), &sa_, &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(ERROR) << "Failed getaddrinfo " << dest_ << " , " << port_ << " : "
               << res << " : " << gai_strerror(res);
    return CONN_ERROR;
  }
  int count = 0;
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    ++count;
    std::string host, port;
    getNameInfo(info->ai_addr, info->ai_addrlen, host, port);
    VLOG(2) << "will connect to " << host << " " << port;
    fd_ = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (fd_ == -1) {
      PLOG(WARNING) << "Error making socket for port " << port_;
      continue;
    }
    VLOG(1) << "new socket " << fd_ << " for port " << port_;

    // make the socket non blocking
    int sockArg = fcntl(fd_, F_GETFL, nullptr);
    sockArg |= O_NONBLOCK;
    int retValue = fcntl(fd_, F_SETFL, sockArg);
    if (retValue == -1) {
      PLOG(ERROR) << "Could not make the socket non-blocking " << port_;
      closeConnection();
      continue;
    }

    if (::connect(fd_, info->ai_addr, info->ai_addrlen) != 0) {
      if (errno != EINPROGRESS) {
        PLOG(INFO) << "Error connecting on " << host << " " << port;
        closeConnection();
        continue;
      }
      auto startTime = Clock::now();
      int connectTimeout = threadCtx_.getOptions().connect_timeout_millis;

      while (true) {
        // check for abort
        if (threadCtx_.getAbortChecker()->shouldAbort()) {
          LOG(ERROR) << "Transfer aborted during connect " << port_ << " "
                     << fd_;
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
          VLOG(1) << "connect() timed out" << host << " " << port;
          closeConnection();
          return CONN_ERROR_RETRYABLE;
        }
        int pollTimeout =
            std::min(connectTimeout - timeElapsed,
                     threadCtx_.getOptions().abort_check_interval_millis);
        struct pollfd pollFds[] = {{fd_, POLLOUT, 0}};

        int retValue;
        if ((retValue = poll(pollFds, 1, pollTimeout)) <= 0) {
          if (errno == EINTR) {
            VLOG(1) << "poll() call interrupted. retrying... " << port_;
            continue;
          }
          if (retValue == 0) {
            VLOG(1) << "poll() timed out " << host << " " << port;
            continue;
          }
          PLOG(ERROR) << "poll() failed " << host << " " << port << " " << fd_;
          closeConnection();
          return CONN_ERROR;
        }
        break;
      }

      // have to check whether the connection attempt succeeded
      int connectResult;
      socklen_t len = sizeof(connectResult);
      if (getsockopt(fd_, SOL_SOCKET, SO_ERROR, &connectResult, &len) < 0) {
        PLOG(WARNING) << "getsockopt() failed";
        closeConnection();
        continue;
      }
      if (connectResult != 0) {
        LOG(WARNING) << "connect did not succeed on " << host << " " << port
                     << " : " << strerrorStr(connectResult);
        closeConnection();
        continue;
      }
    }

    // Set to blocking mode again
    sockArg = fcntl(fd_, F_GETFL, nullptr);
    sockArg &= (~O_NONBLOCK);
    retValue = fcntl(fd_, F_SETFL, sockArg);
    if (retValue == -1) {
      PLOG(ERROR) << "Could not make the socket blocking " << port_;
      closeConnection();
      continue;
    }
    VLOG(1) << "Successful connect on " << fd_;
    peerIp_ = host;
    sa_ = *info;
    break;
  }
  if (fd_ < 0) {
    if (count > 1) {
      // Only log this if not redundant with log above (ie --ipv6=false)
      LOG(INFO) << "Unable to connect to either of the " << count << " addrs";
    }
    return CONN_ERROR_RETRYABLE;
  }
  setSocketTimeouts();
  return OK;
}

const std::string &ClientSocket::getPeerIp() const {
  return peerIp_;
}

std::string ClientSocket::computeCurEncryptionTag() {
  return encryptor_.computeCurrentTag();
}

ClientSocket::~ClientSocket() {
}
}
}  // end namespace facebook::wdt
