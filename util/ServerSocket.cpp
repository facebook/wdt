/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/ServerSocket.h>
#include <wdt/util/SocketUtils.h>
#include <wdt/Reporting.h>
#include <wdt/WdtOptions.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <poll.h>
#include <folly/Conv.h>
#include <fcntl.h>
#include <algorithm>
namespace facebook {
namespace wdt {
using std::swap;
using std::string;

ServerSocket::ServerSocket(int32_t port, int backlog,
                           IAbortChecker const *abortChecker)
    : port_(port), backlog_(backlog), fd_(-1), abortChecker_(abortChecker) {
}

ServerSocket::ServerSocket(ServerSocket &&that) noexcept
    : backlog_(that.backlog_) {
  port_ = that.port_;
  listeningFds_ = std::move(that.listeningFds_);
  fd_ = that.fd_;
  abortChecker_ = that.abortChecker_;
  // A temporary ServerSocket should be changed such that
  // the fd doesn't get closed when it (temp obj) is getting
  // destructed and "this" object will remain intact
  that.fd_ = -1;
  // It is important that we change the port as well. So that the
  // socket that has been moved can't even attempt to listen on its port again
  that.port_ = -1;
}

ServerSocket &ServerSocket::operator=(ServerSocket &&that) {
  swap(port_, that.port_);
  swap(listeningFds_, that.listeningFds_);
  swap(fd_, that.fd_);
  swap(abortChecker_, that.abortChecker_);
  return *this;
}

void ServerSocket::closeAll() {
  VLOG(1) << "Destroying server socket (port, listen fd, fd)" << port_ << ", "
          << listeningFds_ << ", " << fd_;
  if (fd_ >= 0) {
    int ret = ::close(fd_);
    if (ret != 0) {
      PLOG(ERROR) << "Error closing fd for server socket. fd: " << fd_
                  << " port: " << port_;
    }
    fd_ = -1;
  }
  for (auto listeningFd : listeningFds_) {
    if (listeningFd >= 0) {
      int ret = ::close(listeningFd);
      if (ret != 0) {
        PLOG(ERROR)
            << "Error closing listening fd for server socket. listeningFd: "
            << listeningFd << " port: " << port_;
      }
    }
  }
  listeningFds_.clear();
}

ServerSocket::~ServerSocket() {
  closeAll();
}

int ServerSocket::listenInternal(struct addrinfo *info,
                                 const std::string &host) {
  VLOG(1) << "Will listen on " << host << " " << port_ << " "
          << info->ai_family;
  int listeningFd =
      socket(info->ai_family, info->ai_socktype, info->ai_protocol);
  if (listeningFd == -1) {
    PLOG(WARNING) << "Error making server socket " << host << " " << port_;
    return -1;
  }
  int optval = 1;
  if (setsockopt(listeningFd, SOL_SOCKET, SO_REUSEADDR, &optval,
                 sizeof(optval)) != 0) {
    PLOG(ERROR) << "Unable to set SO_REUSEADDR option " << host << " " << port_;
  }
  if (info->ai_family == AF_INET6) {
    // for ipv6 address, turn on ipv6 only flag
    if (setsockopt(listeningFd, IPPROTO_IPV6, IPV6_V6ONLY, &optval,
                   sizeof(optval)) != 0) {
      PLOG(ERROR) << "Unable to set IPV6_V6ONLY flag " << host << " " << port_;
    }
  }
  if (bind(listeningFd, info->ai_addr, info->ai_addrlen)) {
    PLOG(WARNING) << "Error binding " << host << " " << port_;
    ::close(listeningFd);
    return -1;
  }
  if (::listen(listeningFd, backlog_)) {
    PLOG(ERROR) << "listen error for port " << host << " " << port_;
    ::close(listeningFd);
    return -1;
  }
  return listeningFd;
}

int ServerSocket::getSelectedPortAndNewAddress(int listeningFd,
                                               struct addrinfo &sa,
                                               const std::string &host,
                                               addrInfoList &infoList) {
  int port;
  struct sockaddr_in sin;
  socklen_t len = sizeof(sin);
  if (getsockname(listeningFd, (struct sockaddr *)&sin, &len) == -1) {
    PLOG(ERROR) << "getsockname failed " << host;
    return -1;
  }
  port = ntohs(sin.sin_port);
  VLOG(1) << "auto configuring port to " << port;
  std::string portStr = folly::to<std::string>(port);
  int res = getaddrinfo(nullptr, portStr.c_str(), &sa, &infoList);
  if (res) {
    LOG(ERROR) << "getaddrinfo failed " << host << " " << port << " : "
               << gai_strerror(res);
    return -1;
  }
  if (infoList == nullptr) {
    LOG(ERROR) << "getaddrinfo unexpectedly returned nullptr " << host << " "
               << port;
    return -1;
  }
  return port;
}

ErrorCode ServerSocket::listen() {
  if (!listeningFds_.empty()) {
    return OK;
  }
  struct addrinfo sa;
  memset(&sa, 0, sizeof(sa));
  const auto &options = WdtOptions::get();
  if (options.ipv6) {
    sa.ai_family = AF_INET6;
  }
  if (options.ipv4) {
    sa.ai_family = AF_INET;
  }
  sa.ai_socktype = SOCK_STREAM;
  sa.ai_flags = AI_PASSIVE;
  // Lookup
  addrInfoList infoList = nullptr;
  std::string portStr = folly::to<std::string>(port_);
  int res = getaddrinfo(nullptr, portStr.c_str(), &sa, &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(ERROR) << "Failed getaddrinfo ai_passive on " << port_ << " : " << res
               << " : " << gai_strerror(res);
    return CONN_ERROR;
  }
  // if the port specified is 0, then a random port is selected for the first
  // address. We use that same port for other address types. Another addrinfo
  // list is created using the new port. This variable is used to ensure that we
  // do not try to bind again to the previous type.
  int addressTypeAlreadyBound = AF_UNSPEC;
  for (struct addrinfo *info = infoList; info != nullptr;) {
    if (info->ai_family == addressTypeAlreadyBound) {
      // we are already listening for this address type
      VLOG(2) << "Ignoring address family " << info->ai_family
              << " since we are already listing on it " << port_;
      info = info->ai_next;
      continue;
    }

    std::string host, port;
    if (SocketUtils::getNameInfo(info->ai_addr, info->ai_addrlen, host, port)) {
      // even if getnameinfo fail, we can still continue. Error is logged inside
      // SocketUtils
      WDT_CHECK(port_ == folly::to<int32_t>(port));
    }
    int listeningFd = listenInternal(info, host);
    if (listeningFd < 0) {
      info = info->ai_next;
      continue;
    }

    int addressFamily = info->ai_family;
    if (port_ == 0) {
      addrInfoList newInfoList = nullptr;
      int selectedPort =
          getSelectedPortAndNewAddress(listeningFd, sa, host, newInfoList);
      if (selectedPort < 0) {
        ::close(listeningFd);
        info = info->ai_next;
        continue;
      }
      port_ = selectedPort;
      addressTypeAlreadyBound = addressFamily;
      freeaddrinfo(infoList);
      infoList = newInfoList;
      info = infoList;
    } else {
      info = info->ai_next;
    }

    VLOG(1) << "Successful listen on " << listeningFd << " host " << host
            << " port " << port_ << " ai_family " << addressFamily;
    listeningFds_.emplace_back(listeningFd);
  }
  freeaddrinfo(infoList);
  if (listeningFds_.empty()) {
    LOG(ERROR) << "Unable to listen port " << port_;
    return CONN_ERROR_RETRYABLE;
  }
  return OK;
}

ErrorCode ServerSocket::acceptNextConnection(int timeoutMillis,
                                             bool tryCurAddressFirst) {
  ErrorCode code = listen();
  if (code != OK) {
    return code;
  }
  WDT_CHECK(!listeningFds_.empty());
  WDT_CHECK(timeoutMillis > 0);

  const int numFds = listeningFds_.size();
  struct pollfd pollFds[numFds];
  auto startTime = Clock::now();
  while (true) {
    // we need this loop because poll() can return before any file handles
    // have changes or before timing out. In that case, we check whether it
    // is because of EINTR or not. If true, we have to try poll with
    // reduced timeout
    int timeElapsed = durationMillis(Clock::now() - startTime);
    if (timeElapsed >= timeoutMillis) {
      VLOG(1) << "accept() timed out";
      return CONN_ERROR;
    }
    int pollTimeout = timeoutMillis - timeElapsed;
    for (int i = 0; i < numFds; i++) {
      pollFds[i] = {listeningFds_[i], POLLIN, 0};
    }

    int retValue;
    if ((retValue = poll(pollFds, numFds, pollTimeout)) <= 0) {
      if (errno == EINTR) {
        VLOG(1) << "poll() call interrupted. retrying...";
        continue;
      }
      if (retValue == 0) {
        VLOG(3) << "poll() timed out on port : " << port_
                << ", listening fds : " << listeningFds_;
      } else {
        PLOG(ERROR) << "poll() failed on port : " << port_
                    << ", listening fds : " << listeningFds_;
      }
      return CONN_ERROR;
    }
    break;
  }

  if (lastCheckedPollIndex_ >= numFds) {
    // can happen if getaddrinfo returns different set of addresses
    lastCheckedPollIndex_ = 0;
  } else if (!tryCurAddressFirst) {
    // else try the next address
    lastCheckedPollIndex_ = (lastCheckedPollIndex_ + 1) % numFds;
  }

  for (int count = 0; count < numFds; count++) {
    auto &pollFd = pollFds[lastCheckedPollIndex_];
    if (pollFd.revents & POLLIN) {
      struct sockaddr_storage addr;
      socklen_t addrLen = sizeof(addr);
      fd_ = accept(pollFd.fd, (struct sockaddr *)&addr, &addrLen);
      if (fd_ < 0) {
        PLOG(ERROR) << "accept error";
        return CONN_ERROR;
      }
      SocketUtils::getNameInfo((struct sockaddr *)&addr, addrLen, peerIp_,
                               peerPort_);
      VLOG(1) << "New connection, fd : " << fd_ << " from " << peerIp_ << " "
              << peerPort_;
      SocketUtils::setReadTimeout(fd_);
      SocketUtils::setWriteTimeout(fd_);
      return OK;
    }
    lastCheckedPollIndex_ = (lastCheckedPollIndex_ + 1) % numFds;
  }
  LOG(ERROR) << "None of the listening fds got a POLLIN event " << port_;
  return CONN_ERROR;
}

std::string ServerSocket::getPeerIp() const {
  // we keep returning the peer ip for error printing
  return peerIp_;
}

std::string ServerSocket::getPeerPort() const {
  // we keep returning the peer port for error printing
  return peerPort_;
}

int ServerSocket::read(char *buf, int nbyte, bool tryFull) {
  return SocketUtils::readWithAbortCheck(fd_, buf, nbyte, abortChecker_,
                                         tryFull);
}

int ServerSocket::write(const char *buf, int nbyte, bool tryFull) {
  return SocketUtils::writeWithAbortCheck(fd_, buf, nbyte, abortChecker_,
                                          tryFull);
}

int ServerSocket::closeCurrentConnection() {
  int retValue = 0;
  if (fd_ >= 0) {
    retValue = ::close(fd_);
    fd_ = -1;
  }
  return retValue;
}

int ServerSocket::getFd() const {
  return fd_;
}

int32_t ServerSocket::getPort() const {
  return port_;
}

int ServerSocket::getBackLog() const {
  return backlog_;
}
}
}  // end namespace facebook::wdt
