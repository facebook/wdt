/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/ServerSocket.h>
#include <fcntl.h>
#include <folly/Conv.h>
#include <glog/logging.h>
#include <poll.h>
#include <sys/socket.h>
#include <algorithm>
namespace facebook {
namespace wdt {
using std::string;

ServerSocket::ServerSocket(ThreadCtx &threadCtx, int port, int backlog,
                           const EncryptionParams &encryptionParams,
                           int64_t ivChangeInterval,
                           Func &&tagVerificationSuccessCallback)
    : threadCtx_(threadCtx), backlog_(backlog) {
  socket_ = std::make_unique<WdtSocket>(threadCtx, port, encryptionParams,
                                    ivChangeInterval,
                                    std::move(tagVerificationSuccessCallback));
  // for backward compatibility
  socket_->enableUnencryptedPeerSupport();
}

void ServerSocket::closeAllNoCheck() {
  int port = socket_->getPort();
  int fd_ = socket_->getFd();

  WVLOG(1) << "Destroying server socket (port, listen fd, fd) " << port << ", "
           << listeningFds_ << ", " << fd_;
  closeNoCheck();
  // We don't care about listen error, the error that matters is encryption err
  for (auto listeningFd : listeningFds_) {
    if (listeningFd >= 0) {
      int ret = ::close(listeningFd);
      if (ret != 0) {
        WPLOG(ERROR)
            << "Error closing listening fd for server socket. listeningFd: "
            << listeningFd << " port: " << port;
      }
    }
  }
  listeningFds_.clear();
}

ServerSocket::~ServerSocket() {
  closeAllNoCheck();
}

int ServerSocket::listenInternal(struct addrinfo *info,
                                 const std::string &host) {
  int port = socket_->getPort();

  WVLOG(1) << "Will listen on " << host << " " << port << " "
           << info->ai_family;
  int listeningFd =
      socket(info->ai_family, info->ai_socktype, info->ai_protocol);
  if (listeningFd == -1) {
    WPLOG(WARNING) << "Error making server socket " << host << " " << port;
    return -1;
  }
  setReceiveBufferSize(listeningFd);
  int optval = 1;
  if (setsockopt(listeningFd, SOL_SOCKET, SO_REUSEADDR, &optval,
                 sizeof(optval)) != 0) {
    WPLOG(ERROR) << "Unable to set SO_REUSEADDR option " << host << " "
                 << port;
  }
  if (info->ai_family == AF_INET6) {
    // for ipv6 address, turn on ipv6 only flag
    if (setsockopt(listeningFd, IPPROTO_IPV6, IPV6_V6ONLY, &optval,
                   sizeof(optval)) != 0) {
      WPLOG(ERROR) << "Unable to set IPV6_V6ONLY flag " << host << " " << port;
    }
  }
  if (bind(listeningFd, info->ai_addr, info->ai_addrlen)) {
    WPLOG(WARNING) << "Error binding " << host << " " << port;
    ::close(listeningFd);
    return -1;
  }
  if (::listen(listeningFd, backlog_)) {
    WPLOG(ERROR) << "listen error for port " << host << " " << port;
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
    WPLOG(ERROR) << "getsockname failed " << host;
    return -1;
  }
  port = ntohs(sin.sin_port);
  WVLOG(1) << "auto configuring port to " << port;
  std::string portStr = folly::to<std::string>(port);
  int res = getaddrinfo(nullptr, portStr.c_str(), &sa, &infoList);
  if (res) {
    WLOG(ERROR) << "getaddrinfo failed " << host << " " << port << " : "
                << gai_strerror(res);
    return -1;
  }
  if (infoList == nullptr) {
    WLOG(ERROR) << "getaddrinfo unexpectedly returned nullptr " << host << " "
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
  int port = socket_->getPort();

  memset(&sa, 0, sizeof(sa));
  const WdtOptions &options = threadCtx_.getOptions();
  if (options.ipv6) {
    sa.ai_family = AF_INET6;
  }
  if (options.ipv4) {
    sa.ai_family = AF_INET;
  }
  sa.ai_socktype = SOCK_STREAM;
  sa.ai_flags = AI_PASSIVE;
  // Dynamic port is the default on receiver (and setting the start_port flag
  // explictly automatically also sets static_ports to false)
  if (!options.static_ports) {
    WVLOG(1) << "Not using static_ports, changing port " << port << " to 0";
    port = 0;
  }
  // Lookup
  addrInfoList infoList = nullptr;
  std::string portStr = folly::to<std::string>(port);
  int res = getaddrinfo(nullptr, portStr.c_str(), &sa, &infoList);
  if (res) {
    // not errno, can't use WPLOG (perror)
    WLOG(ERROR) << "Failed getaddrinfo ai_passive on " << port << " : " << res
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
      WVLOG(2) << "Ignoring address family " << info->ai_family
               << " since we are already listing on it " << port;
      info = info->ai_next;
      continue;
    }

    std::string host, portString;
    if (WdtSocket::getNameInfo(info->ai_addr, info->ai_addrlen, host,
                               portString)) {
      // even if getnameinfo fail, we can still continue. Error is logged inside
      // SocketUtils
      WDT_CHECK(port == folly::to<int32_t>(portString));
    }
    int listeningFd = listenInternal(info, host);
    if (listeningFd < 0) {
      info = info->ai_next;
      continue;
    }

    int addressFamily = info->ai_family;
    if (port == 0) {
      addrInfoList newInfoList = nullptr;
      int selectedPort =
          getSelectedPortAndNewAddress(listeningFd, sa, host, newInfoList);
      if (selectedPort < 0) {
        ::close(listeningFd);
        info = info->ai_next;
        continue;
      }
      port = selectedPort;
      socket_->setPort(port);
      addressTypeAlreadyBound = addressFamily;
      freeaddrinfo(infoList);
      infoList = newInfoList;
      info = infoList;
    } else {
      info = info->ai_next;
    }

    WVLOG(1) << "Successful listen on " << listeningFd << " host " << host
             << " port " << port << " ai_family " << addressFamily;
    listeningFds_.emplace_back(listeningFd);
  }
  freeaddrinfo(infoList);
  if (listeningFds_.empty()) {
    WLOG(ERROR) << "Unable to listen port " << port;
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

  auto port = socket_->getPort();
  auto fd = socket_->getFd();
  const WdtOptions &options = threadCtx_.getOptions();
  const bool checkAbort = (options.abort_check_interval_millis > 0);

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
      WVLOG(3) << "accept() timed out";
      return CONN_ERROR;
    }
    int pollTimeout = timeoutMillis - timeElapsed;
    if (checkAbort) {
      if (threadCtx_.getAbortChecker()->shouldAbort()) {
        WLOG(ERROR) << "Transfer aborted during accept " << port << " " << fd;
        return ABORT;
      }
      pollTimeout = std::min(pollTimeout, options.abort_check_interval_millis);
    }
    for (int i = 0; i < numFds; i++) {
      pollFds[i] = {listeningFds_[i], POLLIN, 0};
    }

    int retValue = poll(pollFds, numFds, pollTimeout);
    if (retValue > 0) {
      break;
    }
    if (errno == EINTR) {
      WVLOG(1) << "poll() call interrupted. retrying...";
      continue;
    }
    if (retValue == 0) {
      WVLOG(3) << "poll() timed out on port : " << port
               << ", listening fds : " << listeningFds_;
      continue;
    }
    WPLOG(ERROR) << "poll() failed on port : " << port
                 << ", listening fds : " << listeningFds_;
    return CONN_ERROR;
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
      fd = accept(pollFd.fd, (struct sockaddr *)&addr, &addrLen);
      if (fd < 0) {
        WPLOG(ERROR) << "accept error";
        return CONN_ERROR;
      }
      WdtSocket::getNameInfo((struct sockaddr *)&addr, addrLen,
                             peerIp_, peerPort_);
      WVLOG(1) << "New connection, fd : " << fd << " from " << peerIp_ << " "
               << peerPort_;
      socket_->setFd(fd);
      socket_->setSocketTimeouts();
      socket_->setDscp(options.dscp);

      return OK;
    }
    lastCheckedPollIndex_ = (lastCheckedPollIndex_ + 1) % numFds;
  }
  WLOG(ERROR) << "None of the listening fds got a POLLIN event " << port;
  return CONN_ERROR;
}

void ServerSocket::setReceiveBufferSize(int fd) {
  auto port = socket_->getPort();
  int bufSize = threadCtx_.getOptions().receive_buffer_size;
  if (bufSize <= 0) {
    return;
  }
  int status =
      ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, &bufSize, sizeof(bufSize));
  if (status != 0) {
    WPLOG(ERROR) << "Failed to set receive buffer " << port << " size "
                 << bufSize << " fd " << fd;
    return;
  }
  WVLOG(1) << "Receive buffer size set to " << bufSize << " port " << port;
}

std::string ServerSocket::getPeerIp() const {
  // we keep returning the peer ip for error printing
  return peerIp_;
}

std::string ServerSocket::getPeerPort() const {
  // we keep returning the peer port for error printing
  return peerPort_;
}

int ServerSocket::getBackLog() const {
  return backlog_;
}
}
}  // end namespace facebook::wdt
