#include "ServerSocket.h"
#include "SocketUtils.h"
#include "WdtOptions.h"
#include <glog/logging.h>
#include <sys/socket.h>
#include <folly/Conv.h>
#include <fcntl.h>
#include <chrono>
#include <algorithm>
namespace facebook {
namespace wdt {
using std::swap;
typedef std::chrono::high_resolution_clock Clock;

template <typename T>
int durationMillis(T d) {
  return std::chrono::duration_cast<std::chrono::milliseconds>(d).count();
}

using std::string;

ServerSocket::ServerSocket(int32_t port, int backlog)
    : port_(port), backlog_(backlog), listeningFd_(-1), fd_(-1) {
  memset(&sa_, 0, sizeof(sa_));
  const auto &options = WdtOptions::get();
  if (options.ipv6) {
    sa_.ai_family = AF_INET6;
  }
  if (options.ipv4) {
    sa_.ai_family = AF_INET;
  }
  sa_.ai_socktype = SOCK_STREAM;
  sa_.ai_flags = AI_PASSIVE;
}

ServerSocket::ServerSocket(ServerSocket &&that) noexcept
    : backlog_(that.backlog_) {
  port_ = that.port_;
  sa_ = that.sa_;
  listeningFd_ = that.listeningFd_;
  fd_ = that.fd_;
  // A temporary ServerSocket should be changed such that
  // the fd doesn't get closed when it (temp obj) is getting
  // destructed and "this" object will remain intact
  that.listeningFd_ = -1;
  that.fd_ = -1;
}

ServerSocket &ServerSocket::operator=(ServerSocket &&that) {
  swap(port_, that.port_);
  swap(sa_, that.sa_);
  swap(listeningFd_, that.listeningFd_);
  swap(fd_, that.fd_);
  return *this;
}

void ServerSocket::close() {
  VLOG(1) << "Destroying server socket (port, listen fd, fd)" << port_ << ", "
          << listeningFd_ << ", " << fd_;
  if (listeningFd_ >= 0) {
    ::close(listeningFd_);
    listeningFd_ = -1;
  }
  if (fd_ >= 0) {
    ::close(fd_);  // this probably fails because it's already closed by client
    fd_ = -1;
  }
}

ServerSocket::~ServerSocket() {
  close();
}

ErrorCode ServerSocket::listen() {
  if (listeningFd_ > 0) {
    return OK;
  }
  // Lookup
  struct addrinfo *infoList;
  int res = getaddrinfo(nullptr, folly::to<std::string>(port_).c_str(), &sa_,
                        &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(ERROR) << "Failed getaddrinfo ai_passive on " << port_ << " : " << res
               << " : " << gai_strerror(res);
    return CONN_ERROR;
  }
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    VLOG(1) << "Will listen on "
            << SocketUtils::getNameInfo(info->ai_addr, info->ai_addrlen);
    // TODO: set sock options : SO_REUSEADDR,...
    listeningFd_ =
        socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (listeningFd_ == -1) {
      PLOG(WARNING) << "Error making server socket";
      continue;
    }
    if (bind(listeningFd_, info->ai_addr, info->ai_addrlen)) {
      PLOG(WARNING) << "Error binding";
      ::close(listeningFd_);
      listeningFd_ = -1;
      continue;
    }
    VLOG(1) << "Successful bind on " << listeningFd_;
    sa_ = *info;
    break;
  }
  freeaddrinfo(infoList);
  if (listeningFd_ <= 0) {
    LOG(ERROR) << "Unable to bind";
    return CONN_ERROR_RETRYABLE;
  }
  if (::listen(listeningFd_, backlog_)) {
    PLOG(ERROR) << "listen error";
    ::close(listeningFd_);
    listeningFd_ = -1;
    return CONN_ERROR_RETRYABLE;
  }
  return OK;
}

ErrorCode ServerSocket::acceptNextConnection(int timeoutMillis) {
  ErrorCode code = listen();
  if (code != OK) {
    return code;
  }

  if (timeoutMillis > 0) {
    // zero value disables timeout
    auto startTime = Clock::now();
    while (true) {
      // we need this loop because select() can return before any file handles
      // have changes or before timing out. In that case, we check whether it
      // is becuse of EINTR or not. If true, we have to try select with
      // reduced timeout
      int timeElapsed = durationMillis(Clock::now() - startTime);
      if (timeElapsed >= timeoutMillis) {
        LOG(ERROR) << "accept() timed out";
        return CONN_ERROR;
      }
      int selectTimeout = timeoutMillis - timeElapsed;
      fd_set rfds;
      FD_ZERO(&rfds);
      FD_SET(listeningFd_, &rfds);
      struct timeval tv;
      tv.tv_sec = selectTimeout / 1000;
      tv.tv_usec = (selectTimeout % 1000) * 1000;
      if (select(FD_SETSIZE, &rfds, nullptr, nullptr, &tv) <= 0) {
        if (errno == EINTR) {
          VLOG(1) << "select() call interrupted. retrying...";
          continue;
        }
        VLOG(1) << "select() timed out";
        return CONN_ERROR;
      }
      break;
    }
  }

  struct sockaddr addr;
  socklen_t addrLen = sizeof(addr);
  fd_ = accept(listeningFd_, &addr, &addrLen);
  if (fd_ < 0) {
    PLOG(ERROR) << "accept error";
    return CONN_ERROR;
  }
  VLOG(1) << "new connection " << fd_ << " from "
          << SocketUtils::getNameInfo(&addr, addrLen);
  SocketUtils::setReadTimeout(fd_);
  SocketUtils::setWriteTimeout(fd_);
  return OK;
}

int ServerSocket::read(char *buf, int nbyte) const {
  while (true) {
    int retValue = ::read(fd_, buf, nbyte);
    if (retValue < 0 && errno == EINTR) {
      VLOG(2) << "received EINTR. continuing...";
      continue;
    }
    return retValue;
  }
}

int ServerSocket::write(char *buf, int nbyte) const {
  return ::write(fd_, buf, nbyte);
}

int ServerSocket::closeCurrentConnection() {
  int retValue = 0;
  if (fd_ >= 0) {
    retValue = ::close(fd_);
    fd_ = -1;
  }
  return retValue;
}

int ServerSocket::getListenFd() const {
  return listeningFd_;
}
int ServerSocket::getFd() const {
  VLOG(1) << "fd is " << fd_;
  return fd_;
}
int32_t ServerSocket::getPort() const {
  return port_;
}
int ServerSocket::getBackLog() const {
  return backlog_;
}
}
}  // end namespace facebook::wtd
