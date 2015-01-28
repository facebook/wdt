#include "ServerSocket.h"
#include "SocketUtils.h"
#include "WdtOptions.h"
#include <glog/logging.h>
#include <sys/socket.h>
#include <folly/Conv.h>
namespace facebook {
namespace wdt {

using std::string;

ServerSocket::ServerSocket(string port, int backlog)
    : port_(port), backlog_(backlog), listeningFd_(-1), fd_(-1) {
  memset(&sa_, 0, sizeof(sa_));
  const auto &options = WdtOptions::get();
  if (options.ipv6_) {
    sa_.ai_family = AF_INET6;
  }
  sa_.ai_socktype = SOCK_STREAM;
  sa_.ai_flags = AI_PASSIVE;
}

ServerSocket::~ServerSocket() {
  VLOG(1) << "~ServerSocket: potentially closing server socket " << listeningFd_
          << " and most recent connection " << fd_;
  if (listeningFd_ >= 0) {
    close(listeningFd_);
    listeningFd_ = -1;
  }
  if (fd_ >= 0) {
    close(fd_);  // this probably fails because it's already closed by client
    fd_ = -1;
  }
}

ErrorCode ServerSocket::listen() {
  if (listeningFd_ > 0) {
    return OK;
  }
  // Lookup
  struct addrinfo *infoList;
  int res = getaddrinfo(nullptr, port_.c_str(), &sa_, &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(ERROR) << "Failed getaddrinfo ai_passive on " << port_ << " : " << res
               << " : " << gai_strerror(res);
    return CONN_ERROR;
  }
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    VLOG(1) << "will listen on "
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
      close(listeningFd_);
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
    close(listeningFd_);
    listeningFd_ = -1;
    return CONN_ERROR_RETRYABLE;
  }
  return OK;
}

ErrorCode ServerSocket::acceptNextConnection() {
  WDT_CHECK(listen() == OK);
  struct sockaddr addr;
  socklen_t addrLen = sizeof(addr);
  VLOG(1) << "Waiting for new connection...";
  fd_ = accept(listeningFd_, &addr, &addrLen);
  if (fd_ < 0) {
    PLOG(ERROR) << "accept error";
    return CONN_ERROR;
  }
  VLOG(1) << "new connection " << fd_ << " from "
          << SocketUtils::getNameInfo(&addr, addrLen);
  // TODO: set sock options
  return OK;
}

int ServerSocket::read(char *buf, int nbyte) const {
  return ::read(fd_, buf, nbyte);
}

int ServerSocket::write(char *buf, int nbyte) const {
  return ::write(fd_, buf, nbyte);
}

int ServerSocket::closeCurrentConnection() {
  return close(fd_);
}
int ServerSocket::getListenFd() const {
  return listeningFd_;
}
int ServerSocket::getFd() const {
  VLOG(1) << "fd is " << fd_;
  return fd_;
}
const std::string& ServerSocket::getPort() const {
  return port_;
}
int ServerSocket::getBackLog()  const{
  return backlog_;
}
}
}  // end namespace facebook::wtd
