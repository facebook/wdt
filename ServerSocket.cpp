#include "ServerSocket.h"

#include <glog/logging.h>
#include <sys/socket.h>

#include "folly/Conv.h"

DECLARE_bool(ipv6);

namespace facebook {
namespace wdt {

using std::string;

ServerSocket::ServerSocket(string port, int backlog)
    : port_(port), backlog_(backlog), listeningFd_(-1), fd_(-1) {
  memset(&sa_, 0, sizeof(sa_));
  if (FLAGS_ipv6) {
    sa_.ai_family = AF_INET6;
  }
  sa_.ai_socktype = SOCK_STREAM;
  sa_.ai_flags = AI_PASSIVE;
}

ServerSocket::~ServerSocket() {
  LOG(INFO) << "~ServerSocket: potentially closing server socket "
            << listeningFd_ << " and most recent connection " << fd_;
  if (listeningFd_ >= 0) {
    close(listeningFd_);
    listeningFd_ = -1;
  }
  if (fd_ >= 0) {
    close(fd_);  // this probably fails because it's already closed by client
    fd_ = -1;
  }
}

/* static */
string ServerSocket::getNameInfo(const struct sockaddr *sa, socklen_t salen) {
  char host[NI_MAXHOST], service[NI_MAXSERV];
  int res = getnameinfo(sa, salen, host, sizeof(host), service, sizeof(service),
                        NI_NUMERICHOST | NI_NUMERICSERV);
  if (res) {
    LOG(ERROR) << "getnameinfo failed " << gai_strerror(res);
  }
  return folly::to<string>(host, " ", service);
}

bool ServerSocket::listen() {
  if (listeningFd_ > 0) {
    return true;
  }
  // Lookup
  struct addrinfo *infoList;
  int res = getaddrinfo(nullptr, port_.c_str(), &sa_, &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(FATAL) << "Failed getaddrinfo ai_passive on " << port_ << " : " << res
               << " : " << gai_strerror(res);
  }
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    LOG(INFO) << "will listen on "
              << getNameInfo(info->ai_addr, info->ai_addrlen);
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
    LOG(INFO) << "Successful bind on " << listeningFd_;
    sa_ = *info;
    break;
  }
  freeaddrinfo(infoList);
  if (listeningFd_ <= 0) {
    LOG(ERROR) << "Unable to bind";
    return false;
  }
  if (::listen(listeningFd_, backlog_)) {
    PLOG(ERROR) << "listen error";
    close(listeningFd_);
    listeningFd_ = -1;
    return false;
  }
  return true;
}

int ServerSocket::getNextFd() {
  if (!listen()) {
    return -1;
  }
  struct sockaddr addr;
  socklen_t addrLen = sizeof(addr);
  LOG(INFO) << "Waiting for new connection...";
  fd_ = accept(listeningFd_, &addr, &addrLen);
  if (fd_ < 0) {
    PLOG(ERROR) << "accept error";
  }
  LOG(INFO) << "new connection " << fd_ << " from "
            << getNameInfo(&addr, addrLen);
  // TODO: set sock options
  return fd_;
}
}
}  // end namespace facebook::wtd
