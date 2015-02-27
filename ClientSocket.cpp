#include "ClientSocket.h"
#include "SocketUtils.h"
#include "WdtOptions.h"
#include <glog/logging.h>
#include <sys/socket.h>

namespace facebook {
namespace wdt {

using std::string;

ClientSocket::ClientSocket(string dest, string port)
    : dest_(dest), port_(port), fd_(-1) {
  memset(&sa_, 0, sizeof(sa_));
  const auto &options = WdtOptions::get();
  if (options.ipv6) {
    sa_.ai_family = AF_INET6;
  }
  if (options.ipv4) {
    sa_.ai_family = AF_INET;
  }
  sa_.ai_socktype = SOCK_STREAM;
}

ErrorCode ClientSocket::connect() {
  if (fd_ > 0) {
    return OK;
  }
  // Lookup
  struct addrinfo *infoList;
  int res = getaddrinfo(dest_.c_str(), port_.c_str(), &sa_, &infoList);
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
    VLOG(2) << "will connect to "
            << SocketUtils::getNameInfo(info->ai_addr, info->ai_addrlen);
    fd_ = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (fd_ == -1) {
      PLOG(WARNING) << "Error making socket";
      continue;
    }
    if (::connect(fd_, info->ai_addr, info->ai_addrlen)) {
      PLOG(INFO) << "Error connecting on "
                 << SocketUtils::getNameInfo(info->ai_addr, info->ai_addrlen);
      this->close();
      continue;
    }
    VLOG(1) << "Successful connect on " << fd_;
    sa_ = *info;
    break;
  }
  freeaddrinfo(infoList);
  if (fd_ < 0) {
    if (count > 1) {
      // Only log this if not redundant with log above (ie --ipv6=false)
      LOG(INFO) << "Unable to connect to either of the " << count << " addrs";
    }
    return CONN_ERROR_RETRYABLE;
  }
  // TODO: set sock options
  return OK;
}

int ClientSocket::getFd() const {
  VLOG(1) << "fd is " << fd_;
  return fd_;
}

std::string ClientSocket::getPort() const {
  return port_;
}

int ClientSocket::read(char *buf, int nbyte) const {
  return ::read(fd_, buf, nbyte);
}

int ClientSocket::write(char *buf, int nbyte) const {
  return ::write(fd_, buf, nbyte);
}

void ClientSocket::close() {
  if (fd_ >= 0) {
    VLOG(1) << "Closing socket : " << fd_;
    if (::close(fd_) < 0) {
      VLOG(1) << "Socket close failed for fd " << fd_;
    }
    fd_ = -1;
  }
}

void ClientSocket::shutdown() const {
  if (::shutdown(fd_, SHUT_WR) < 0) {
    VLOG(1) << "Socket shutdown failed for fd " << fd_;
  }
}

ClientSocket::~ClientSocket() {
  this->close();
}
}
}  // end namespace facebook::wtd
