#include "ClientSocket.h"
#include "ServerSocket.h"  // for getnameinfo

#include <glog/logging.h>
#include <sys/socket.h>

DEFINE_bool(ipv6, true, "use ipv6 only");
DEFINE_bool(ipv4, false, "use ipv4 only, takes precedence over -ipv6");

namespace facebook {
namespace wdt {

using std::string;

ClientSocket::ClientSocket(string dest, string port)
    : dest_(dest), port_(port), fd_(-1) {
  memset(&sa_, 0, sizeof(sa_));
  if (FLAGS_ipv6) {
    sa_.ai_family = AF_INET6;
  }
  if (FLAGS_ipv4) {
    sa_.ai_family = AF_INET;
  }
  sa_.ai_socktype = SOCK_STREAM;
}

bool ClientSocket::connect() {
  if (fd_ > 0) {
    return true;
  }
  // Lookup
  struct addrinfo *infoList;
  int res = getaddrinfo(dest_.c_str(), port_.c_str(), &sa_, &infoList);
  if (res) {
    // not errno, can't use PLOG (perror)
    LOG(FATAL) << "Failed getaddrinfo " << dest_ << " , " << port_ << " : "
               << res << " : " << gai_strerror(res);
  }
  for (struct addrinfo *info = infoList; info != nullptr;
       info = info->ai_next) {
    VLOG(2) << "will connect to "
            << ServerSocket::getNameInfo(info->ai_addr, info->ai_addrlen);
    fd_ = socket(info->ai_family, info->ai_socktype, info->ai_protocol);
    if (fd_ == -1) {
      PLOG(WARNING) << "Error making socket";
      continue;
    }
    if (::connect(fd_, info->ai_addr, info->ai_addrlen)) {
      PLOG(WARNING) << "Error connecting";
      close(fd_);
      fd_ = -1;
      continue;
    }
    VLOG(1) << "Successful connect on " << fd_;
    sa_ = *info;
    break;
  }
  freeaddrinfo(infoList);
  if (fd_ <= 0) {
    LOG(ERROR) << "Unable to connect";
    return false;
  }
  // TODO: set sock options
  return true;
}

int ClientSocket::getFd() const {
  VLOG(1) << "fd is " << fd_;
  return fd_;
}
}
}  // end namespace facebook::wtd
