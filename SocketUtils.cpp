#include "SocketUtils.h"

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
void SocketUtils::setReadTimeout(int fd, int timeout) {
  struct timeval tv;
  tv.tv_sec = timeout;
  tv.tv_usec = 0;
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv, sizeof(struct timeval));
}
}
}
