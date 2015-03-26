#include "SocketUtils.h"
#include "WdtOptions.h"

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
void SocketUtils::setReadTimeout(int fd) {
  const auto &options = WdtOptions::get();
  if (options.read_timeout_millis > 0) {
    struct timeval tv;
    tv.tv_sec = options.read_timeout_millis / 1000;            // milli to sec
    tv.tv_usec = (options.read_timeout_millis % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}

/* statis */
void SocketUtils::setWriteTimeout(int fd) {
  const auto &options = WdtOptions::get();
  if (options.write_timeout_millis > 0) {
    struct timeval tv;
    tv.tv_sec = options.write_timeout_millis / 1000;  // milli to sec
    tv.tv_usec =
        (options.write_timeout_millis % 1000) * 1000;  // milli to micro
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,
               sizeof(struct timeval));
  }
}
}
}
