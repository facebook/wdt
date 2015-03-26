#pragma once

#include <sys/socket.h>
#include <string>

namespace facebook {
namespace wdt {

class SocketUtils {
 public:
  static int getReceiveBufferSize(int fd);
  static std::string getNameInfo(const struct sockaddr *sa, socklen_t salen);
  static void setReadTimeout(int fd);
  static void setWriteTimeout(int fd);
};
}
}  // namespace facebook::wdt
