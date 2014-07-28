#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace facebook {
namespace wdt {
class ServerSocket {
 public:
  ServerSocket(std::string port, int backlog);
  ~ServerSocket();
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  bool listen();
  /// will accept next (/only) incoming connection
  int getNextFd();
  static std::string getNameInfo(const struct sockaddr* sa, socklen_t salen);

 private:
  const std::string port_;
  const int backlog_;
  int listeningFd_;
  int fd_;
  struct addrinfo sa_;
};
}
} // namespace facebook::wdt
