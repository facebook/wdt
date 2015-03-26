#pragma once

#include "ErrorCodes.h"

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace facebook {
namespace wdt {
class ServerSocket {
 public:
  ServerSocket(std::string port, int backlog);
  virtual ~ServerSocket();
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  ErrorCode listen();
  /// will accept next (/only) incoming connection
  ErrorCode acceptNextConnection(int timeoutMillis);
  int read(char *buf, int nbyte) const;
  int write(char *buf, int nbyte) const;
  int getFd() const;
  int getListenFd() const;
  int closeCurrentConnection();
  static std::string getNameInfo(const struct sockaddr *sa, socklen_t salen);
  const std::string &getPort() const;
  int getBackLog() const;

 private:
  const std::string port_;
  const int backlog_;
  int listeningFd_;
  int fd_;
  struct addrinfo sa_;
};
}
}  // namespace facebook::wdt
