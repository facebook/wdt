#pragma once

#include "ErrorCodes.h"
#include "WdtBase.h"

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace facebook {
namespace wdt {
class ServerSocket {
 public:
  ServerSocket(ServerSocket &&that) noexcept;
  ServerSocket(const ServerSocket &that) = delete;
  ServerSocket(int32_t port, int backlog,
               WdtBase::IAbortChecker const *abortChecker);
  ServerSocket &operator=(const ServerSocket &that) = delete;
  ServerSocket &operator=(ServerSocket &&that);
  virtual ~ServerSocket();
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  ErrorCode listen();
  /// will accept next (/only) incoming connection
  ErrorCode acceptNextConnection(int timeoutMillis);
  /// tries to read nbyte data and periodically checks for abort
  int read(char *buf, int nbyte, bool tryFull = true);
  /// tries to write nbyte data and periodically checks for abort
  int write(const char *buf, int nbyte, bool tryFull = true);
  int getFd() const;
  int getListenFd() const;
  int closeCurrentConnection();
  static std::string getNameInfo(const struct sockaddr *sa, socklen_t salen);
  int32_t getPort() const;
  int getBackLog() const;
  /// Destroy the active connection and the listening fd
  /// if done by the same thread who owned the socket
  void closeAll();

 private:
  int32_t port_;
  const int backlog_;
  int listeningFd_;
  int fd_;
  struct addrinfo sa_;
  WdtBase::IAbortChecker const *abortChecker_;
};
}
}  // namespace facebook::wdt
