#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include "ErrorCodes.h"
#include "WdtBase.h"

namespace facebook {
namespace wdt {
class ClientSocket {
 public:
  ClientSocket(const std::string &dest, const std::string &port,
               WdtBase::IAbortChecker const *abortChecker);
  virtual ErrorCode connect();
  /// tries to read nbyte data and periodically checks for abort
  virtual int read(char *buf, int nbyte, bool tryFull = true) const;
  /// tries to write nbyte data and periodically checks for abort
  virtual int write(const char *buf, int nbyte, bool tryFull = true) const;
  void close();
  int getFd() const;
  std::string getPort() const;
  void shutdown() const;
  virtual ~ClientSocket();

 private:
  const std::string dest_;
  const std::string port_;
  int fd_;
  struct addrinfo sa_;
  WdtBase::IAbortChecker const *abortChecker_;
};
}
}  // namespace facebook::wdt
