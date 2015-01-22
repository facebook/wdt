#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include "ErrorCodes.h"

namespace facebook {
namespace wdt {
class ClientSocket {
 public:
  ClientSocket(std::string dest, std::string port);
  virtual ErrorCode connect();
  virtual int read(char *buf, int nbyte) const;
  virtual int write(char *buf, int nbyte) const;
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
};
}
}  // namespace facebook::wdt
