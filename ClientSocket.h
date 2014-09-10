#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace facebook {
namespace wdt {
class ClientSocket {
 public:
  ClientSocket(std::string dest, std::string port);
  bool connect();
  int getFd() const;

 private:
  const std::string dest_;
  const std::string port_;
  int fd_;
  struct addrinfo sa_;
};
}
}  // namespace facebook::wdt
