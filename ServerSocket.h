/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
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
  /// @return       peer ip
  std::string getPeerIp() const;
  /// @return       peer port
  std::string getPeerPort() const;
  int getFd() const;
  int getListenFd() const;
  int closeCurrentConnection();
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
  std::string peerIp_;
  std::string peerPort_;
  struct addrinfo sa_;
  WdtBase::IAbortChecker const *abortChecker_;
};
}
}  // namespace facebook::wdt
