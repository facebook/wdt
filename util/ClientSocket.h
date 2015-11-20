/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <string>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <wdt/AbortChecker.h>
#include <wdt/ErrorCodes.h>

namespace facebook {
namespace wdt {
class ClientSocket {
 public:
  ClientSocket(const std::string &dest, const int port,
               IAbortChecker const *abortChecker);
  virtual ErrorCode connect();
  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  int getUnackedBytes() const;
  /// tries to read nbyte data and periodically checks for abort
  virtual int read(char *buf, int nbyte, bool tryFull = true);
  /// tries to read nbyte data with a specificand periodically checks for abort
  virtual int readWithTimeout(char *buf, int nbyte, int timeoutMs,
                              bool tryFull = true);
  /// tries to write nbyte data and periodically checks for abort
  virtual int write(const char *buf, int nbyte, bool tryFull = true);
  virtual void close();
  int getFd() const;
  int getPort() const;
  /// @return   peer-ip of the connected socket
  const std::string &getPeerIp() const;
  virtual void shutdown();
  virtual ~ClientSocket();

 protected:
  const std::string dest_;
  const int port_;
  int fd_;
  std::string peerIp_;
  struct addrinfo sa_;
  IAbortChecker const *abortChecker_;
};
}
}  // namespace facebook::wdt
