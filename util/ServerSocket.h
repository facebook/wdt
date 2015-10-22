/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ErrorCodes.h>
#include <wdt/AbortChecker.h>

#include <string>
#include <vector>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>

namespace facebook {
namespace wdt {

typedef struct addrinfo *addrInfoList;

class ServerSocket {
 public:
  ServerSocket(ServerSocket &&that) noexcept;
  ServerSocket(const ServerSocket &that) = delete;
  ServerSocket(int32_t port, int backlog, IAbortChecker const *abortChecker);
  ServerSocket &operator=(const ServerSocket &that) = delete;
  ServerSocket &operator=(ServerSocket &&that);
  virtual ~ServerSocket();
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  ErrorCode listen();
  /// will accept next (/only) incoming connection
  /// @param timeoutMillis        accept timeout in millis
  /// @param tryCurAddressFirst   if this is true, current address is tried
  ///                             first during poll round-robin
  ErrorCode acceptNextConnection(int timeoutMillis, bool tryCurAddressFirst);
  /// tries to read nbyte data and periodically checks for abort
  int read(char *buf, int nbyte, bool tryFull = true);
  /// tries to write nbyte data and periodically checks for abort
  int write(const char *buf, int nbyte, bool tryFull = true);
  /// @return       peer ip
  std::string getPeerIp() const;
  /// @return       peer port
  std::string getPeerPort() const;
  int getFd() const;
  int closeCurrentConnection();
  int32_t getPort() const;
  int getBackLog() const;
  /// Destroy the active connection and the listening fd
  /// if done by the same thread who owned the socket
  void closeAll();

 private:
  int32_t port_;
  const int backlog_;
  std::vector<int> listeningFds_;
  int fd_;
  /// index of the poll-fd last checked. This is used to not try the same fd
  /// every-time. We use round-robin policy to avoid accepting from a single fd
  int lastCheckedPollIndex_{0};
  std::string peerIp_;
  std::string peerPort_;
  IAbortChecker const *abortChecker_;

  /**
   * Tries to listen to addr provided
   *
   * @param info    address to listen to
   * @param host    ip address
   *                selection, this will be set
   *
   * @return        socket fd, -1 in case of error
   */
  int listenInternal(struct addrinfo *info, const std::string &host);

  /**
   * Returns the selected port and new address list to use
   *
   * @param listeningFd     socket fd
   * @param sa              socket address
   * @param host            ip address
   * @param infoList        this is set to the new address list
   *
   * @return                selected port if successful, -1 otherwise
   */
  int getSelectedPortAndNewAddress(int listeningFd, struct addrinfo &sa,
                                   const std::string &host,
                                   addrInfoList &infoList);
};
}
}  // namespace facebook::wdt
