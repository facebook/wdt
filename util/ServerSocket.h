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
#include <wdt/util/WdtSocket.h>

#include <netdb.h>
#include <string>
#include <vector>

namespace facebook {
namespace wdt {

typedef struct addrinfo *addrInfoList;

class ServerSocket : public WdtSocket {
 public:
  ServerSocket(ThreadCtx &threadCtx, int port, int backlog,
               const EncryptionParams &encryptionParams,
               int64_t ivChangeInterval, Func &&tagVerificationSuccessCallback);
  virtual ~ServerSocket();
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  ErrorCode listen();
  /// will accept next (/only) incoming connection
  /// @param timeoutMillis        accept timeout in millis
  /// @param tryCurAddressFirst   if this is true, current address is tried
  ///                             first during poll round-robin
  ErrorCode acceptNextConnection(int timeoutMillis, bool tryCurAddressFirst);
  /// @return       peer ip
  std::string getPeerIp() const;
  /// @return       peer port
  std::string getPeerPort() const;
  int getBackLog() const;
  /// Destroy the active connection and the listening fd
  /// if done by the same thread who owned the socket
  /// This call should be avoided. correct end should be using closeConnetion()
  /// as this does not properly logically close the socket and is meant for
  //  abort/errors only.
  void closeAllNoCheck();

 private:
  /// sets the receive buffer size for this socket
  void setReceiveBufferSize(int fd);

  const int backlog_;
  std::vector<int> listeningFds_;
  /// index of the poll-fd last checked. This is used to not try the same fd
  /// every-time. We use round-robin policy to avoid accepting from a single fd
  int lastCheckedPollIndex_{0};
  std::string peerIp_;
  std::string peerPort_;

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
