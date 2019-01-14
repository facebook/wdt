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
#include <wdt/util/IServerSocket.h>

#include <netdb.h>
#include <string>
#include <vector>

namespace facebook {
namespace wdt {

typedef struct addrinfo *addrInfoList;

class ServerSocket : public IServerSocket {
 public:
  ServerSocket(ThreadCtx &threadCtx, int port, int backlog,
               const EncryptionParams &encryptionParams,
               int64_t ivChangeInterval, Func &&tagVerificationSuccessCallback);
  ~ServerSocket() override;
  /// Sets up listening socket (first wildcard type (ipv4 or ipv6 depending
  /// on flag)).
  ErrorCode listen() override;
  /// will accept next (/only) incoming connection
  /// @param timeoutMillis        accept timeout in millis
  /// @param tryCurAddressFirst   if this is true, current address is tried
  ///                             first during poll round-robin
  ErrorCode acceptNextConnection(int timeoutMillis,
                                 bool tryCurAddressFirst) override;
  /// @return       peer ip
  std::string getPeerIp() const override;
  /// @return       peer port
  std::string getPeerPort() const override;
  int getBackLog() const override;
  /// Destroy the active connection and the listening fd
  /// if done by the same thread who owned the socket
  /// This call should be avoided. correct end should be using closeConnetion()
  /// as this does not properly logically close the socket and is meant for
  //  abort/errors only.
  void closeAllNoCheck() override;

  /// tries to read nbyte data and periodically checks for abort
  int read(char *buf, int nbyte, bool tryFull = true) override {
    return socket_->read(buf, nbyte, tryFull);
  }

  /// tries to read nbyte data with a specific timeout and periodically checks
  /// for abort
  int readWithTimeout(char *buf, int nbyte, int timeoutMs,
                      bool tryFull = true) override {
    return socket_->readWithTimeout(buf, nbyte, timeoutMs, tryFull);
  }

  /// tries to write nbyte data and periodically checks for abort, if retry is
  /// true, socket tries to write as long as it makes some progress within a
  /// write timeout
  int write(char *buf, int nbyte, bool retry = false) override {
    return socket_->write(buf, nbyte, retry);
  }

  /// writes the tag/mac (for gcm) and shuts down the write half of the
  /// underlying socket
  ErrorCode shutdownWrites() override {
    return socket_->shutdownWrites();
  }

  /// expect logical and physical end of stream: read the tag and finialize
  ErrorCode expectEndOfStream() override {
    return socket_->expectEndOfStream();
  }

  /**
   * Normal closing of the current connection.
   * may return ENCRYPTION_ERROR if the stream is corrupt (gcm mode)
   */
  ErrorCode closeConnection() override {
    return socket_->closeConnection();
  }

  /**
   * Close unexpectedly (will not read/write the checksum).
   * This api is to be avoided/elminated.
   */
  void closeNoCheck() override {
    socket_->closeNoCheck();
  }

  /// @return     current fd
  int getFd() const override {
    return socket_->getFd();
  }

  /// @return     port
  int getPort() const override {
    return socket_->getPort();
  }

  /// @return     current encryption type
  EncryptionType getEncryptionType() const override {
    return socket_->getEncryptionType();
  }

  /// @return     possible non-retryable error
  ErrorCode getNonRetryableErrCode() const override {
    return socket_->getNonRetryableErrCode();
  }

  /// @return     read error code
  ErrorCode getReadErrCode() const override {
    return socket_->getReadErrCode();
  }

  /// @return     write error code
  ErrorCode getWriteErrCode() const override {
    return socket_->getWriteErrCode();
  }

  void disableIvChange() override {
    socket_->disableIvChange();
  }

 private:
  std::unique_ptr<WdtSocket> socket_{nullptr};
  ThreadCtx &threadCtx_;

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
