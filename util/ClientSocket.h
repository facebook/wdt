/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <netdb.h>
#include <wdt/ErrorCodes.h>
#include <wdt/util/WdtSocket.h>
#include <wdt/util/IClientSocket.h>
#include <string>

namespace facebook {
namespace wdt {
class ClientSocket : public IClientSocket {
 public:
  ClientSocket(ThreadCtx &threadCtx, const std::string &dest, int port,
               const EncryptionParams &encryptionParams,
               int64_t ivChangeInterval);
  virtual ErrorCode connect() override;
  /// @return   peer-ip of the connected socket
  const std::string &getPeerIp() const override;
  /// shutdown() is now on WdtSocket as shutdownWrites()
  virtual ~ClientSocket () override;

  /// tries to read nbyte data and periodically checks for abort
  int read(char *buf, int nbyte, bool tryFull = true) override {
    return socket_->read(buf, nbyte, tryFull);
  }

  /// tries to read nbyte data with a specific and periodically checks for abort
  int readWithTimeout(char *buf, int nbyte, int timeoutMs,
                      bool tryFull = true) override {
    return socket_->readWithTimeout(buf, nbyte, timeoutMs,tryFull);
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

  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  int getUnackedBytes() const override {
    return socket_->getUnackedBytes();
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

 protected:
  /// sets the send buffer size for this socket
  void setSendBufferSize();

  void setFd(int fd) {
    socket_->setFd(fd);
  }

  const std::string dest_;
  std::string peerIp_;
  struct addrinfo sa_;
  std::unique_ptr<WdtSocket> socket_{nullptr};
  ThreadCtx &threadCtx_;
};
}
}  // namespace facebook::wdt
