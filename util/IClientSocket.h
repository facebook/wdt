/**
 * Copyright (c) 2018-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ErrorCodes.h>
#include <wdt/util/EncryptionUtils.h>
#include <string>

namespace facebook {
namespace wdt {
class IClientSocket {
 public:
  virtual ~IClientSocket() {
  }
  /// tries to establish a connection with the server port
  virtual ErrorCode connect() = 0;
  /// @return   peer-ip of the connected socket
  virtual const std::string &getPeerIp() const = 0;
  /// tries to read nbyte data and periodically checks for abort
  virtual int read(char *buf, int nbyte, bool tryFull = true) = 0;
  /// tries to read nbyte data with a specific timeout and periodically checks
  /// for abort
  virtual int readWithTimeout(char *buf, int nbyte, int timeoutMs,
                              bool tryFull = true) = 0;
  /// tries to write nbyte data, if retry is true, socket tries to
  /// write as long as it makes some progress within a write timeout
  virtual int write(char *buf, int nbyte, bool retry = false) = 0;

  virtual ErrorCode shutdownWrites() = 0;

  virtual ErrorCode expectEndOfStream() = 0;
  /**
   * Normal closing of the current connection.
   */
  virtual ErrorCode closeConnection() = 0;
  /// Close unexpectedly. This api should be avoided.
  virtual void closeNoCheck() = 0;
  /// @return     current fd
  virtual int getFd() const = 0;
  /// @return     port
  virtual int getPort() const = 0;
  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  virtual int getUnackedBytes() const = 0;
  /// @return     current encryption type
  virtual EncryptionType getEncryptionType() const = 0;
  /// @return     possible non-retryable error
  virtual ErrorCode getNonRetryableErrCode() const = 0;
  /// @return     read error code
  virtual ErrorCode getReadErrCode() const = 0;
  /// @return     write error code
  virtual ErrorCode getWriteErrCode() const = 0;
};
}  // namespace wdt
}  // namespace facebook
