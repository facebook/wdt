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
#include <string>

namespace facebook {
namespace wdt {
class ClientSocket : public WdtSocket {
 public:
  ClientSocket(ThreadCtx &threadCtx, const std::string &dest, int port,
               const EncryptionParams &encryptionParams);
  virtual ErrorCode connect();
  /// @return   peer-ip of the connected socket
  const std::string &getPeerIp() const;
  /// @return   current encryptor tag
  std::string computeCurEncryptionTag();
  /// shutdown() is now on WdtSocket as shutdownWrites()
  virtual ~ClientSocket();

 protected:
  /// sets the send buffer size for this socket
  void setSendBufferSize();

  const std::string dest_;
  std::string peerIp_;
  struct addrinfo sa_;
};
}
}  // namespace facebook::wdt
