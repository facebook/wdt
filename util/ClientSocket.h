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
#include <netdb.h>
#include <wdt/ErrorCodes.h>
#include <wdt/util/WdtSocket.h>

namespace facebook {
namespace wdt {
class ClientSocket : public WdtSocket {
 public:
  ClientSocket(const std::string &dest, const int port,
               IAbortChecker const *abortChecker,
               const EncryptionParams &encryptionParams);
  virtual ErrorCode connect();
  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  int getUnackedBytes() const;
  /// @return   peer-ip of the connected socket
  const std::string &getPeerIp() const;
  virtual void shutdown();
  virtual ~ClientSocket();

 protected:
  const std::string dest_;
  std::string peerIp_;
  struct addrinfo sa_;
};
}
}  // namespace facebook::wdt
