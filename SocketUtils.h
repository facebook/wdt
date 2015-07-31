/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "WdtBase.h"

#include <sys/socket.h>
#include <string>

namespace facebook {
namespace wdt {

class SocketUtils {
 public:
  static int getReceiveBufferSize(int fd);
  /**
   * Returns ip and port for a socket address
   *
   * @param sa      socket address
   * @param salen   socket address length
   * @param host    this is set to host name
   * @param port    this is set to port
   *
   * @return        whether getnameinfo was successful or not
   */
  static bool getNameInfo(const struct sockaddr *sa, socklen_t salen,
                          std::string &host, std::string &port);
  static void setReadTimeout(int fd);
  static void setWriteTimeout(int fd);
  /// @see ioWithAbortCheck
  static int64_t readWithAbortCheck(int fd, char *buf, int64_t nbyte,
                                    WdtBase::IAbortChecker const *abortChecker,
                                    bool tryFull);
  /// @see ioWithAbortCheck
  static int64_t writeWithAbortCheck(int fd, const char *buf, int64_t nbyte,
                                     WdtBase::IAbortChecker const *abortChecker,
                                     bool tryFull);

 private:
  /**
   * Tries to read/write numBytes amount of data from fd. Also, checks for abort
   * after every read/write call. Also, retries till the input timeout.
   * Optionally, returns after first successful read/write call.
   *
   * @param readOrWrite   read/write
   * @param fd            socket file descriptor
   * @param tbuf          buffer
   * @param numBytes      number of bytes to read/write
   * @param abortChecker  abort checker callback
   * @param timeoutMs     timeout in milliseconds
   * @param tryFull       if true, this function tries to read complete data.
   *                      Otherwise, this function returns after the first
   *                      successful read/write. This is set to false for
   *                      receiver pipelining.
   *
   * @return              in case of success number of bytes read/written, else
   *                      returns -1
   */
  template <typename F, typename T>
  static int64_t ioWithAbortCheck(F readOrWrite, int fd, T tbuf,
                                  int64_t numBytes,
                                  WdtBase::IAbortChecker const *abortChecker,
                                  int timeoutMs, bool tryFull);

  /**
   * Depending on the network timeout and abort interval, returns the timeout to
   * set for socket operations
   *
   * @param networkTimeout  network timeout
   *
   * @return                timeout for socket operations
   */
  static int getTimeout(int networkTimeout);
};
}
}  // namespace facebook::wdt
