#pragma once

#include "WdtBase.h"

#include <sys/socket.h>
#include <string>

namespace facebook {
namespace wdt {

class SocketUtils {
 public:
  static int getReceiveBufferSize(int fd);
  static std::string getNameInfo(const struct sockaddr *sa, socklen_t salen);
  static void setReadTimeout(int fd);
  static void setWriteTimeout(int fd);
  /// @see ioWithAbortCheck
  static ssize_t readWithAbortCheck(int fd, char *buf, size_t nbyte,
                                    WdtBase::IAbortChecker const *abortChecker,
                                    bool tryFull);
  /// @see ioWithAbortCheck
  static ssize_t writeWithAbortCheck(int fd, const char *buf, size_t nbyte,
                                     WdtBase::IAbortChecker const *abortChecker,
                                     bool tryFull);

 private:
  /**
   * tries to read/write numBytes amount of data from fd. Also, checks for abort
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
   *                      Otherwise, this function returns after thefirst
   *                      successful read/write. This is set to false for
   *                      receiver pipelining.
   *
   * @return              in case of successnumber of bytes read/written, else
   *                      returns -1
   */
  template <typename F, typename T>
  static ssize_t ioWithAbortCheck(F readOrWrite, int fd, T tbuf,
                                  size_t numBytes,
                                  WdtBase::IAbortChecker const *abortChecker,
                                  int timeoutMs, bool tryFull);
};
}
}  // namespace facebook::wdt
