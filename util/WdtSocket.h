#pragma once

#include <sys/socket.h>
#include <wdt/ErrorCodes.h>
#include <wdt/Protocol.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/util/EncryptionUtils.h>
#include <memory>

namespace facebook {
namespace wdt {

/// base socket class
class WdtSocket {
 public:
  WdtSocket(ThreadCtx &threadCtx, int port,
            const EncryptionParams &encryptionParams);

  // making the object non-copyable and non-movable
  WdtSocket(const WdtSocket &stats) = delete;
  WdtSocket &operator=(const WdtSocket &stats) = delete;
  WdtSocket(WdtSocket &&stats) = delete;
  WdtSocket &operator=(WdtSocket &&stats) = delete;

  /// tries to read nbyte data and periodically checks for abort
  int read(char *buf, int nbyte, bool tryFull = true);

  /// tries to read nbyte data with a specific and periodically checks for abort
  int readWithTimeout(char *buf, int nbyte, int timeoutMs, bool tryFull = true);

  /// tries to write nbyte data and periodically checks for abort, if retry is
  /// true, socket tries to write as long as it makes some progress within a
  /// write timeout
  int write(char *buf, int nbyte, bool retry = false);

  /// writes the tag/mac (for gcm) and shuts down the write half of the
  /// underlying socket
  virtual ErrorCode shutdownWrites();

  /// expect logical and physical end of stream: read the tag and finialize
  virtual ErrorCode expectEndOfStream();

  /**
   * Normal closing of the current connection.
   * may return ENCRYPTION_ERROR if the stream is corrupt (gcm mode)
   */
  virtual ErrorCode closeConnection();

  /**
   * Close unexpectedly (will not read/write the checksum).
   * This api is to be avoided/elminated.
   */
  void closeNoCheck();

  /// @return     current fd
  int getFd() const;

  /// @return     port
  int getPort() const;

  /// @return     current encryption type
  EncryptionType getEncryptionType() const;

  /// @return     possible non-retryable error
  ErrorCode getNonRetryableErrCode() const;

  /// @return     read error code
  ErrorCode getReadErrCode() const;

  /// @return     write error code
  ErrorCode getWriteErrCode() const;

  /// saves decryptor ctx after offset
  void saveDecryptorCtx(int offset);

  /// verify whether the given tag matches previously saved context
  bool verifyTag(std::string &tag);

  /// @return   tcp receive buffer size
  int getReceiveBufferSize() const;

  /// @return   tcp send buffer size
  int getSendBufferSize() const;

  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  int getUnackedBytes() const;

  virtual ~WdtSocket();

 protected:
  // TODO: doc would be nice... (for tryFull, retry...)

  int readInternal(char *buf, int nbyte, int timeoutMs, bool tryFull);

  int writeInternal(const char *buf, int nbyte, int timeoutMs, bool retry);

  void readEncryptionSettingsOnce(int timeoutMs);

  void writeEncryptionSettingsOnce();

  /// If doTagIOs is false will not try to read/write the final encryption tag
  virtual ErrorCode closeConnectionInternal(bool doTagIOs);

  ErrorCode finalizeWrites(bool doTagIOs);

  ErrorCode finalizeReads(bool doTagIOs);

  /// sets read and write timeouts for the socket
  void setSocketTimeouts();

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

  int port_{-1};
  int fd_{-1};

  ThreadCtx &threadCtx_;

  EncryptionParams encryptionParams_;
  bool encryptionSettingsWritten_{false};
  bool encryptionSettingsRead_{false};
  AESEncryptor encryptor_;
  AESDecryptor decryptor_;
  /// buffer used to encrypt/decrypt
  char buf_[Protocol::kMaxEncryption];

  /// If this is true, then if we get a cmd other than encryption cmd from the
  /// peer, we expect the other side to not be encryption aware. We turn off
  /// encryption in that case
  bool supportUnencryptedPeer_{false};

  // need two error codes because a socket does bi-directional communication
  ErrorCode readErrorCode_{OK};
  ErrorCode writeErrorCode_{OK};

  /// Have we already completed encryption and wrote the tag
  bool writesFinalized_{false};
  /// Have we already read the tag and completed decryption
  bool readsFinalized_{false};

  const int OFFSET_NOT_SET = -1;
  const int CTX_SAVED = -2;

  /// offset after which decryptor ctx should be saved
  int ctxSaveOffset_{OFFSET_NOT_SET};

 private:
  /// computes effective timeout depending on the network timeout and abort
  /// check interval
  int getEffectiveTimeout(int networkTimeout);

  /// @see ioWithAbortCheck
  int64_t readWithAbortCheck(char *buf, int64_t nbyte, int timeoutMs,
                             bool tryFull);
  /// @see ioWithAbortCheck
  int64_t writeWithAbortCheck(const char *buf, int64_t nbyte, int timeoutMs,
                              bool tryFull);

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
  int64_t ioWithAbortCheck(F readOrWrite, T tbuf, int64_t numBytes,
                           int timeoutMs, bool tryFull);
};
}
}
