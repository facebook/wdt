#pragma once

#include <sys/socket.h>
#include <wdt/ErrorCodes.h>
#include <wdt/Protocol.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/util/EncryptionUtils.h>
#include <memory>

namespace facebook {
namespace wdt {

using Func = std::function<void()>;

/// base socket class
/// Do not read/write more than 2Gb at a time (int sizes)
/// This is ok because you don't want internal blocks bigger than a few
/// Mbytes anyway.
class WdtSocket {
 public:
  WdtSocket(ThreadCtx &threadCtx, int port,
            const EncryptionParams &encryptionParams, int64_t ivChangeInterval,
            Func &&tagVerificationSuccessCallback);

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
  ErrorCode shutdownWrites();

  /// expect logical and physical end of stream: read the tag and finialize
  ErrorCode expectEndOfStream();

  /**
   * Normal closing of the current connection.
   * may return ENCRYPTION_ERROR if the stream is corrupt (gcm mode)
   */
  ErrorCode closeConnection();

  /**
   * Close unexpectedly (will not read/write the checksum).
   * This api is to be avoided/elminated.
   */
  void closeNoCheck();

  /// @return     current fd
  int getFd() const;

  void setFd(int fd);

  /// @return     port
  int getPort() const;

  void setPort(int port);

  /// @return     current encryption type
  EncryptionType getEncryptionType() const;

  /// @return     possible non-retryable error
  ErrorCode getNonRetryableErrCode() const;

  /// @return     read error code
  ErrorCode getReadErrCode() const;

  /// @return     write error code
  ErrorCode getWriteErrCode() const;

  /// @return   tcp receive buffer size
  int getReceiveBufferSize() const;

  /// @return   tcp send buffer size
  int getSendBufferSize() const;

  /// @return   number of unacked bytes in send buffer, returns -1 in case it
  ///           fails to get unacked bytes for this socket
  int getUnackedBytes() const;

  int64_t getNumRead() const {
    return totalRead_;
  }

  int64_t getNumWritten() const {
    return totalWritten_;
  }

  void disableIvChange() {
    WLOG(INFO) << "Disabling periodic encryption iv change";
    ivChangeInterval_ = 0;
  }

  /// sets read and write timeouts for the socket
  void setSocketTimeouts();

  // manipulates DSCP Bits
  void setDscp(int dscp);

  ThreadCtx& getThreadCtx() {
    return threadCtx_;
  }

  void enableUnencryptedPeerSupport() {
    supportUnencryptedPeer_ = true;
  }
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

  virtual ~WdtSocket();

 private:
  void resetEncryptor();

  void resetDecryptor();

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

  // computes next tag offset
  int computeNextTagOffset(int64_t totalProcessed, int64_t tagInterval);

  // reads from socket and decrypts. Does not understand tag verification
  int readAndDecrypt(char *buf, int nbyte, int timeoutMs, bool tryFull);

  // reads from socket, decrypts and verifies tag. If the read contains a tag,
  // first, we read till the tag and decrypt. Then, the tag(plain-text) is read
  // and verified. After that remaining bytes are read.
  // This method expects one tag contained in the read. So, nbyte must be
  // less than readTagInterval_
  int readAndDecryptWithTag(char *buf, int nbyte, int timeoutMs, bool tryFull);

  // reads encryption tag. Returns empty string in case of failure.
  std::string readEncryptionTag();

  // checks whether decryption iv has changed or not. If yes, it reads the new
  // iv
  bool checkAndChangeDecryptionIv(const std::string &tag);

  // reads from socket. Does not understand encryption
  int readInternal(char *buf, int nbyte, int timeoutMs, bool tryFull);

  // encrypts and writes. Does not understand encryption tag
  int encryptAndWrite(char *buf, int nbyte, int timeoutMs, bool retry);

  // encrypts, writes and also adds tag if necessary.
  // This method expects one tag contained in the write. So, nbyte must be less
  // than writeTagInterval_
  int encryptAndWriteWithTag(char *buf, int nbyte, int timeoutMs, bool retry);

  // writes encryption tag. Returns status
  bool writeEncryptionTag();

  // checks whether encryption iv needs to change, and if yes, changes it. This
  // also sends the new iv
  bool checkAndChangeEncryptionIv();

  // writes to socket. Does not understand encryption
  int writeInternal(const char *buf, int nbyte, int timeoutMs, bool retry);

  void readEncryptionSettingsOnce(int timeoutMs);

  void writeEncryptionSettingsOnce();

  /// If doTagIOs is false will not try to read/write the final encryption tag
  virtual ErrorCode closeConnectionInternal(bool doTagIOs);

  ErrorCode finalizeWrites(bool doTagIOs);

  ErrorCode finalizeReads(bool doTagIOs);

  int port_{-1};
  int fd_{-1};

  ThreadCtx &threadCtx_;

  EncryptionParams encryptionParams_;
  int64_t ivChangeInterval_{0};

  Func tagVerificationSuccessCallback_{nullptr};

  bool encryptionSettingsWritten_{false};
  bool encryptionSettingsRead_{false};
  std::unique_ptr<AESEncryptor> encryptor_;
  std::unique_ptr<AESDecryptor> decryptor_;
  /// buffer used to encrypt/decrypt
  char buf_[Protocol::kEncryptionCmdLen];

  int32_t readTagInterval_{0};
  int32_t writeTagInterval_{0};

  int64_t totalRead_{0};
  int64_t totalWritten_{0};

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
};
}
}
