#include <wdt/util/WdtSocket.h>
#include <wdt/Protocol.h>
#include <folly/Bits.h>
#include <folly/String.h>  // for humanify
#include <unistd.h>
#include <netdb.h>
#include <sys/ioctl.h>
#ifdef WDT_HAS_SOCKIOS_H
#include <linux/sockios.h>
#endif

namespace facebook {
namespace wdt {

WdtSocket::WdtSocket(ThreadCtx &threadCtx, const int port,
                     const EncryptionParams &encryptionParams)
    : port_(port), threadCtx_(threadCtx), encryptionParams_(encryptionParams) {
}

// TODO: consider refactoring this to return error code
void WdtSocket::readEncryptionSettingsOnce(int timeoutMs) {
  if (!encryptionParams_.isSet() || encryptionSettingsRead_) {
    return;
  }
  WDT_CHECK(!encryptionParams_.getSecret().empty());

  int numRead = readInternal(buf_, 1, timeoutMs, true);
  if (numRead != 1) {
    LOG(ERROR) << "Failed to read encryption settings " << numRead << " "
               << port_;
    return;
  }
  if (buf_[0] != Protocol::ENCRYPTION_CMD) {
    LOG(ERROR) << "Expected to read ENCRYPTION_CMD(e), but got " << buf_[0];
    readErrorCode_ = UNEXPECTED_CMD_ERROR;
    return;
  }
  int toRead = Protocol::kMaxEncryption - 1;  // already read 1 byte for cmd
  numRead = readInternal(buf_, toRead,
                         threadCtx_.getOptions().read_timeout_millis, true);
  if (numRead != toRead) {
    LOG(ERROR) << "Failed to read encryption settings " << numRead << " "
               << toRead << " " << port_;
    readErrorCode_ = SOCKET_READ_ERROR;
    return;
  }
  int64_t off = 0;
  EncryptionType encryptionType;
  std::string iv;
  if (!Protocol::decodeEncryptionSettings(buf_, off, Protocol::kMaxEncryption,
                                          encryptionType, iv)) {
    LOG(ERROR) << "Failed to decode encryption settings";
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if (encryptionType != encryptionParams_.getType()) {
    LOG(ERROR) << "Encryption type mismatch "
               << encryptionTypeToStr(encryptionType) << " "
               << encryptionTypeToStr(encryptionParams_.getType());
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if (!decryptor_.start(encryptionParams_, iv)) {
    readErrorCode_ = ENCRYPTION_ERROR;
    return;
  }
  LOG(INFO) << "Successfully read encryption settings " << port_ << " "
            << encryptionTypeToStr(encryptionType);
  encryptionSettingsRead_ = true;
}

void WdtSocket::writeEncryptionSettingsOnce() {
  if (!encryptionParams_.isSet() || encryptionSettingsWritten_) {
    return;
  }
  WDT_CHECK(!encryptionParams_.getSecret().empty());

  int timeoutMs = threadCtx_.getOptions().write_timeout_millis;
  std::string iv;
  if (!encryptor_.start(encryptionParams_, iv)) {
    writeErrorCode_ = ENCRYPTION_ERROR;
    return;
  }
  int64_t off = 0;
  buf_[off++] = Protocol::ENCRYPTION_CMD;
  Protocol::encodeEncryptionSettings(buf_, off, off + Protocol::kMaxEncryption,
                                     encryptionParams_.getType(), iv);
  int written = writeInternal(buf_, off, timeoutMs, false);
  if (written != off) {
    LOG(ERROR) << "Failed to write encryption settings " << written << " "
               << port_;
    return;
  }
  encryptionSettingsWritten_ = true;
}

int WdtSocket::readInternal(char *buf, int nbyte, int timeoutMs, bool tryFull) {
  int numRead = readWithAbortCheck(buf, nbyte, timeoutMs, tryFull);
  if (numRead == 0) {
    readErrorCode_ = SOCKET_READ_ERROR;
    return 0;
  }
  if (numRead < 0) {
    if (errno == EAGAIN || errno == EINTR) {
      readErrorCode_ = WDT_TIMEOUT;
    } else {
      readErrorCode_ = SOCKET_READ_ERROR;
    }
    return numRead;
  }
  // clear error code if successful
  readErrorCode_ = OK;
  return numRead;
}

int WdtSocket::writeInternal(const char *buf, int nbyte, int timeoutMs,
                             bool retry) {
  int count = 0;
  int written = 0;
  while (written < nbyte) {
    int w = writeWithAbortCheck(buf + written, nbyte - written, timeoutMs,
                                /* always try to write everything */ true);
    if (w <= 0) {
      writeErrorCode_ = SOCKET_WRITE_ERROR;
      return (written > 0 ? written : -1);
    }
    if (!retry) {
      return w;
    }
    written += w;
    count++;
  }
  WDT_CHECK_EQ(nbyte, written);
  LOG_IF(INFO, count > 1) << "Took " << count << " attempts to write " << nbyte
                          << " bytes to socket";
  return written;
}

int WdtSocket::readWithTimeout(char *buf, int nbyte, int timeoutMs,
                               bool tryFull) {
  WDT_CHECK_GT(nbyte, 0);
  if (readErrorCode_ != OK && readErrorCode_ != WDT_TIMEOUT) {
    LOG(ERROR) << "Socket read failed before, not trying to read again "
               << port_;
    return -1;
  }
  readErrorCode_ = OK;
  int numRead = 0;
  readEncryptionSettingsOnce(timeoutMs);
  if (supportUnencryptedPeer_ && readErrorCode_ == UNEXPECTED_CMD_ERROR) {
    LOG(WARNING)
        << "Turning off encryption since the other side does not support "
           "encryption " << port_;
    readErrorCode_ = OK;
    buf[0] = buf_[0];
    numRead = 1;
    // also turn off encryption
    encryptionParams_.erase();
  } else if (readErrorCode_ != OK) {
    return -1;
  }
  if (nbyte == numRead) {
    return nbyte;
  }
  bool encrypt = encryptionParams_.isSet();
  int ret = readInternal(buf + numRead, nbyte - numRead, timeoutMs, tryFull);
  if (ret >= 0) {
    numRead += ret;
  } else {
    return (numRead > 0 ? numRead : -1);
  }
  if (!encrypt) {
    return numRead;
  }
  int numDecrypted = 0;
  if (ctxSaveOffset_ >= 0) {
    if (ctxSaveOffset_ <= numRead) {
      if (!decryptor_.decrypt(buf, ctxSaveOffset_, buf)) {
        readErrorCode_ = ENCRYPTION_ERROR;
        return -1;
      }
      decryptor_.saveContext();
      numDecrypted = ctxSaveOffset_;
      ctxSaveOffset_ = CTX_SAVED;
    } else {
      ctxSaveOffset_ -= numRead;
    }
  }
  // have to decrypt data
  if (!decryptor_.decrypt((buf + numDecrypted), (numRead - numDecrypted),
                          (buf + numDecrypted))) {
    readErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  return numRead;
}

int WdtSocket::read(char *buf, int nbyte, bool tryFull) {
  return readWithTimeout(buf, nbyte,
                         threadCtx_.getOptions().read_timeout_millis, tryFull);
}

int WdtSocket::write(char *buf, int nbyte, bool retry) {
  WDT_CHECK_GT(nbyte, 0);
  if (writeErrorCode_ != OK) {
    LOG(ERROR) << "Socket write failed before, not trying to write again "
               << port_;
    return -1;
  }
  writeEncryptionSettingsOnce();
  if (writeErrorCode_ != OK) {
    return -1;
  }
  bool encrypt = encryptionParams_.isSet();
  if (encrypt && !encryptor_.encrypt(buf, nbyte, buf)) {
    writeErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  int written = writeInternal(
      buf, nbyte, threadCtx_.getOptions().write_timeout_millis, retry);
  if (written != nbyte) {
    LOG(ERROR) << "Socket write failure " << written << " " << nbyte;
    writeErrorCode_ = SOCKET_WRITE_ERROR;
  }
  return written;
}

int64_t WdtSocket::readWithAbortCheck(char *buf, int64_t nbyte, int timeoutMs,
                                      bool tryFull) {
  PerfStatCollector statCollector(threadCtx_, PerfStatReport::SOCKET_READ);
  return ioWithAbortCheck(::read, buf, nbyte, timeoutMs, tryFull);
}

int64_t WdtSocket::writeWithAbortCheck(const char *buf, int64_t nbyte,
                                       int timeoutMs, bool tryFull) {
  PerfStatCollector statCollector(threadCtx_, PerfStatReport::SOCKET_WRITE);
  return ioWithAbortCheck(::write, buf, nbyte, timeoutMs, tryFull);
}

template <typename F, typename T>
int64_t WdtSocket::ioWithAbortCheck(F readOrWrite, T tbuf, int64_t numBytes,
                                    int timeoutMs, bool tryFull) {
  WDT_CHECK(threadCtx_.getAbortChecker() != nullptr)
      << "abort checker can not be null";
  bool checkAbort = (threadCtx_.getOptions().abort_check_interval_millis > 0);
  auto startTime = Clock::now();
  int64_t doneBytes = 0;
  int retries = 0;
  while (doneBytes < numBytes) {
    const int64_t ret =
        readOrWrite(fd_, tbuf + doneBytes, numBytes - doneBytes);
    if (ret < 0) {
      // error
      if (errno != EINTR && errno != EAGAIN) {
        PLOG(ERROR) << "non-retryable error encountered during socket io "
                    << fd_ << " " << doneBytes << " " << retries;
        return (doneBytes > 0 ? doneBytes : ret);
      }
    } else if (ret == 0) {
      // eof
      VLOG(1) << "EOF received during socket io. fd : " << fd_
              << ", finished bytes : " << doneBytes
              << ", retries : " << retries;
      return doneBytes;
    } else {
      // success
      doneBytes += ret;
      if (!tryFull) {
        // do not have to read/write entire data
        return doneBytes;
      }
    }
    if (checkAbort && threadCtx_.getAbortChecker()->shouldAbort()) {
      LOG(ERROR) << "transfer aborted during socket io " << fd_ << " "
                 << doneBytes << " " << retries;
      return (doneBytes > 0 ? doneBytes : -1);
    }
    if (timeoutMs > 0) {
      int duration = durationMillis(Clock::now() - startTime);
      if (duration >= timeoutMs) {
        LOG(INFO) << "socket io timed out after " << duration << " ms, retries "
                  << retries << " fd " << fd_ << " doneBytes " << doneBytes;
        return (doneBytes > 0 ? doneBytes : -1);
      }
    }
    retries++;
  }
  VLOG_IF(1, retries > 1) << "socket io for " << doneBytes << " bytes took "
                          << retries << " retries";
  return doneBytes;
}

ErrorCode WdtSocket::shutdownWrites() {
  ErrorCode code = finalizeWrites(true);
  if (::shutdown(fd_, SHUT_WR) < 0) {
    if (code == OK) {
      PLOG(WARNING) << "Socket shutdown failed for fd " << fd_;
      code = ERROR;
    }
  }
  return code;
}

ErrorCode WdtSocket::expectEndOfStream() {
  return finalizeReads(true);
}

// TODO: Seems like we need to reset/clear encry/decr even on errors as we
// reuse/restart/recycle encryption objects and sockets (more correctness
// analysis of error cases needed)

ErrorCode WdtSocket::finalizeReads(bool doTagIOs) {
  VLOG(1) << "Finalizing reads/encryption " << port_ << " " << fd_;
  const int toRead = encryptionTypeToTagLen(encryptionParams_.getType());
  std::string tag;
  ErrorCode code = OK;
  if (toRead && doTagIOs) {
    tag.resize(toRead);
    int read = readInternal(&(tag.front()), tag.size(),
                            threadCtx_.getOptions().read_timeout_millis, true);
    if (read != toRead) {
      LOG(ERROR) << "Unable to read tag at end of stream got " << read
                 << " needed " << toRead << " " << folly::humanify(tag);
      tag.clear();
      code = ENCRYPTION_ERROR;
    }
  }
  if (!decryptor_.finish(tag)) {
    code = ENCRYPTION_ERROR;
  }
  ctxSaveOffset_ = OFFSET_NOT_SET;
  readsFinalized_ = true;
  return code;
}

ErrorCode WdtSocket::finalizeWrites(bool doTagIOs) {
  VLOG(1) << "Finalizing writes/encryption " << port_ << " " << fd_;
  ErrorCode code = OK;
  std::string tag;
  if (!encryptor_.finish(tag)) {
    code = ENCRYPTION_ERROR;
  }
  if (!tag.empty() && doTagIOs) {
    const int timeoutMs = threadCtx_.getOptions().write_timeout_millis;
    const int expected = tag.size();
    if (writeInternal(tag.data(), tag.size(), timeoutMs, false) != expected) {
      PLOG(ERROR) << "Encryption Tag write error";
      code = ENCRYPTION_ERROR;
    }
  }
  writesFinalized_ = true;
  return code;
}

ErrorCode WdtSocket::closeConnection() {
  return closeConnectionInternal(true);
}
void WdtSocket::closeNoCheck() {
  closeConnectionInternal(false);
}

ErrorCode WdtSocket::closeConnectionInternal(bool doTagIOs) {
  VLOG(1) << "Closing socket " << port_ << " " << fd_;
  if (fd_ < 0) {
    return OK;
  }
  ErrorCode errorCode = getNonRetryableErrCode();
  if (!writesFinalized_) {
    errorCode = getMoreInterestingError(errorCode, finalizeWrites(doTagIOs));
  }
  if (!readsFinalized_) {
    errorCode = getMoreInterestingError(errorCode, finalizeReads(doTagIOs));
  }
  if (::close(fd_) != 0) {
    PLOG(ERROR) << "Failed to close socket " << fd_ << " " << port_;
    errorCode = getMoreInterestingError(ERROR, errorCode);
  }
  // This looks like a reset() make it explicit (and check it's complete)
  fd_ = -1;
  readErrorCode_ = OK;
  writeErrorCode_ = OK;
  encryptionSettingsRead_ = false;
  encryptionSettingsWritten_ = false;
  writesFinalized_ = false;
  readsFinalized_ = false;
  VLOG(1) << "Error code from close " << errorCodeToStr(errorCode);
  return errorCode;
}

int WdtSocket::getFd() const {
  return fd_;
}

int WdtSocket::getPort() const {
  return port_;
}

EncryptionType WdtSocket::getEncryptionType() const {
  return encryptionParams_.getType();
}

ErrorCode WdtSocket::getReadErrCode() const {
  return readErrorCode_;
}

ErrorCode WdtSocket::getWriteErrCode() const {
  return writeErrorCode_;
}

ErrorCode WdtSocket::getNonRetryableErrCode() const {
  ErrorCode errCode = OK;
  if (readErrorCode_ != OK && readErrorCode_ != SOCKET_READ_ERROR &&
      readErrorCode_ != WDT_TIMEOUT && readErrorCode_ != ENCRYPTION_ERROR) {
    errCode = getMoreInterestingError(errCode, readErrorCode_);
  }
  if (writeErrorCode_ != OK && writeErrorCode_ != SOCKET_WRITE_ERROR &&
      writeErrorCode_ != ENCRYPTION_ERROR) {
    errCode = getMoreInterestingError(errCode, writeErrorCode_);
  }
  return errCode;
}

void WdtSocket::saveDecryptorCtx(const int offset) {
  WDT_CHECK_EQ(OFFSET_NOT_SET, ctxSaveOffset_);
  ctxSaveOffset_ = offset;
}

bool WdtSocket::verifyTag(std::string &tag) {
  WDT_CHECK_EQ(CTX_SAVED, ctxSaveOffset_);
  bool status = decryptor_.verifyTag(tag);
  ctxSaveOffset_ = OFFSET_NOT_SET;
  return status;
}

int WdtSocket::getEffectiveTimeout(int networkTimeout) {
  int abortInterval = threadCtx_.getOptions().abort_check_interval_millis;
  if (abortInterval <= 0) {
    return networkTimeout;
  }
  if (networkTimeout <= 0) {
    return abortInterval;
  }
  return std::min(networkTimeout, abortInterval);
}

void WdtSocket::setSocketTimeouts() {
  int readTimeout =
      getEffectiveTimeout(threadCtx_.getOptions().read_timeout_millis);
  if (readTimeout > 0) {
    struct timeval tv;
    tv.tv_sec = readTimeout / 1000;            // milli to sec
    tv.tv_usec = (readTimeout % 1000) * 1000;  // milli to micro
    if (setsockopt(fd_, SOL_SOCKET, SO_RCVTIMEO, (char *)&tv,
                   sizeof(struct timeval)) != 0) {
      PLOG(ERROR) << "Unable to set read timeout for " << port_ << " " << fd_;
    }
  }
  int writeTimeout =
      getEffectiveTimeout(threadCtx_.getOptions().write_timeout_millis);
  if (writeTimeout > 0) {
    struct timeval tv;
    tv.tv_sec = writeTimeout / 1000;            // milli to sec
    tv.tv_usec = (writeTimeout % 1000) * 1000;  // milli to micro
    if (setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, (char *)&tv,
                   sizeof(struct timeval)) != 0) {
      PLOG(ERROR) << "Unable to set write timeout for " << port_ << " " << fd_;
    }
  }
}

/* static */
bool WdtSocket::getNameInfo(const struct sockaddr *sa, socklen_t salen,
                            std::string &host, std::string &port) {
  char hostBuf[NI_MAXHOST], portBuf[NI_MAXSERV];
  int res = getnameinfo(sa, salen, hostBuf, sizeof(hostBuf), portBuf,
                        sizeof(portBuf), NI_NUMERICHOST | NI_NUMERICSERV);
  if (res) {
    LOG(ERROR) << "getnameinfo failed " << gai_strerror(res);
    return false;
  }
  host = std::string(hostBuf);
  port = std::string(portBuf);
  return true;
}

int WdtSocket::getReceiveBufferSize() const {
  int size;
  socklen_t sizeSize = sizeof(size);
  getsockopt(fd_, SOL_SOCKET, SO_RCVBUF, (void *)&size, &sizeSize);
  return size;
}

int WdtSocket::getSendBufferSize() const {
  int size;
  socklen_t sizeSize = sizeof(size);
  getsockopt(fd_, SOL_SOCKET, SO_SNDBUF, (void *)&size, &sizeSize);
  return size;
}

int WdtSocket::getUnackedBytes() const {
#ifdef WDT_HAS_SOCKIOS_H
  int numUnackedBytes;
  int ret;
  {
    PerfStatCollector statCollector(threadCtx_, PerfStatReport::IOCTL);
    ret = ::ioctl(fd_, SIOCOUTQ, &numUnackedBytes);
  }
  if (ret != 0) {
    PLOG(ERROR) << "Failed to get unacked bytes for socket " << fd_;
    numUnackedBytes = -1;
  }
  return numUnackedBytes;
#else
  LOG(WARNING) << "Wdt has no way to determine unacked bytes for socket";
  return -1;
#endif
}
WdtSocket::~WdtSocket() {
  VLOG(1) << "~WdtSocket " << port_ << " " << fd_;
  closeConnection();
}
}
}
