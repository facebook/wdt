#include <wdt/util/WdtSocket.h>
#include <folly/lang/Bits.h>
#include <folly/String.h>  // for humanify
#include <netdb.h>
#include <sys/ioctl.h>
#include <unistd.h>
#include <wdt/Protocol.h>
#ifdef WDT_HAS_SOCKIOS_H
#include <linux/sockios.h>
#endif

namespace facebook {
namespace wdt {

WdtSocket::WdtSocket(ThreadCtx &threadCtx, const int port,
                     const EncryptionParams &encryptionParams,
                     const int64_t ivChangeInterval,
                     Func &&tagVerificationSuccessCallback)
    : port_(port),
      threadCtx_(threadCtx),
      encryptionParams_(encryptionParams),
      ivChangeInterval_(ivChangeInterval),
      tagVerificationSuccessCallback_(
          std::move(tagVerificationSuccessCallback)) {
  if (encryptionTypeToTagLen(encryptionParams_.getType())) {
    // encryption has tag verification support
    writeTagInterval_ = threadCtx_.getOptions().encryption_tag_interval_bytes;
  }
  resetEncryptor();
  resetDecryptor();
}

// TODO: consider refactoring this to return error code
void WdtSocket::readEncryptionSettingsOnce(int timeoutMs) {
  if (!encryptionParams_.isSet() || encryptionSettingsRead_) {
    return;
  }
  WDT_CHECK(!encryptionParams_.getSecret().empty());

  int numRead = readInternal(buf_, 1, timeoutMs, true);
  if (numRead != 1) {
    WLOG(ERROR) << "Failed to read encryption settings " << numRead << " "
                << port_;
    return;
  }
  if (buf_[0] != Protocol::ENCRYPTION_CMD) {
    WLOG(ERROR) << "Expected to read ENCRYPTION_CMD(e), but got " << buf_[0];
    readErrorCode_ = UNEXPECTED_CMD_ERROR;
    return;
  }
  int toRead = Protocol::kEncryptionCmdLen - 1;  // already read 1 byte for cmd
  numRead = readInternal(buf_, toRead,
                         threadCtx_.getOptions().read_timeout_millis, true);
  if (numRead != toRead) {
    WLOG(ERROR) << "Failed to read encryption settings " << numRead << " "
                << toRead << " " << port_;
    readErrorCode_ = SOCKET_READ_ERROR;
    return;
  }
  int64_t off = 0;
  EncryptionType encryptionType;
  std::string iv;
  if (!Protocol::decodeEncryptionSettings(
          buf_, off, Protocol::kEncryptionCmdLen, encryptionType, iv,
          readTagInterval_)) {
    WLOG(ERROR) << "Failed to decode encryption settings";
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if (encryptionType != encryptionParams_.getType()) {
    WLOG(ERROR) << "Encryption type mismatch "
                << encryptionTypeToStr(encryptionType) << " "
                << encryptionTypeToStr(encryptionParams_.getType());
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if (readTagInterval_ < 0) {
    WLOG(ERROR) << "Encryption tag verification interval can't be negative "
                << readTagInterval_;
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if ((readTagInterval_ > 0) && (encryptionTypeToTagLen(encryptionType) == 0)) {
    WLOG(ERROR) << "Tag verification should not be enabled for "
                << encryptionTypeToStr(encryptionType) << " "
                << readTagInterval_;
    readErrorCode_ = PROTOCOL_ERROR;
    return;
  }
  if (!decryptor_->start(encryptionParams_, iv)) {
    readErrorCode_ = ENCRYPTION_ERROR;
    return;
  }
  WLOG(INFO) << "Successfully read encryption settings " << port_ << " "
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
  if (!encryptor_->start(encryptionParams_, iv)) {
    writeErrorCode_ = ENCRYPTION_ERROR;
    return;
  }
  int64_t off = 0;
  buf_[off++] = Protocol::ENCRYPTION_CMD;
  Protocol::encodeEncryptionSettings(
      buf_, off, off + Protocol::kEncryptionCmdLen, encryptionParams_.getType(),
      iv, writeTagInterval_);
  int written = writeInternal(buf_, off, timeoutMs, false);
  if (written != off) {
    WLOG(ERROR) << "Failed to write encryption settings " << written << " "
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
      break;
    }
    written += w;
    count++;
    if (!retry) {
      break;
    }
  }
  if (written != nbyte) {
    WLOG(ERROR) << "Socket write failure " << written << " " << nbyte;
    writeErrorCode_ = SOCKET_WRITE_ERROR;
    return -1;
  }
  WLOG_IF(INFO, count > 1) << "Took " << count << " attempts to write " << nbyte
                           << " bytes to socket";
  return written;
}

bool WdtSocket::checkAndChangeDecryptionIv(const std::string &tag) {
  if (ivChangeInterval_ == 0) {
    return true;
  }
  if (decryptor_->getNumProcessed() + readTagInterval_ <= ivChangeInterval_) {
    // can wait till next tag read
    return true;
  }
  WLOG(INFO) << "Need to change decryption iv " << port_;
  // read the new iv
  std::string iv;
  iv.resize(kAESBlockSize);
  const int numRead =
      readInternal(&(iv.front()), kAESBlockSize,
                   threadCtx_.getOptions().read_timeout_millis, true);
  if (numRead != kAESBlockSize) {
    WLOG(ERROR) << "Unable to read encryption iv " << numRead << " "
                << kAESBlockSize;
    return false;
  }
  if (!decryptor_->finish(tag)) {
    WLOG(ERROR) << "Failed to verify encryption tag";
    return false;
  }
  resetDecryptor();
  if (!decryptor_->start(encryptionParams_, iv)) {
    return false;
  }
  return true;
}

std::string WdtSocket::readEncryptionTag() {
  std::string tag;
  const int toRead = encryptionTypeToTagLen(encryptionParams_.getType());
  tag.resize(toRead);
  int read = readInternal(&(tag.front()), tag.size(),
                          threadCtx_.getOptions().read_timeout_millis, true);
  if (read != toRead) {
    WLOG(ERROR) << "Unable to read tag, got " << read << " needed " << toRead
                << " " << folly::humanify(tag);
    tag.clear();
    // It is important to mark this as encryption error. Because, we treat
    // WDT_TIMEOUT as retryable error. But, a failure to read the tag makes this
    // socket unusable.
    // If we want to make this failure retryable, then, we have to cache the
    // part of the tag read which is complicated.
    readErrorCode_ = ENCRYPTION_ERROR;
  }
  WVLOG(1) << "Encryption tag read " << folly::humanify(tag) << " totalRead_ "
           << totalRead_ << " tag interval " << readTagInterval_;
  return tag;
}

int WdtSocket::computeNextTagOffset(int64_t totalProcessed,
                                    int64_t tagInterval) {
  if (totalProcessed == 0) {
    return tagInterval;
  }
  int nextTagOffset = (tagInterval - (totalProcessed % tagInterval));
  if (nextTagOffset == tagInterval) {
    nextTagOffset = 0;
  }
  return nextTagOffset;
}

int WdtSocket::readAndDecrypt(char *buf, int nbyte, int timeoutMs,
                              bool tryFull) {
  WDT_CHECK_GT(nbyte, 0);
  const bool encrypt = encryptionParams_.isSet();
  WDT_CHECK(encrypt);
  // tag is transferred in plain text
  int numRead = readInternal(buf, nbyte, timeoutMs, tryFull);
  if (numRead <= 0) {
    return numRead;
  }
  // have to decrypt data
  if (!decryptor_->decrypt(buf, numRead, buf)) {
    readErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  return numRead;
}

int WdtSocket::readAndDecryptWithTag(char *buf, int nbyte, int timeoutMs,
                                     bool tryFull) {
  WDT_CHECK_GT(readTagInterval_, 0);
  WDT_CHECK_LE(nbyte, readTagInterval_);
  // first try to figure out whether this read will contain a tag
  const int nextTagOffset = computeNextTagOffset(totalRead_, readTagInterval_);

  int numRead = 0;
  if (nextTagOffset < nbyte) {
    // tag is contained in this read
    if (nextTagOffset > 0) {
      // try to read till the tag
      const int ret = readAndDecrypt(buf, nextTagOffset, timeoutMs, tryFull);
      if (ret <= 0) {
        // read error
        return ret;
      }
      totalRead_ += ret;
      if (ret < nextTagOffset) {
        // couldn't read till the tag. Tag will get read during next read
        return ret;
      }
      WDT_CHECK_EQ(nextTagOffset, ret);
      numRead = ret;
    }
    WDT_CHECK_EQ(0, totalRead_ % readTagInterval_);
    // now read and verify tag
    const std::string tag = readEncryptionTag();
    if (tag.empty()) {
      // readEncryptionTag already logs error
      return -1;
    }
    if (!decryptor_->verifyTag(tag)) {
      // verifyTag logs
      readErrorCode_ = ENCRYPTION_ERROR;
      return -1;
    }
    // tag verification successful, inform higher layer
    if (tagVerificationSuccessCallback_ != nullptr) {
      tagVerificationSuccessCallback_();
    }
    if (!checkAndChangeDecryptionIv(tag)) {
      readErrorCode_ = ENCRYPTION_ERROR;
      return -1;
    }
  }
  // now try to read rest of the data
  const int ret =
      readAndDecrypt(buf + numRead, nbyte - numRead, timeoutMs, tryFull);
  if (ret <= 0) {
    // read error
    return (numRead > 0 ? numRead : ret);
  }
  totalRead_ += ret;
  return numRead + ret;
}

int WdtSocket::readWithTimeout(char *buf, int nbyte, int timeoutMs,
                               bool tryFull) {
  WDT_CHECK_GT(nbyte, 0);
  if (readErrorCode_ != OK && readErrorCode_ != WDT_TIMEOUT) {
    WLOG(ERROR) << "Socket read failed before, not trying to read again "
                << port_;
    return -1;
  }
  readErrorCode_ = OK;
  int numRead = 0;

  // first try to find encryption settings
  readEncryptionSettingsOnce(timeoutMs);
  if (supportUnencryptedPeer_ && readErrorCode_ == UNEXPECTED_CMD_ERROR) {
    WLOG(WARNING)
        << "Turning off encryption since the other side does not support "
           "encryption "
        << port_;
    readErrorCode_ = OK;
    buf[0] = buf_[0];
    numRead = 1;
    // also turn off encryption
    encryptionParams_.erase();
  } else if (readErrorCode_ != OK) {
    return -1;
  }

  const bool encrypt = encryptionParams_.isSet();
  if (!encrypt) {
    // handle the non-encryption case
    int ret = readInternal(buf + numRead, nbyte - numRead, timeoutMs, tryFull);
    if (ret >= 0) {
      return numRead + ret;
    }
    // read failure
    return (numRead > 0 ? numRead : -1);
  }

  // handle encryption case
  WDT_CHECK_EQ(0, numRead);
  if (readTagInterval_ <= 0) {
    return readAndDecrypt(buf, nbyte, timeoutMs, tryFull);
  }
  // tag verification enabled
  // have to break the read in chunks of readTagInterval_
  while (numRead < nbyte) {
    const int remaining = nbyte - numRead;
    const int toRead = std::min(remaining, readTagInterval_);
    const int ret =
        readAndDecryptWithTag(buf + numRead, toRead, timeoutMs, tryFull);
    if (ret <= 0) {
      // read error
      return (numRead > 0 ? numRead : ret);
    }
    numRead += ret;
    if (ret < toRead) {
      // couldn't read full data
      return numRead;
    }
  }
  return numRead;
}

int WdtSocket::read(char *buf, int nbyte, bool tryFull) {
  return readWithTimeout(buf, nbyte,
                         threadCtx_.getOptions().read_timeout_millis, tryFull);
}

int WdtSocket::encryptAndWrite(char *buf, int nbyte, int timeoutMs,
                               bool retry) {
  WDT_CHECK_GT(nbyte, 0);
  const bool encrypt = encryptionParams_.isSet();
  WDT_CHECK(encrypt);

  if (!encryptor_->encrypt(buf, nbyte, buf)) {
    writeErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  int written = writeInternal(buf, nbyte, timeoutMs, retry);
  if (written != nbyte) {
    WLOG(ERROR) << "Socket write failure " << written << " " << nbyte;
    writeErrorCode_ = SOCKET_WRITE_ERROR;
  }
  return written;
}

bool WdtSocket::checkAndChangeEncryptionIv() {
  if (ivChangeInterval_ == 0) {
    return true;
  }
  if (encryptor_->getNumProcessed() + writeTagInterval_ <= ivChangeInterval_) {
    // can wait till next tag write
    return true;
  }
  WLOG(INFO) << "Need to change encryption iv " << port_;
  resetEncryptor();
  std::string iv;
  if (!encryptor_->start(encryptionParams_, iv)) {
    return false;
  }
  WDT_CHECK_EQ(kAESBlockSize, iv.size());
  const int written =
      writeInternal(iv.data(), kAESBlockSize,
                    threadCtx_.getOptions().write_timeout_millis, false);
  if (written != kAESBlockSize) {
    WLOG(ERROR) << "Unable to write new encryption iv " << written << " "
                << kAESBlockSize;
    return false;
  }
  return true;
}

bool WdtSocket::writeEncryptionTag() {
  const std::string tag = encryptor_->computeCurrentTag();
  if (tag.empty()) {
    // computeCurrentTag logs
    writeErrorCode_ = ENCRYPTION_ERROR;
    return false;
  }
  const int toWrite = tag.size();
  const int written = writeInternal(
      tag.data(), toWrite, threadCtx_.getOptions().write_timeout_millis, false);
  if (written != toWrite) {
    WLOG(ERROR) << "Unable to write encryption tag " << written << " "
                << toWrite;
    return false;
  }
  WVLOG(1) << "Encryption tag written " << folly::humanify(tag)
           << " totalWritten_ " << totalWritten_ << " tag interval "
           << writeTagInterval_;
  return true;
}

int WdtSocket::encryptAndWriteWithTag(char *buf, int nbyte, int timeoutMs,
                                      bool retry) {
  WDT_CHECK_GT(writeTagInterval_, 0);
  WDT_CHECK_LE(nbyte, writeTagInterval_);
  // first try to figure out whether this write will contain a tag
  const int nextTagOffset =
      computeNextTagOffset(totalWritten_, writeTagInterval_);

  int written = 0;
  if (nextTagOffset < nbyte) {
    // tag is contained in this write
    if (nextTagOffset > 0) {
      // try to write till the tag
      const int ret = encryptAndWrite(buf, nextTagOffset, timeoutMs, retry);
      if (ret != nextTagOffset) {
        return -1;
      }
      totalWritten_ += ret;
      written = ret;
    }
    WDT_CHECK_EQ(0, totalWritten_ % writeTagInterval_);
    // now write the tag
    if (!writeEncryptionTag()) {
      return -1;
    }
    if (!checkAndChangeEncryptionIv()) {
      writeErrorCode_ = ENCRYPTION_ERROR;
      return -1;
    }
  }
  // now try to write rest of the data
  const int remainingWrite = nbyte - written;
  const int ret =
      encryptAndWrite(buf + written, remainingWrite, timeoutMs, retry);
  if (ret != remainingWrite) {
    return -1;
  }
  totalWritten_ += ret;
  return nbyte;
}

int WdtSocket::write(char *buf, int nbyte, bool retry) {
  WDT_CHECK_GT(nbyte, 0);
  if (writeErrorCode_ != OK) {
    WLOG(ERROR) << "Socket write failed before, not trying to write again "
                << port_;
    return -1;
  }

  // first write encryption settings once
  writeEncryptionSettingsOnce();
  if (writeErrorCode_ != OK) {
    return -1;
  }

  const int timeoutMs = threadCtx_.getOptions().write_timeout_millis;
  const bool encrypt = encryptionParams_.isSet();
  // handle no-encryption case
  if (!encrypt) {
    return writeInternal(buf, nbyte, timeoutMs, retry);
  }
  if (writeTagInterval_ <= 0) {
    return encryptAndWrite(buf, nbyte, timeoutMs, retry);
  }
  // tag verification enabled
  // have to break the write in chunks of writeTagInterval_
  int written = 0;
  while (written < nbyte) {
    const int remaining = nbyte - written;
    const int toWrite = std::min(remaining, writeTagInterval_);
    const int ret =
        encryptAndWriteWithTag(buf + written, toWrite, timeoutMs, retry);
    if (ret != toWrite) {
      // write failure
      return -1;
    }
    written += ret;
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
        WPLOG(ERROR) << "non-retryable error encountered during socket io "
                     << fd_ << " " << doneBytes << " " << retries;
        return (doneBytes > 0 ? doneBytes : ret);
      }
    } else if (ret == 0) {
      // eof
      WVLOG(1) << "EOF received during socket io. fd : " << fd_
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
      WLOG(ERROR) << "transfer aborted during socket io " << fd_ << " "
                  << doneBytes << " " << retries;
      return (doneBytes > 0 ? doneBytes : -1);
    }
    if (timeoutMs > 0) {
      int duration = durationMillis(Clock::now() - startTime);
      if (duration >= timeoutMs) {
        WLOG(INFO) << "socket io timed out after " << duration
                   << " ms, retries " << retries << " fd " << fd_
                   << " doneBytes " << doneBytes;
        return (doneBytes > 0 ? doneBytes : -1);
      }
    }
    retries++;
  }
  WVLOG_IF(1, retries > 1) << "socket io for " << doneBytes << " bytes took "
                           << retries << " retries";
  return doneBytes;
}

ErrorCode WdtSocket::shutdownWrites() {
  ErrorCode code = finalizeWrites(true);
  if (::shutdown(fd_, SHUT_WR) < 0) {
    if (code == OK) {
      WPLOG(WARNING) << "Socket shutdown failed for fd " << fd_;
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
  WVLOG(1) << "Finalizing reads/encryption " << port_ << " " << fd_;
  const int toRead = encryptionTypeToTagLen(encryptionParams_.getType());
  std::string tag;
  ErrorCode code = OK;
  if (toRead && doTagIOs) {
    tag = readEncryptionTag();
    if (tag.empty()) {
      WLOG(ERROR) << "Unable to read tag at the end of stream " << port_;
      code = ENCRYPTION_ERROR;
    }
  }
  if (!decryptor_->finish(tag)) {
    code = ENCRYPTION_ERROR;
  }
  readsFinalized_ = true;
  return code;
}

ErrorCode WdtSocket::finalizeWrites(bool doTagIOs) {
  WVLOG(1) << "Finalizing writes/encryption " << port_ << " " << fd_;
  ErrorCode code = OK;
  std::string tag;
  if (!encryptor_->finish(tag)) {
    code = ENCRYPTION_ERROR;
  }
  if (!tag.empty() && doTagIOs) {
    const int timeoutMs = threadCtx_.getOptions().write_timeout_millis;
    const int expected = tag.size();
    if (writeInternal(tag.data(), tag.size(), timeoutMs, false) != expected) {
      WPLOG(ERROR) << "Encryption Tag write error";
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
  WVLOG(1) << "Closing socket " << port_ << " " << fd_;
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
    WPLOG(ERROR) << "Failed to close socket " << fd_ << " " << port_;
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
  totalRead_ = 0;
  totalWritten_ = 0;
  resetEncryptor();
  resetDecryptor();
  WVLOG(1) << "Error code from close " << errorCodeToStr(errorCode);
  return errorCode;
}

int WdtSocket::getFd() const {
  return fd_;
}

void WdtSocket::setFd(int fd) {
  fd_ = fd;
}

int WdtSocket::getPort() const {
  return port_;
}

void WdtSocket::setPort(int port) {
  port_ = port;
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
      WPLOG(ERROR) << "Unable to set read timeout for " << port_ << " " << fd_;
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
      WPLOG(ERROR) << "Unable to set write timeout for " << port_ << " " << fd_;
    }
  }
}

void WdtSocket::setDscp(int dscp) {
  if (dscp > 0) {
    if (threadCtx_.getOptions().ipv6) {
      int classval = dscp << 2;
      if(setsockopt(fd_, IPPROTO_IPV6, IPV6_TCLASS, (char*)&classval,
          sizeof(classval)) != 0) {
        WPLOG(ERROR) << "Unable to set DSCP flag for " << port_ << " " << fd_;
      }
    }
    if (threadCtx_.getOptions().ipv4) {
      int ip_tos = dscp << 2;
      if(setsockopt(fd_, IPPROTO_IP, IP_TOS, (char*)&ip_tos,
          sizeof(ip_tos)) != 0) {
        WPLOG(ERROR) << "Unable to set DSCP flag for " << port_ << " " << fd_;
      }
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
    WLOG(ERROR) << "getnameinfo failed " << gai_strerror(res);
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

void WdtSocket::resetEncryptor() {
  encryptor_ = std::make_unique<AESEncryptor>();
}

void WdtSocket::resetDecryptor() {
  decryptor_ = std::make_unique<AESDecryptor>();
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
    WPLOG(ERROR) << "Failed to get unacked bytes for socket " << fd_;
    numUnackedBytes = -1;
  }
  return numUnackedBytes;
#else
  WLOG(WARNING) << "Wdt has no way to determine unacked bytes for socket";
  return -1;
#endif
}
WdtSocket::~WdtSocket() {
  WVLOG(1) << "~WdtSocket " << port_ << " " << fd_;
  closeNoCheck();
}
}
}
