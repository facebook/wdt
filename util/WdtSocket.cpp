#include <wdt/util/WdtSocket.h>
#include <wdt/util/SocketUtils.h>
#include <wdt/Protocol.h>
#include <folly/Bits.h>

namespace facebook {
namespace wdt {

WdtSocket::WdtSocket(const int port, IAbortChecker const *abortChecker,
                     const EncryptionParams &encryptionParams)
    : port_(port),
      abortChecker_(abortChecker),
      encryptionParams_(encryptionParams),
      options_(WdtOptions::get()) {
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
  numRead = readInternal(buf_, toRead, options_.read_timeout_millis, true);
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

  int timeoutMs = WdtOptions::get().write_timeout_millis;
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
  int numRead = SocketUtils::readWithAbortCheck(fd_, buf, nbyte, abortChecker_,
                                                timeoutMs, tryFull);
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

int WdtSocket::writeInternal(char *buf, int nbyte, int timeoutMs, bool retry) {
  int count = 0;
  int written = 0;
  while (written < nbyte) {
    int w = SocketUtils::writeWithAbortCheck(
        fd_, buf + written, nbyte - written, abortChecker_, timeoutMs,
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
  // have to decrypt data
  if (!decryptor_.decrypt((uint8_t *)buf, numRead, (uint8_t *)buf)) {
    readErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  return numRead;
}

int WdtSocket::read(char *buf, int nbyte, bool tryFull) {
  return readWithTimeout(buf, nbyte, options_.read_timeout_millis, tryFull);
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
  if (encrypt && !encryptor_.encrypt((uint8_t *)buf, nbyte, (uint8_t *)buf)) {
    writeErrorCode_ = ENCRYPTION_ERROR;
    return -1;
  }
  int written = writeInternal(buf, nbyte, options_.write_timeout_millis, retry);
  if (written != nbyte) {
    LOG(ERROR) << "Socket write failure " << written << " " << nbyte;
    writeErrorCode_ = SOCKET_WRITE_ERROR;
  }
  return written;
}

void WdtSocket::closeConnection() {
  if (fd_ < 0) {
    return;
  }
  VLOG(1) << "Closing socket " << port_ << " " << fd_;
  if (::close(fd_) != 0) {
    LOG(ERROR) << "Failed to close socket " << fd_ << " " << port_;
  }
  fd_ = -1;
  readErrorCode_ = OK;
  writeErrorCode_ = OK;
  encryptionSettingsRead_ = false;
  encryptionSettingsWritten_ = false;
  encryptor_.finish();
  decryptor_.finish();
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
      readErrorCode_ != WDT_TIMEOUT) {
    errCode = getMoreInterestingError(errCode, readErrorCode_);
  }
  if (writeErrorCode_ != OK && writeErrorCode_ != SOCKET_WRITE_ERROR) {
    errCode = getMoreInterestingError(errCode, writeErrorCode_);
  }
  return errCode;
}

WdtSocket::~WdtSocket() {
  closeConnection();
}
}
}
