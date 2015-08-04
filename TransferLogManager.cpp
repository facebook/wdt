/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "TransferLogManager.h"

#include <wdt/WdtConfig.h>
#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "SerializationUtil.h"
#include "Reporting.h"

#include <folly/Range.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <map>
#include <ctime>
#include <iomanip>

namespace facebook {
namespace wdt {

void TransferLogManager::setRootDir(const std::string &rootDir) {
  rootDir_ = rootDir;
}

std::string TransferLogManager::getFullPath(const std::string &relPath) {
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  std::string fullPath = rootDir_;
  if (fullPath.back() != '/') {
    fullPath.push_back('/');
  }
  fullPath.append(relPath);
  return fullPath;
}

int TransferLogManager::open() {
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  auto openFlags = O_CREAT | O_WRONLY | O_APPEND;
  int fd = ::open(getFullPath(LOG_NAME).c_str(), openFlags, 0644);
  if (fd < 0) {
    PLOG(ERROR) << "Could not open wdt log";
  }
  return fd;
}

bool TransferLogManager::openAndStartWriter(const std::string &curSenderIp) {
  WDT_CHECK(fd_ == -1) << "Trying to open wdt log multiple times";

  const auto &options = WdtOptions::get();
  if (!options.disable_sender_verfication_during_resumption) {
    if (!senderIp_.empty() && senderIp_ != curSenderIp) {
      LOG(ERROR) << "Current sender ip does not match ip in the "
                    "transfer log "
                 << curSenderIp << " " << senderIp_
                 << ", ignoring transfer log";
      parsedFileChunksInfo_.clear();
      // remove the previous log
      unlink();
    }
  }
  senderIp_ = curSenderIp;

  fd_ = open();
  if (fd_ < 0) {
    return false;
  } else {
    writerThread_ =
        std::move(std::thread(&TransferLogManager::writeEntriesToDisk, this));
    LOG(INFO) << "Log writer thread started " << fd_;
    return true;
  }
}

void TransferLogManager::enableLogging() {
  loggingEnabled_ = true;
}

int64_t TransferLogManager::timestampInMicroseconds() const {
  auto timestamp = Clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             timestamp.time_since_epoch())
      .count();
}

std::string TransferLogManager::getFormattedTimestamp(int64_t timestampMicros) {
  // This assumes Clock's epoch is Posix's epoch (1970/1/1)
  // to_time_t is unfortunately only on the system_clock and not
  // on high_resolution_clock (on MacOS at least it isn't)
  time_t seconds = timestampMicros / kMicroToSec;
  int microseconds = timestampMicros - seconds * kMicroToSec;
  // need 25 bytes to encode date in format mm/dd/yy HH:MM:SS.MMMMMM
  char buf[25];
  struct tm tm;
  localtime_r(&seconds, &tm);
  snprintf(buf, sizeof(buf), "%02d/%02d/%02d %02d:%02d:%02d.%06d",
           tm.tm_mon + 1, tm.tm_mday, (tm.tm_year % 100), tm.tm_hour, tm.tm_min,
           tm.tm_sec, microseconds);
  return buf;
}

void TransferLogManager::addLogHeader() {
  if (!loggingEnabled_ || fd_ < 0) {
    return;
  }
  VLOG(1) << "Adding log header " << LOG_VERSION << " " << recoveryId_;
  char buf[kMaxEntryLength];
  // increment by 2 bytes to later store the total length
  char *ptr = buf + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = HEADER;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, LOG_VERSION);
  encodeString(ptr, size, recoveryId_);
  encodeString(ptr, size, senderIp_);

  folly::storeUnaligned<int16_t>(buf, size);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size + sizeof(int16_t));
}

void TransferLogManager::addFileCreationEntry(const std::string &fileName,
                                              int64_t seqId, int64_t fileSize) {
  if (!loggingEnabled_ || fd_ < 0) {
    return;
  }
  VLOG(1) << "Adding file entry to log " << fileName << " " << seqId << " "
          << fileSize;
  char buf[kMaxEntryLength];
  // increment by 2 bytes to later store the total length
  char *ptr = buf + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = FILE_CREATION;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeString(ptr, size, fileName);
  encodeInt(ptr, size, seqId);
  encodeInt(ptr, size, fileSize);

  folly::storeUnaligned<int16_t>(buf, size);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size + sizeof(int16_t));
}

void TransferLogManager::addBlockWriteEntry(int64_t seqId, int64_t offset,
                                            int64_t blockSize) {
  if (!loggingEnabled_ || fd_ < 0) {
    return;
  }
  VLOG(1) << "Adding block entry to log " << seqId << " " << offset << " "
          << blockSize;
  char buf[kMaxEntryLength];
  // increment by 2 bytes to later store the total length
  char *ptr = buf + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = BLOCK_WRITE;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, seqId);
  encodeInt(ptr, size, offset);
  encodeInt(ptr, size, blockSize);

  folly::storeUnaligned<int16_t>(buf, size);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size + sizeof(int16_t));
}

void TransferLogManager::addInvalidationEntry(int64_t seqId) {
  if (!loggingEnabled_ || fd_ < 0) {
    return;
  }
  VLOG(1) << "Adding invalidation entry " << seqId;
  char buf[kMaxEntryLength];
  int64_t size = 0;
  encodeInvalidationEntry(buf, size, seqId);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size + sizeof(int16_t));
}

bool TransferLogManager::close() {
  if (fd_ < 0) {
    return false;
  }
  if (::close(fd_) != 0) {
    PLOG(ERROR) << "Failed to close wdt log " << fd_;
    fd_ = -1;
    return false;
  }
  LOG(INFO) << "wdt log closed";
  fd_ = -1;
  return true;
}

bool TransferLogManager::unlink() {
  std::string fullLogName = getFullPath(LOG_NAME);
  if (::unlink(fullLogName.c_str()) != 0) {
    PLOG(ERROR) << "Could not unlink " << fullLogName;
    return false;
  }
  return true;
}

bool TransferLogManager::closeAndStopWriter() {
  if (fd_ < 0) {
    return false;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    finished_ = true;
    conditionFinished_.notify_all();
  }
  writerThread_.join();
  WDT_CHECK(entries_.empty());
  if (!close()) {
    return false;
  }
  return true;
}

void TransferLogManager::writeEntriesToDisk() {
  WDT_CHECK(fd_ >= 0) << "Writer thread started before the log is opened";
  auto &options = WdtOptions::get();
  WDT_CHECK(options.transfer_log_write_interval_ms >= 0);
  auto waitingTime =
      std::chrono::milliseconds(options.transfer_log_write_interval_ms);
  std::vector<std::string> entries;
  bool finished = false;
  while (!finished) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionFinished_.wait_for(lock, waitingTime);
      finished = finished_;
      // make a copy of all the entries so that we do not need to hold lock
      // during writing
      entries = entries_;
      entries_.clear();
    }
    std::string buffer;
    // write entries to disk
    for (const auto &entry : entries) {
      buffer.append(entry);
    }
    int toWrite = buffer.size();
    int written = ::write(fd_, buffer.c_str(), toWrite);
    if (written != toWrite) {
      PLOG(ERROR) << "Disk write error while writing transfer log " << written
                  << " " << toWrite;
      close();
      return;
    }
  }
}

bool TransferLogManager::parseLogHeader(char *buf, int16_t entrySize,
                                        int64_t &timestamp, int &version,
                                        std::string &recoveryId,
                                        std::string &senderIp) {
  folly::ByteRange br((uint8_t *)buf, entrySize);
  try {
    timestamp = decodeInt(br);
    version = decodeInt(br);
    if (!decodeString(br, buf, entrySize, recoveryId)) {
      return false;
    }
    if (!decodeString(br, buf, entrySize, senderIp)) {
      return false;
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, entrySize);
}

bool TransferLogManager::parseFileCreationEntry(char *buf, int16_t entrySize,
                                                int64_t &timestamp,
                                                std::string &fileName,
                                                int64_t &seqId,
                                                int64_t &fileSize) {
  folly::ByteRange br((uint8_t *)buf, entrySize);
  try {
    timestamp = decodeInt(br);
    if (!decodeString(br, buf, entrySize, fileName)) {
      return false;
    }
    seqId = decodeInt(br);
    fileSize = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, entrySize);
}

bool TransferLogManager::parseBlockWriteEntry(char *buf, int16_t entrySize,
                                              int64_t &timestamp,
                                              int64_t &seqId, int64_t &offset,
                                              int64_t &blockSize) {
  folly::ByteRange br((uint8_t *)buf, entrySize);
  try {
    timestamp = decodeInt(br);
    seqId = decodeInt(br);
    offset = decodeInt(br);
    blockSize = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, entrySize);
}

bool TransferLogManager::parseInvalidationEntry(char *buf, int16_t entrySize,
                                                int64_t &timestamp,
                                                int64_t &seqId) {
  folly::ByteRange br((uint8_t *)buf, entrySize);
  try {
    timestamp = decodeInt(br);
    seqId = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, entrySize);
}

void TransferLogManager::encodeInvalidationEntry(char *dest, int64_t &off,
                                                 int64_t seqId) {
  int64_t oldOffset = off;
  char *ptr = dest + off + sizeof(int16_t);
  ptr[off++] = ENTRY_INVALIDATION;
  encodeInt(ptr, off, timestampInMicroseconds());
  encodeInt(ptr, off, seqId);
  folly::storeUnaligned<int16_t>(dest, off - oldOffset);
}

bool TransferLogManager::writeInvalidationEntries(
    const std::set<int64_t> &seqIds) {
  int fd = open();
  if (fd < 0) {
    return false;
  }
  char buf[kMaxEntryLength];
  for (auto seqId : seqIds) {
    int64_t size = 0;
    encodeInvalidationEntry(buf, size, seqId);
    int toWrite = size + sizeof(int16_t);
    int written = ::write(fd, buf, toWrite);
    if (written != toWrite) {
      PLOG(ERROR) << "Disk write error while writing transfer log " << written
                  << " " << toWrite;
      ::close(fd);
      return false;
    }
  }
  if (::fsync(fd) != 0) {
    PLOG(ERROR) << "fsync() failed for fd " << fd;
    ::close(fd);
    return false;
  }
  if (::close(fd) != 0) {
    PLOG(ERROR) << "close() failed for fd " << fd;
  }
  return true;
}

bool TransferLogManager::truncateExtraBytesAtEnd(int fd, int extraBytes) {
  LOG(INFO) << "Removing extra " << extraBytes
            << " bytes from the end of transfer log";
  struct stat statBuffer;
  if (fstat(fd, &statBuffer) != 0) {
    PLOG(ERROR) << "fstat failed on fd " << fd;
    return false;
  }
  off_t fileSize = statBuffer.st_size;
  if (::ftruncate(fd, fileSize - extraBytes) != 0) {
    PLOG(ERROR) << "ftruncate failed for fd " << fd;
    return false;
  }
  return true;
}

bool TransferLogManager::parseAndPrint() {
  std::vector<FileChunksInfo> parsedInfo;
  return parseVerifyAndFix("", true, parsedInfo);
}

bool TransferLogManager::parseAndMatch(const std::string &recoveryId) {
  recoveryId_ = recoveryId;
  return parseVerifyAndFix(recoveryId_, false, parsedFileChunksInfo_);
}

bool TransferLogManager::parseVerifyAndFix(
    const std::string &recoveryId, bool parseOnly,
    std::vector<FileChunksInfo> &parsedInfo) {
  WDT_CHECK(parsedInfo.empty()) << "parsedInfo vector must be empty";
  std::string fullLogName = getFullPath(LOG_NAME);
  int logFd = ::open(fullLogName.c_str(), O_RDONLY);
  if (logFd < 0) {
    PLOG(ERROR) << "Unable to open transfer log " << fullLogName;
    return false;
  }
  auto errorGuard = folly::makeGuard([&] {
    if (logFd >= 0) {
      ::close(logFd);
    }
    if (!parseOnly) {
      if (::rename(getFullPath(LOG_NAME).c_str(),
                   getFullPath(BUGGY_LOG_NAME).c_str()) != 0) {
        PLOG(ERROR) << "log rename failed " << LOG_NAME << " "
                    << BUGGY_LOG_NAME;
      }
    }
  });
  std::map<int64_t, FileChunksInfo> fileInfoMap;
  std::map<int64_t, int64_t> seqIdToSizeMap;
  std::string fileName, logRecoveryId;
  int64_t timestamp, seqId, fileSize, offset, blockSize;
  int logVersion;
  std::set<int64_t> invalidSeqIds;
  char entry[kMaxEntryLength];

  while (true) {
    int16_t entrySize;
    int toRead = sizeof(entrySize);
    int numRead = ::read(logFd, &entrySize, toRead);
    if (numRead < 0) {
      PLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                  << toRead;
      return false;
    }
    if (numRead == 0) {
      break;
    }
    if (numRead != toRead) {
      // extra bytes at the end, most likely part of the previous write
      // succeeded partially
      if (parseOnly) {
        LOG(INFO) << "Extra " << numRead << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(logFd, numRead)) {
        return false;
      }
      break;
    }
    if (entrySize < 0 || entrySize > kMaxEntryLength) {
      LOG(ERROR) << "Transfer log parse error, invalid entry length "
                 << entrySize;
      return false;
    }
    numRead = ::read(logFd, entry, entrySize);
    if (numRead < 0) {
      PLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                  << entrySize;
      return false;
    }
    if (numRead == 0) {
      break;
    }
    if (numRead != entrySize) {
      if (parseOnly) {
        LOG(INFO) << "Extra " << numRead << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(logFd, numRead)) {
        return false;
      }
      break;
    }
    EntryType type = (EntryType)entry[0];
    switch (type) {
      case HEADER: {
        if (!parseLogHeader(entry + 1, entrySize - 1, timestamp, logVersion,
                            logRecoveryId, senderIp_)) {
          return false;
        }
        if (logVersion != LOG_VERSION) {
          LOG(ERROR) << "Can not parse log version " << logVersion
                     << ", parser version " << LOG_VERSION;
          return false;
        }
        if (!parseOnly && recoveryId != logRecoveryId) {
          LOG(ERROR)
              << "Current recovery-id does not match with log recovery-id "
              << recoveryId << " " << logRecoveryId;
          return false;
        }
        if (senderIp_.empty()) {
          LOG(ERROR) << "Log header has empty sender ip";
          return false;
        }
        if (parseOnly) {
          std::cout << getFormattedTimestamp(timestamp)
                    << " New transfer started, log-version " << logVersion
                    << " recovery-id " << logRecoveryId << " sender-ip "
                    << senderIp_ << std::endl;
        }
        break;
      }
      case FILE_CREATION: {
        if (!parseFileCreationEntry(entry + 1, entrySize - 1, timestamp,
                                    fileName, seqId, fileSize)) {
          return false;
        }
        if (fileInfoMap.find(seqId) != fileInfoMap.end() ||
            invalidSeqIds.find(seqId) != invalidSeqIds.end()) {
          LOG(ERROR) << "Multiple FILE_CREATION entry for same sequence-id "
                     << fileName << " " << seqId << " " << fileSize;
          return false;
        }
        if (parseOnly) {
          std::cout << getFormattedTimestamp(timestamp) << " File created "
                    << fileName << " seq-id " << seqId << " file-size "
                    << fileSize << std::endl;
          fileInfoMap.emplace(seqId, FileChunksInfo(seqId, fileName, fileSize));
          break;
        }
        // verify size
        bool sizeVerificationSuccess = false;
        struct stat buffer;
        if (stat(getFullPath(fileName).c_str(), &buffer) != 0) {
          PLOG(ERROR) << "stat failed for " << fileName;
        } else {
#ifdef HAS_POSIX_FALLOCATE
          sizeVerificationSuccess = (buffer.st_size == fileSize);
#else
          sizeVerificationSuccess = (buffer.st_size <= fileSize);
#endif
        }

        if (sizeVerificationSuccess) {
          fileInfoMap.emplace(seqId, FileChunksInfo(seqId, fileName, fileSize));
          seqIdToSizeMap.emplace(seqId, buffer.st_size);
        } else {
          LOG(INFO) << "Sanity check failed for " << fileName << " seq-id "
                    << seqId << " file-size " << fileSize;
          invalidSeqIds.insert(seqId);
        }
        break;
      }
      case BLOCK_WRITE: {
        if (!parseBlockWriteEntry(entry + 1, entrySize - 1, timestamp, seqId,
                                  offset, blockSize)) {
          return false;
        }
        if (invalidSeqIds.find(seqId) != invalidSeqIds.end()) {
          LOG(INFO) << "Block entry for an invalid sequence-id " << seqId
                    << ", ignoring";
          continue;
        }
        auto it = fileInfoMap.find(seqId);
        if (it == fileInfoMap.end()) {
          LOG(ERROR) << "Block entry for unknown sequence-id " << seqId << " "
                     << offset << " " << blockSize;
          return false;
        }
        FileChunksInfo &chunksInfo = it->second;
        if (parseOnly) {
          std::cout << getFormattedTimestamp(timestamp) << " Block written "
                    << chunksInfo.getFileName() << " seq-id " << seqId
                    << " offset " << offset << " block-size " << blockSize
                    << std::endl;
        } else {
          auto sizeIt = seqIdToSizeMap.find(seqId);
          WDT_CHECK(sizeIt != seqIdToSizeMap.end());
          if (offset + blockSize > sizeIt->second) {
            LOG(ERROR) << "Block end point is greater than file size in disk "
                       << chunksInfo.getFileName() << " seq-id " << seqId
                       << " offset " << offset << " block-size " << blockSize
                       << " file size in disk " << sizeIt->second;
            return false;
          }
        }
        chunksInfo.addChunk(Interval(offset, offset + blockSize));
        break;
      }
      case ENTRY_INVALIDATION: {
        if (!parseInvalidationEntry(entry + 1, entrySize - 1, timestamp,
                                    seqId)) {
          return false;
        }
        if (fileInfoMap.find(seqId) == fileInfoMap.end() &&
            invalidSeqIds.find(seqId) == invalidSeqIds.end()) {
          LOG(ERROR) << "Invalidation entry for an unknown sequence id "
                     << seqId;
          return false;
        }
        if (parseOnly) {
          std::cout << getFormattedTimestamp(timestamp)
                    << " Invalidation entry for seq-id " << seqId << std::endl;
        }
        fileInfoMap.erase(seqId);
        invalidSeqIds.erase(seqId);
        break;
      }
      default: {
        LOG(ERROR) << "Invalid entry type found " << type;
        return false;
      }
    }
  }
  if (parseOnly) {
    // no need to add invalidation entries in case of invocation from cmd line
    return true;
  }
  if (::close(logFd) != 0) {
    PLOG(ERROR) << "close() failed for fd " << logFd;
  }
  logFd = -1;
  if (!invalidSeqIds.empty()) {
    if (!writeInvalidationEntries(invalidSeqIds)) {
      return false;
    }
  }
  errorGuard.dismiss();
  for (auto &pair : fileInfoMap) {
    FileChunksInfo &fileInfo = pair.second;
    fileInfo.mergeChunks();
    parsedInfo.emplace_back(std::move(fileInfo));
  }
  return true;
}
}
}
