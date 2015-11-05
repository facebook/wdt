/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/TransferLogManager.h>

#include <wdt/util/SerializationUtil.h>

#include <folly/Range.h>
#include <wdt/Reporting.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <folly/String.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <ctime>
#include <iomanip>

namespace facebook {
namespace wdt {

const int TransferLogManager::LOG_VERSION = 2;

int64_t LogEncoderDecoder::timestampInMicroseconds() const {
  auto timestamp = Clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             timestamp.time_since_epoch()).count();
}

int64_t LogEncoderDecoder::encodeLogHeader(char *dest,
                                           const std::string &recoveryId,
                                           const std::string &senderIp,
                                           int64_t config) {
  // increment by 2 bytes to later store the total length
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::HEADER;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, TransferLogManager::LOG_VERSION);
  encodeString(ptr, size, recoveryId);
  encodeString(ptr, size, senderIp);
  encodeInt(ptr, size, config);

  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeLogHeader(char *buf, int16_t size,
                                        int64_t &timestamp, int &version,
                                        std::string &recoveryId,
                                        std::string &senderIp,
                                        int64_t &config) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
    version = decodeInt(br);
    if (!decodeString(br, buf, size, recoveryId)) {
      return false;
    }
    if (!decodeString(br, buf, size, senderIp)) {
      return false;
    }
    config = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

int64_t LogEncoderDecoder::encodeFileCreationEntry(char *dest,
                                                   const std::string &fileName,
                                                   const int64_t seqId,
                                                   const int64_t fileSize) {
  // increment by 2 bytes to later store the total length
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::FILE_CREATION;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeString(ptr, size, fileName);
  encodeInt(ptr, size, seqId);
  encodeInt(ptr, size, fileSize);

  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeFileCreationEntry(char *buf, int16_t size,
                                                int64_t &timestamp,
                                                std::string &fileName,
                                                int64_t &seqId,
                                                int64_t &fileSize) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
    if (!decodeString(br, buf, size, fileName)) {
      return false;
    }
    seqId = decodeInt(br);
    fileSize = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

int64_t LogEncoderDecoder::encodeBlockWriteEntry(char *dest,
                                                 const int64_t seqId,
                                                 const int64_t offset,
                                                 const int64_t blockSize) {
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::BLOCK_WRITE;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, seqId);
  encodeInt(ptr, size, offset);
  encodeInt(ptr, size, blockSize);

  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeBlockWriteEntry(char *buf, int16_t size,
                                              int64_t &timestamp,
                                              int64_t &seqId, int64_t &offset,
                                              int64_t &blockSize) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
    seqId = decodeInt(br);
    offset = decodeInt(br);
    blockSize = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

int64_t LogEncoderDecoder::encodeFileResizeEntry(char *dest,
                                                 const int64_t seqId,
                                                 const int64_t fileSize) {
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::FILE_RESIZE;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, seqId);
  encodeInt(ptr, size, fileSize);

  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeFileResizeEntry(char *buf, int16_t size,
                                              int64_t &timestamp,
                                              int64_t &seqId,
                                              int64_t &fileSize) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
    seqId = decodeInt(br);
    fileSize = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

int64_t LogEncoderDecoder::encodeFileInvalidationEntry(char *dest,
                                                       const int64_t &seqId) {
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::FILE_INVALIDATION;
  encodeInt(ptr, size, timestampInMicroseconds());
  encodeInt(ptr, size, seqId);
  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeFileInvalidationEntry(char *buf, int16_t size,
                                                    int64_t &timestamp,
                                                    int64_t &seqId) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
    seqId = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

int64_t LogEncoderDecoder::encodeDirectoryInvalidationEntry(char *dest) {
  char *ptr = dest + sizeof(int16_t);
  int64_t size = 0;
  ptr[size++] = TransferLogManager::DIRECTORY_INVALIDATION;
  encodeInt(ptr, size, timestampInMicroseconds());
  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeDirectoryInvalidationEntry(char *buf,
                                                         int16_t size,
                                                         int64_t &timestamp) {
  folly::ByteRange br((uint8_t *)buf, size);
  try {
    timestamp = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return !checkForOverflow(br.start() - (uint8_t *)buf, size);
}

void TransferLogManager::setRootDir(const std::string &rootDir) {
  rootDir_ = rootDir;
  if (rootDir_.back() != '/') {
    rootDir_.push_back('/');
  }
}

std::string TransferLogManager::getFullPath(const std::string &relPath) {
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  std::string fullPath = rootDir_;
  fullPath.append(relPath);
  return fullPath;
}

ErrorCode TransferLogManager::openLog() {
  WDT_CHECK(fd_ < 0);
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  const auto &options = WdtOptions::get();
  WDT_CHECK(options.enable_download_resumption);

  int openFlags = O_CREAT | O_RDWR;
  fd_ = ::open(getFullPath(LOG_NAME).c_str(), openFlags, 0644);
  if (fd_ < 0) {
    PLOG(ERROR) << "Could not open wdt log";
    return BYTE_SOURCE_READ_ERROR;
  }
  // try to acquire file lock
  if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
    PLOG(ERROR) << "Failed to acquire transfer log lock " << rootDir_ << " "
                << fd_;
    close();
    return TRANSFER_LOG_ACQUIRE_ERROR;
  }
  LOG(INFO) << "Transfer log opened";
  if (!options.resume_using_dir_tree) {
    // start writer thread
    writerThread_ = std::thread(&TransferLogManager::writeEntriesToDisk, this);
    LOG(INFO) << "Log writer thread started";
  }
  return OK;
}

void TransferLogManager::close() {
  if (fd_ < 0) {
    return;
  }
  if (::close(fd_) != 0) {
    PLOG(ERROR) << "Failed to close wdt log " << fd_;
  } else {
    LOG(INFO) << "Transfer log closed";
  }
  fd_ = -1;
}

void TransferLogManager::closeLog() {
  const auto &options = WdtOptions::get();
  if (fd_ < 0) {
    return;
  }
  if (!options.resume_using_dir_tree) {
    // stop writer thread
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
    }
    conditionFinished_.notify_one();
    writerThread_.join();
  }
  close();
}

TransferLogManager::~TransferLogManager() {
  WDT_CHECK_LT(fd_, 0) << "Destructor called, but transfer log not called";
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
    if (buffer.empty()) {
      // do not write when there is nothing to write
      continue;
    }
    int toWrite = buffer.size();
    int written = ::write(fd_, buffer.c_str(), toWrite);
    if (written != toWrite) {
      PLOG(ERROR) << "Disk write error while writing transfer log " << written
                  << " " << toWrite;
      return;
    }
  }
}

bool TransferLogManager::verifySenderIp(const std::string &curSenderIp) {
  if (fd_ < 0) {
    return false;
  }
  const auto &options = WdtOptions::get();
  bool verifySuccessful = true;
  if (!options.disable_sender_verification_during_resumption) {
    if (senderIp_.empty()) {
      LOG(INFO) << "Sender-ip empty, not verifying sender-ip, new-ip: "
                << curSenderIp;
    } else if (senderIp_ != curSenderIp) {
      LOG(ERROR) << "Current sender ip does not match ip in the "
                    "transfer log " << curSenderIp << " " << senderIp_
                 << ", ignoring transfer log";
      verifySuccessful = false;
      invalidateDirectory();
    }
  } else {
    LOG(WARNING) << "Sender-ip verification disabled " << senderIp_ << " "
                 << curSenderIp;
  }
  senderIp_ = curSenderIp;
  return verifySuccessful;
}

void TransferLogManager::fsync() {
  WDT_CHECK(fd_ >= 0);
  if (::fsync(fd_) != 0) {
    PLOG(ERROR) << "fsync failed for transfer log " << fd_;
  }
}

void TransferLogManager::invalidateDirectory() {
  if (fd_ < 0) {
    return;
  }
  LOG(WARNING) << "Invalidating directory " << rootDir_;
  resumptionStatus_ = INCONSISTENT_DIRECTORY;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeDirectoryInvalidationEntry(buf);
  int64_t written = ::write(fd_, buf, size);
  if (written != size) {
    PLOG(ERROR)
        << "Disk write error while writing directory invalidation entry "
        << written << " " << size;
    closeLog();
  }
  fsync();
  return;
}

void TransferLogManager::writeLogHeader() {
  if (fd_ < 0) {
    return;
  }
  LOG(INFO) << "Writing log header, version: " << LOG_VERSION
            << " recovery-id: " << recoveryId_ << " config: " << config_
            << " sender-ip: " << senderIp_;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeLogHeader(buf, recoveryId_, senderIp_, config_);
  int64_t written = ::write(fd_, buf, size);
  if (written != size) {
    PLOG(ERROR) << "Disk write error while writing log header " << written
                << " " << size;
    closeLog();
    return;
  }
  fsync();
  // header signifies a valid directory state
  resumptionStatus_ = OK;
  headerWritten_ = true;
}

void TransferLogManager::addFileCreationEntry(const std::string &fileName,
                                              int64_t seqId, int64_t fileSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  VLOG(1) << "Adding file entry to log " << fileName << " " << seqId << " "
          << fileSize;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeFileCreationEntry(buf, fileName, seqId, fileSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addBlockWriteEntry(int64_t seqId, int64_t offset,
                                            int64_t blockSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  VLOG(1) << "Adding block entry to log " << seqId << " " << offset << " "
          << blockSize;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeBlockWriteEntry(buf, seqId, offset, blockSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addFileResizeEntry(int64_t seqId, int64_t fileSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  VLOG(1) << "Adding file resize entry to log " << seqId << " " << fileSize;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeFileResizeEntry(buf, seqId, fileSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addFileInvalidationEntry(int64_t seqId) {
  if (fd_ < 0) {
    return;
  }
  VLOG(1) << "Adding invalidation entry " << seqId;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeFileInvalidationEntry(buf, seqId);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::unlink() {
  LOG(INFO) << "unlinking " << LOG_NAME;
  std::string fullLogName = getFullPath(LOG_NAME);
  if (::unlink(fullLogName.c_str()) != 0) {
    PLOG(ERROR) << "Could not unlink " << fullLogName;
  }
}

void TransferLogManager::renameBuggyLog() {
  LOG(INFO) << "Renaming " << LOG_NAME << " to " << BUGGY_LOG_NAME;
  if (::rename(getFullPath(LOG_NAME).c_str(),
               getFullPath(BUGGY_LOG_NAME).c_str()) != 0) {
    PLOG(ERROR) << "log rename failed " << LOG_NAME << " " << BUGGY_LOG_NAME;
  }
  return;
}

ErrorCode TransferLogManager::getResumptionStatus() {
  return resumptionStatus_;
}

bool TransferLogManager::parseAndPrint() {
  std::vector<FileChunksInfo> parsedInfo;
  return parseVerifyAndFix("", 0, true, parsedInfo) == OK;
}

ErrorCode TransferLogManager::parseAndMatch(
    const std::string &recoveryId, int64_t config,
    std::vector<FileChunksInfo> &fileChunksInfo) {
  recoveryId_ = recoveryId;
  config_ = config;
  return parseVerifyAndFix(recoveryId_, config, false, fileChunksInfo);
}

ErrorCode TransferLogManager::parseVerifyAndFix(
    const std::string &recoveryId, int64_t config, bool parseOnly,
    std::vector<FileChunksInfo> &parsedInfo) {
  if (fd_ < 0) {
    return INVALID_LOG;
  }
  LogParser parser(encoderDecoder_, rootDir_, recoveryId, config, parseOnly);
  resumptionStatus_ = parser.parseLog(fd_, senderIp_, parsedInfo);
  if (resumptionStatus_ == INVALID_LOG) {
    // leave the log, but close it. Keeping the invalid log ensures that the
    // directory remains invalid. Closing the log means that nothing else can
    // be written to it again
    closeLog();
  } else if (resumptionStatus_ == INCONSISTENT_DIRECTORY) {
    if (!parseOnly) {
      // if we are only parsing, we should not modify anything
      invalidateDirectory();
    }
  }
  LOG(INFO) << "Transfer log parsing finished";
  return resumptionStatus_;
}

LogParser::LogParser(LogEncoderDecoder &encoderDecoder,
                     const std::string &rootDir, const std::string &recoveryId,
                     int64_t config, bool parseOnly)
    : encoderDecoder_(encoderDecoder),
      rootDir_(rootDir),
      recoveryId_(recoveryId),
      config_(config),
      parseOnly_(parseOnly) {
}

bool LogParser::writeFileInvalidationEntries(int fd,
                                             const std::set<int64_t> &seqIds) {
  char buf[TransferLogManager::kMaxEntryLength];
  for (auto seqId : seqIds) {
    int64_t size = encoderDecoder_.encodeFileInvalidationEntry(buf, seqId);
    int toWrite = size + sizeof(int16_t);
    int written = ::write(fd, buf, toWrite);
    if (written != toWrite) {
      PLOG(ERROR) << "Disk write error while writing invalidation entry to "
                     "transfer log " << written << " " << toWrite;
      return false;
    }
  }
  return true;
}

bool LogParser::truncateExtraBytesAtEnd(int fd, int extraBytes) {
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
  // ftruncate does not change the offset, so change the offset to the end of
  // the log
  if (::lseek(fd, fileSize - extraBytes, SEEK_SET) < 0) {
    PLOG(ERROR) << "lseek failed for fd " << fd;
    return false;
  }
  return true;
}

void LogParser::clearParsedData() {
  fileInfoMap_.clear();
  seqIdToSizeMap_.clear();
  invalidSeqIds_.clear();
}

std::string LogParser::getFormattedTimestamp(int64_t timestampMicros) {
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

ErrorCode LogParser::processHeaderEntry(char *buf, int size,
                                        std::string &senderIp) {
  int64_t timestamp;
  int logVersion;
  std::string logRecoveryId;
  int64_t logConfig;
  if (!encoderDecoder_.decodeLogHeader(buf, size, timestamp, logVersion,
                                       logRecoveryId, senderIp, logConfig)) {
    LOG(ERROR) << "Couldn't decode the log header";
    return INVALID_LOG;
  }
  if (logVersion != TransferLogManager::LOG_VERSION) {
    LOG(ERROR) << "Can not parse log version " << logVersion
               << ", parser version " << TransferLogManager::LOG_VERSION;
    return INVALID_LOG;
  }
  if (senderIp.empty()) {
    LOG(ERROR) << "Log header has empty sender ip";
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp)
              << " Header entry, log-version " << logVersion << " recovery-id "
              << logRecoveryId << " sender-ip " << senderIp << " config "
              << logConfig << std::endl;
    // we do not perform verifications for parse only mode
    headerParsed_ = true;
    return OK;
  }
  if (recoveryId_ != logRecoveryId) {
    LOG(ERROR) << "Current recovery-id does not match with log recovery-id "
               << recoveryId_ << " " << logRecoveryId;
    return INCONSISTENT_DIRECTORY;
  }
  if (config_ != logConfig) {
    LOG(ERROR) << "Current config does not match with log config " << config_
               << " " << logConfig;
    return INCONSISTENT_DIRECTORY;
  }
  headerParsed_ = true;
  return OK;
}

ErrorCode LogParser::processFileCreationEntry(char *buf, int size) {
  if (!headerParsed_) {
    LOG(ERROR)
        << "Invalid log: File creation entry found before transfer log header";
    return INVALID_LOG;
  }
  const auto &options = WdtOptions::get();
  int64_t timestamp, seqId, fileSize;
  std::string fileName;
  if (!encoderDecoder_.decodeFileCreationEntry(buf, size, timestamp, fileName,
                                               seqId, fileSize)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp) << " File created "
              << fileName << " seq-id " << seqId << " file-size " << fileSize
              << std::endl;
    return OK;
  }
  if (options.resume_using_dir_tree) {
    LOG(ERROR) << "Can not have a file creation entry in directory based "
                  "resumption mode " << fileName << " " << seqId << " "
               << fileSize;
    return INVALID_LOG;
  }
  if (fileInfoMap_.find(seqId) != fileInfoMap_.end() ||
      invalidSeqIds_.find(seqId) != invalidSeqIds_.end()) {
    LOG(ERROR) << "Multiple FILE_CREATION entry for same sequence-id "
               << fileName << " " << seqId << " " << fileSize;
    return INVALID_LOG;
  }
  // verify size
  bool sizeVerificationSuccess = false;
  struct stat buffer;
  std::string fullPath;
  folly::toAppend(rootDir_, fileName, &fullPath);
  if (stat(fullPath.c_str(), &buffer) != 0) {
    PLOG(ERROR) << "stat failed for " << fileName;
  } else {
    if (options.shouldPreallocateFiles()) {
      sizeVerificationSuccess = (buffer.st_size >= fileSize);
    } else {
      sizeVerificationSuccess = true;
    }
  }

  if (sizeVerificationSuccess) {
    fileInfoMap_.emplace(seqId,
                         FileChunksInfo(seqId, fileName, buffer.st_size));
    seqIdToSizeMap_.emplace(seqId, fileSize);
  } else {
    LOG(INFO) << "Sanity check failed for " << fileName << " seq-id " << seqId
              << " file-size " << fileSize;
    invalidSeqIds_.insert(seqId);
  }
  return OK;
}

ErrorCode LogParser::processFileResizeEntry(char *buf, int size) {
  if (!headerParsed_) {
    LOG(ERROR)
        << "Invalid log: File resize entry found before transfer log header";
    return INVALID_LOG;
  }
  const auto &options = WdtOptions::get();
  int64_t timestamp, seqId, fileSize;
  if (!encoderDecoder_.decodeFileResizeEntry(buf, size, timestamp, seqId,
                                             fileSize)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp) << " File resized,"
              << " seq-id " << seqId << " new file-size " << fileSize
              << std::endl;
    return OK;
  }
  if (options.resume_using_dir_tree) {
    LOG(ERROR) << "Can not have a file resize entry in directory based "
                  "resumption mode " << seqId << " " << fileSize;
    return INVALID_LOG;
  }
  auto it = fileInfoMap_.find(seqId);
  if (it == fileInfoMap_.end()) {
    LOG(ERROR) << "File resize entry for unknown sequence-id " << seqId << " "
               << fileSize;
    return INVALID_LOG;
  }
  FileChunksInfo &chunksInfo = it->second;
  const std::string &fileName = chunksInfo.getFileName();
  auto sizeIt = seqIdToSizeMap_.find(seqId);
  WDT_CHECK(sizeIt != seqIdToSizeMap_.end());
  if (fileSize < sizeIt->second) {
    LOG(ERROR) << "File size can not reduce during resizing " << fileName << " "
               << seqId << " " << fileSize << " " << sizeIt->second;
    return INVALID_LOG;
  }

  if (options.shouldPreallocateFiles() && fileSize > chunksInfo.getFileSize()) {
    LOG(ERROR) << "Size on the disk is less than the resized size for "
               << fileName << " seq-id " << seqId << " disk-size "
               << chunksInfo.getFileSize() << " resized-size " << fileSize;
    return INVALID_LOG;
  }
  sizeIt->second = fileSize;
  return OK;
}

ErrorCode LogParser::processBlockWriteEntry(char *buf, int size) {
  if (!headerParsed_) {
    LOG(ERROR)
        << "Invalid log: Block write entry found before transfer log header";
    return INVALID_LOG;
  }
  const auto &options = WdtOptions::get();
  int64_t timestamp, seqId, offset, blockSize;
  if (!encoderDecoder_.decodeBlockWriteEntry(buf, size, timestamp, seqId,
                                             offset, blockSize)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp) << " Block written,"
              << " seq-id " << seqId << " offset " << offset << " block-size "
              << blockSize << std::endl;
    return OK;
  }
  if (options.resume_using_dir_tree) {
    LOG(ERROR) << "Can not have a block write entry in directory based "
                  "resumption mode " << seqId << " " << offset << " "
               << blockSize;
    return INVALID_LOG;
  }
  if (invalidSeqIds_.find(seqId) != invalidSeqIds_.end()) {
    LOG(INFO) << "Block entry for an invalid sequence-id " << seqId
              << ", ignoring";
    return OK;
  }
  auto it = fileInfoMap_.find(seqId);
  if (it == fileInfoMap_.end()) {
    LOG(ERROR) << "Block entry for unknown sequence-id " << seqId << " "
               << offset << " " << blockSize;
    return INVALID_LOG;
  }
  FileChunksInfo &chunksInfo = it->second;
  // check whether the block is within disk size
  if (offset + blockSize > chunksInfo.getFileSize()) {
    LOG(ERROR) << "Block end point is greater than file size in disk "
               << chunksInfo.getFileName() << " seq-id " << seqId << " offset "
               << offset << " block-size " << blockSize << " file size in disk "
               << chunksInfo.getFileSize();
    return INVALID_LOG;
  }
  chunksInfo.addChunk(Interval(offset, offset + blockSize));
  return OK;
}

ErrorCode LogParser::processFileInvalidationEntry(char *buf, int size) {
  if (!headerParsed_) {
    LOG(ERROR) << "Invalid log: File invalidation entry found before transfer "
                  "log header";
    return INVALID_LOG;
  }
  const auto &options = WdtOptions::get();
  int64_t timestamp, seqId;
  if (!encoderDecoder_.decodeFileInvalidationEntry(buf, size, timestamp,
                                                   seqId)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp)
              << " Invalidation entry for seq-id " << seqId << std::endl;
  }
  if (options.resume_using_dir_tree) {
    LOG(ERROR) << "Can not have a file invalidation entry in directory based "
                  "resumption mode " << seqId;
    return INVALID_LOG;
  }
  if (fileInfoMap_.find(seqId) == fileInfoMap_.end() &&
      invalidSeqIds_.find(seqId) == invalidSeqIds_.end()) {
    LOG(ERROR) << "Invalidation entry for an unknown sequence id " << seqId;
    return INVALID_LOG;
  }
  fileInfoMap_.erase(seqId);
  invalidSeqIds_.erase(seqId);
  return OK;
}

ErrorCode LogParser::processDirectoryInvalidationEntry(char *buf, int size) {
  int64_t timestamp;
  if (!encoderDecoder_.decodeDirectoryInvalidationEntry(buf, size, timestamp)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp) << " Directory invalidated"
              << std::endl;
    return OK;
  }
  headerParsed_ = false;
  return INCONSISTENT_DIRECTORY;
}

ErrorCode LogParser::parseLog(int fd, std::string &senderIp,
                              std::vector<FileChunksInfo> &fileChunksInfo) {
  char entry[TransferLogManager::kMaxEntryLength];
  // empty log is valid
  ErrorCode status = OK;
  while (true) {
    int16_t entrySize;
    int toRead = sizeof(entrySize);
    int numRead = ::read(fd, &entrySize, toRead);
    if (numRead < 0) {
      PLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                  << toRead;
      return INVALID_LOG;
    }
    if (numRead == 0) {
      break;
    }
    if (numRead != toRead) {
      // extra bytes at the end, most likely part of the previous write
      // succeeded partially
      if (parseOnly_) {
        LOG(INFO) << "Extra " << numRead << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(fd, numRead)) {
        return INVALID_LOG;
      }
      break;
    }
    if (entrySize < 0 || entrySize > TransferLogManager::kMaxEntryLength) {
      LOG(ERROR) << "Transfer log parse error, invalid entry length "
                 << entrySize;
      return INVALID_LOG;
    }
    numRead = ::read(fd, entry, entrySize);
    if (numRead < 0) {
      PLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                  << entrySize;
      return INVALID_LOG;
    }
    if (numRead == 0) {
      break;
    }
    if (numRead != entrySize) {
      if (parseOnly_) {
        LOG(INFO) << "Extra " << numRead << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(fd, numRead)) {
        return INVALID_LOG;
      }
      break;
    }
    TransferLogManager::EntryType type =
        (TransferLogManager::EntryType)entry[0];
    if (status == INCONSISTENT_DIRECTORY &&
        type != TransferLogManager::HEADER) {
      // If the directory is invalid, no need to process any entry other than
      // header, because only a header can validate a directory
      continue;
    }
    switch (type) {
      case TransferLogManager::HEADER:
        status = processHeaderEntry(entry + 1, entrySize - 1, senderIp);
        break;
      case TransferLogManager::FILE_CREATION:
        status = processFileCreationEntry(entry + 1, entrySize - 1);
        break;
      case TransferLogManager::BLOCK_WRITE:
        status = processBlockWriteEntry(entry + 1, entrySize - 1);
        break;
      case TransferLogManager::FILE_RESIZE:
        status = processFileResizeEntry(entry + 1, entrySize - 1);
        break;
      case TransferLogManager::FILE_INVALIDATION:
        status = processFileInvalidationEntry(entry + 1, entrySize - 1);
        break;
      case TransferLogManager::DIRECTORY_INVALIDATION:
        status = processDirectoryInvalidationEntry(entry + 1, entrySize - 1);
        break;
      default:
        LOG(ERROR) << "Invalid entry type found " << type;
        return INVALID_LOG;
    }
    if (status == INVALID_LOG) {
      LOG(ERROR) << "Invalid transfer log";
      return status;
    }
    if (status == INCONSISTENT_DIRECTORY) {
      clearParsedData();
    }
  }
  if (status == OK) {
    for (auto &pair : fileInfoMap_) {
      FileChunksInfo &fileInfo = pair.second;
      fileInfo.mergeChunks();
      fileChunksInfo.emplace_back(std::move(fileInfo));
    }
    if (!invalidSeqIds_.empty()) {
      if (!writeFileInvalidationEntries(fd, invalidSeqIds_)) {
        return INVALID_LOG;
      }
    }
  }
  return status;
}
}
}
