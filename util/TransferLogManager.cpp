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

#include <fcntl.h>
#include <folly/lang/Bits.h>
#include <folly/Conv.h>
#include <folly/Range.h>

#include <sys/file.h>
#include <sys/stat.h>
#include <wdt/Reporting.h>
#include <ctime>
#include <iomanip>

using folly::ByteRange;
using std::string;

namespace facebook {
namespace wdt {

// TODO consider revamping this log format

const int TransferLogManager::WLOG_VERSION = 2;

int64_t LogEncoderDecoder::timestampInMicroseconds() const {
  auto timestamp = Clock::now();
  return std::chrono::duration_cast<std::chrono::microseconds>(
             timestamp.time_since_epoch())
      .count();
}

int64_t LogEncoderDecoder::encodeLogHeader(char *dest, int64_t max,
                                           const string &recoveryId,
                                           const string &senderIp,
                                           int64_t config) {
  // increment by 2 bytes to later store the total length
  char *ptr = dest + sizeof(int16_t);
  max -= sizeof(int16_t);
  int64_t size = 0;
  WDT_CHECK_GE(max, 1);
  ptr[size++] = TransferLogManager::HEADER;
  bool ok = encodeVarI64C(ptr, max, size, timestampInMicroseconds()) &&
            encodeVarI64C(ptr, max, size, TransferLogManager::WLOG_VERSION) &&
            encodeString(ptr, max, size, recoveryId) &&
            encodeString(ptr, max, size, senderIp) &&
            encodeVarI64C(ptr, max, size, config);
  if (!ok) {
    WLOG(ERROR) << "Log header buffer too small " << max << " for header "
                << recoveryId << " , " << senderIp;
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size);
  return (size + sizeof(int16_t));
}

bool LogEncoderDecoder::decodeLogHeader(char *buf, int16_t size,
                                        int64_t &timestamp, int &version,
                                        string &recoveryId, string &senderIp,
                                        int64_t &config) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp) && decodeInt32C(br, version) &&
            decodeString(br, recoveryId) && decodeString(br, senderIp) &&
            decodeInt64C(br, config);
  if (!ok || br.size() != 0) {
    WLOG(ERROR) << "Error decoding log header size " << size << " ok " << ok
                << " left over " << br.size();
    return false;
  }
  return ok;
}

int64_t LogEncoderDecoder::encodeFileCreationEntry(char *dest, int64_t max,
                                                   const string &fileName,
                                                   const int64_t seqId,
                                                   const int64_t fileSize) {
  // increment by 2 bytes to later store the total length
  int64_t size = sizeof(int16_t);
  WDT_CHECK_GE(max, size + 1);
  dest[size++] = TransferLogManager::FILE_CREATION;
  bool ok = encodeVarI64C(dest, max, size, timestampInMicroseconds()) &&
            encodeString(dest, max, size, fileName) &&
            encodeVarI64C(dest, max, size, seqId) &&
            encodeVarI64C(dest, max, size, fileSize);
  if (!ok) {
    WLOG(ERROR) << "Log header buffer too small " << max << " for file c entry "
                << fileName;
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size - sizeof(int16_t));
  return size;
}

bool LogEncoderDecoder::decodeFileCreationEntry(char *buf, int16_t size,
                                                int64_t &timestamp,
                                                string &fileName,
                                                int64_t &seqId,
                                                int64_t &fileSize) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp) && decodeString(br, fileName) &&
            decodeInt64C(br, seqId) && decodeInt64C(br, fileSize);
  if (!ok || (br.size() != 0)) {
    WLOG(ERROR) << "Did not decode properly file creat entry " << size << " ok "
                << ok << " left over " << br.size();
    return false;
  }
  return true;
}

int64_t LogEncoderDecoder::encodeBlockWriteEntry(char *dest, int64_t max,
                                                 const int64_t seqId,
                                                 const int64_t offset,
                                                 const int64_t blockSize) {
  int64_t size = sizeof(int16_t);
  WDT_CHECK_GE(max, size + 1);
  dest[size++] = TransferLogManager::BLOCK_WRITE;
  bool ok = encodeVarI64C(dest, max, size, timestampInMicroseconds()) &&
            encodeVarI64C(dest, max, size, seqId) &&
            encodeVarI64C(dest, max, size, offset) &&
            encodeVarI64C(dest, max, size, blockSize);
  if (!ok) {
    WLOG(ERROR) << "Failed to encode blockwrite entry into " << max;
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size - sizeof(int16_t));
  return size;
}

bool LogEncoderDecoder::decodeBlockWriteEntry(char *buf, int16_t size,
                                              int64_t &timestamp,
                                              int64_t &seqId, int64_t &offset,
                                              int64_t &blockSize) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp) && decodeInt64C(br, seqId) &&
            decodeInt64C(br, offset) && decodeInt64C(br, blockSize);
  if (!ok || (br.size() != 0)) {
    WLOG(ERROR) << "Did not decode properly block write entry " << size
                << " ok " << ok << " left over " << br.size();
    return false;
  }
  return true;
}

int64_t LogEncoderDecoder::encodeFileResizeEntry(char *dest, int64_t max,
                                                 const int64_t seqId,
                                                 const int64_t fileSize) {
  int64_t size = sizeof(int16_t);
  WDT_CHECK_GE(max, size + 1);
  dest[size++] = TransferLogManager::FILE_RESIZE;
  bool ok = encodeVarI64C(dest, max, size, timestampInMicroseconds()) &&
            encodeVarI64C(dest, max, size, seqId) &&
            encodeVarI64C(dest, max, size, fileSize);
  if (!ok) {
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size - sizeof(int16_t));
  return size;
}

bool LogEncoderDecoder::decodeFileResizeEntry(char *buf, int16_t size,
                                              int64_t &timestamp,
                                              int64_t &seqId,
                                              int64_t &fileSize) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp) && decodeInt64C(br, seqId) &&
            decodeInt64C(br, fileSize);
  if (!ok || (br.size() != 0)) {
    WLOG(ERROR) << "Did not decode properly block write entry " << size
                << " ok " << ok << " left over " << br.size();
    return false;
  }
  return true;
}

int64_t LogEncoderDecoder::encodeFileInvalidationEntry(char *dest, int64_t max,
                                                       const int64_t &seqId) {
  int64_t size = sizeof(int16_t);
  WDT_CHECK_GE(max, size + 1);
  dest[size++] = TransferLogManager::FILE_INVALIDATION;
  bool ok = encodeVarI64C(dest, max, size, timestampInMicroseconds()) &&
            encodeVarI64C(dest, max, size, seqId);
  if (!ok) {
    WLOG(ERROR) << "Failed to encode inval entry into " << max;
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size - sizeof(int16_t));
  return size;
}

bool LogEncoderDecoder::decodeFileInvalidationEntry(char *buf, int16_t size,
                                                    int64_t &timestamp,
                                                    int64_t &seqId) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp) && decodeInt64C(br, seqId);
  if (!ok || (br.size() != 0)) {
    WLOG(ERROR) << "Did not decode properly file inval entry " << size << " ok "
                << ok << " left over " << br.size();
    return false;
  }
  return true;
}

int64_t LogEncoderDecoder::encodeDirectoryInvalidationEntry(char *dest,
                                                            int64_t max) {
  int64_t size = sizeof(int16_t);
  WDT_CHECK_GE(max, size + 1);
  dest[size++] = TransferLogManager::DIRECTORY_INVALIDATION;
  if (!encodeVarI64C(dest, max, size, timestampInMicroseconds())) {
    WLOG(ERROR) << "No room in " << max << " for dir inval entry";
    return -1;
  }
  folly::storeUnaligned<int16_t>(dest, size - sizeof(int16_t));
  return size;
}

// TODO make the caller make and pass the byterange
bool LogEncoderDecoder::decodeDirectoryInvalidationEntry(char *buf,
                                                         int16_t size,
                                                         int64_t &timestamp) {
  ByteRange br = makeByteRange(buf, size);
  bool ok = decodeInt64C(br, timestamp);
  if (!ok || (br.size() != 0)) {
    WLOG(ERROR) << "Did not decode properly dir inval entry " << size << " ok "
                << ok << " left over " << br.size();
    return false;
  }
  return true;
}

string TransferLogManager::getFullPath(const string &relPath) {
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  string fullPath = rootDir_;
  fullPath.append(relPath);
  return fullPath;
}

ErrorCode TransferLogManager::openLog() {
  WDT_CHECK(fd_ < 0);
  WDT_CHECK(!rootDir_.empty()) << "Root directory not set";
  WDT_CHECK(options_.enable_download_resumption);

  const string logPath = getFullPath(kWdtLogName);
  fd_ = ::open(logPath.c_str(), O_RDWR);
  if (fd_ < 0) {
    if (errno != ENOENT) {
      WPLOG(ERROR) << "Could not open wdt log " << logPath;
      return TRANSFER_LOG_ACQUIRE_ERROR;
    } else {
      // creation of the log path (which can still be a race)
      WLOG(INFO) << logPath << " doesn't exist... creating...";
      fd_ = ::open(logPath.c_str(), O_CREAT | O_EXCL, 0644);
      if (fd_ < 0) {
        WPLOG(WARNING) << "Could not create wdt log (maybe ok if race): "
                       << logPath;
      } else {
        // On windows/cygwin for instance the flock will silently succeed yet
        // not lock on a newly created file... workaround is to close and reopen
        ::close(fd_);
      }
      fd_ = ::open(logPath.c_str(), O_RDWR);
      if (fd_ < 0) {
        WPLOG(ERROR) << "Still couldn't open wdt log after create attempt: "
                     << logPath;
        return TRANSFER_LOG_ACQUIRE_ERROR;
      }
    }
  }
  // try to acquire file lock
  if (::flock(fd_, LOCK_EX | LOCK_NB) != 0) {
    WPLOG(ERROR) << "Failed to acquire transfer log lock " << logPath << " "
                 << fd_;
    close();
    return TRANSFER_LOG_ACQUIRE_ERROR;
  }
  WLOG(INFO) << "Transfer log opened and lock acquired on " << logPath;
  return OK;
}

ErrorCode TransferLogManager::startThread() {
  if (!options_.resume_using_dir_tree) {
    // start writer thread
    if (resumptionStatus_ != OK) {
      return resumptionStatus_;
    }
    writerThread_ =
        std::thread(&TransferLogManager::threadProcWriteEntriesToDisk, this);
    WLOG(INFO) << "Log writer thread started";
  }
  return OK;
}

void TransferLogManager::shutdownThread() {
  if (writerThread_.joinable()) {
    // stop writer thread
    {
      std::lock_guard<std::mutex> lock(mutex_);
      finished_ = true;
    }
    conditionFinished_.notify_one();
    writerThread_.join();
  }
}

ErrorCode TransferLogManager::checkLog() {
  if (fd_ < 0) {
    WLOG(WARNING) << "No log to check";
    return ERROR;
  }
  const string fullLogName = getFullPath(kWdtLogName);
  struct stat stat1, stat2;
  if (stat(fullLogName.c_str(), &stat1)) {
    WPLOG(ERROR) << "CORRUPTION! Can't stat log file " << fullLogName
                 << " (deleted under us)";
    exit(TRANSFER_LOG_ACQUIRE_ERROR);
    return ERROR;
  }
  if (fstat(fd_, &stat2)) {
    WPLOG(ERROR) << "Unable to stat log by fd " << fd_;
    exit(TRANSFER_LOG_ACQUIRE_ERROR);
    return ERROR;
  }
  if (stat1.st_ino != stat2.st_ino) {
    WLOG(ERROR) << "CORRUPTION! log file " << fullLogName << " changed "
                << " old/open fd inode " << stat2.st_ino << " on fs "
                << stat1.st_ino;
    exit(TRANSFER_LOG_ACQUIRE_ERROR);
    return ERROR;
  }
  WLOG(INFO) << fullLogName << " still ok with " << stat1.st_ino;
  return OK;
}

void TransferLogManager::close() {
  if (fd_ < 0) {
    return;
  }
  checkLog();
  if (::close(fd_) != 0) {
    WPLOG(ERROR) << "Failed to close wdt log " << fd_;
  } else {
    WLOG(INFO) << "Transfer log closed";
  }
  fd_ = -1;
}

void TransferLogManager::closeLog() {
  if (fd_ < 0) {
    return;
  }
  if (!options_.resume_using_dir_tree) {
    shutdownThread();
  }
  close();
}

TransferLogManager::~TransferLogManager() {
  WDT_CHECK_LT(fd_, 0) << "Destructor called, but transfer log not closed";
}

bool TransferLogManager::writeEntriesToDiskNoLock(
    const std::vector<std::string> &entries) {
  string buffer;
  // write entries to disk
  for (const auto &entry : entries) {
    buffer.append(entry);
  }
  if (buffer.empty()) {
    // do not write when there is nothing to write
    return true;
  }
  int64_t toWrite = buffer.size();
  int64_t written = ::write(fd_, buffer.c_str(), toWrite);
  if (written != toWrite) {
    WPLOG(ERROR) << "Disk write error while writing transfer log " << written
                 << " " << toWrite;
    return false;
  }

  return true;
}

void TransferLogManager::threadProcWriteEntriesToDisk() {
  WDT_CHECK(fd_ >= 0) << "Writer thread started before the log is opened";
  WLOG(INFO) << "Transfer log writer thread started";
  WDT_CHECK(options_.transfer_log_write_interval_ms >= 0);

  auto waitingTime =
      std::chrono::milliseconds(options_.transfer_log_write_interval_ms);
  std::vector<string> entries;
  bool finished = false;
  while (!finished) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      // conditionFinished_ will put writer thread to sleep. However, writer
      // thread wakes up at interval of "watingTime" (in ms) to write log
      // entries to disk. When "shutdownThread" is called, writer thread will
      // wake up immediately and exit after writing current log entries to disk
      conditionFinished_.wait_for(lock, waitingTime);
      finished = finished_;
      // make a copy of all the entries so that we do not need to hold lock
      // during writing
      entries = entries_;
      entries_.clear();
    }

    if (!writeEntriesToDiskNoLock(entries)) {
      return;
    }
  }
  WLOG(INFO) << "Transfer log writer thread finished";
}

bool TransferLogManager::verifySenderIp(const string &curSenderIp) {
  if (fd_ < 0) {
    return false;
  }
  bool verifySuccessful = true;
  if (!options_.disable_sender_verification_during_resumption) {
    if (senderIp_.empty()) {
      WLOG(INFO) << "Sender-ip empty, not verifying sender-ip, new-ip: "
                 << curSenderIp;
    } else if (senderIp_ != curSenderIp) {
      WLOG(ERROR) << "Current sender ip does not match ip in the "
                     "transfer log "
                  << curSenderIp << " " << senderIp_
                  << ", ignoring transfer log";
      verifySuccessful = false;
      invalidateDirectory();
    }
  } else {
    WLOG(WARNING) << "Sender-ip verification disabled " << senderIp_ << " "
                  << curSenderIp;
  }
  senderIp_ = curSenderIp;
  return verifySuccessful;
}

void TransferLogManager::fsyncLog() {
  WDT_CHECK(fd_ >= 0);
  if (::fsync(fd_) != 0) {
    WPLOG(ERROR) << "fsync failed for transfer log " << fd_;
  }
}

void TransferLogManager::invalidateDirectory() {
  if (fd_ < 0) {
    return;
  }
  WLOG(WARNING) << "Invalidating directory " << rootDir_;
  resumptionStatus_ = INCONSISTENT_DIRECTORY;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeDirectoryInvalidationEntry(buf, sizeof(buf));
  int64_t written = ::write(fd_, buf, size);
  if (written != size) {
    WPLOG(ERROR)
        << "Disk write error while writing directory invalidation entry "
        << written << " " << size;
    closeLog();
  }
  fsyncLog();
  return;
}

void TransferLogManager::writeLogHeader() {
  if (fd_ < 0) {
    return;
  }
  WLOG(INFO) << "Writing log header, version: " << WLOG_VERSION
             << " recovery-id: " << recoveryId_ << " config: " << config_
             << " sender-ip: " << senderIp_;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeLogHeader(
      buf, kMaxEntryLength, recoveryId_, senderIp_, config_);
  int64_t written = ::write(fd_, buf, size);
  if (written != size) {
    WPLOG(ERROR) << "Disk write error while writing log header " << written
                 << " " << size;
    closeLog();
    return;
  }
  fsyncLog();
  // header signifies a valid directory state
  resumptionStatus_ = OK;
  headerWritten_ = true;
}

void TransferLogManager::addFileCreationEntry(const string &fileName,
                                              int64_t seqId, int64_t fileSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  WVLOG(1) << "Adding file entry to log " << fileName << " " << seqId << " "
           << fileSize;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeFileCreationEntry(
      buf, sizeof(buf), fileName, seqId, fileSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addBlockWriteEntry(int64_t seqId, int64_t offset,
                                            int64_t blockSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  WVLOG(1) << "Adding block entry to log " << seqId << " " << offset << " "
           << blockSize;
  char buf[kMaxEntryLength];
  int64_t size = encoderDecoder_.encodeBlockWriteEntry(buf, sizeof(buf), seqId,
                                                       offset, blockSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addFileResizeEntry(int64_t seqId, int64_t fileSize) {
  if (fd_ < 0 || !headerWritten_) {
    return;
  }
  WVLOG(1) << "Adding file resize entry to log " << seqId << " " << fileSize;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeFileResizeEntry(buf, sizeof(buf), seqId, fileSize);

  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::addFileInvalidationEntry(int64_t seqId) {
  if (fd_ < 0) {
    return;
  }
  WLOG(INFO) << "Adding invalidation entry " << seqId;
  char buf[kMaxEntryLength];
  int64_t size =
      encoderDecoder_.encodeFileInvalidationEntry(buf, kMaxEntryLength, seqId);
  std::lock_guard<std::mutex> lock(mutex_);
  entries_.emplace_back(buf, size);
}

void TransferLogManager::unlink() {
  WDT_CHECK_LT(fd_, 0) << "Unlink called before closeLog!";
  WLOG(INFO) << "unlinking " << kWdtLogName;
  string fullLogName = getFullPath(kWdtLogName);
  if (::unlink(fullLogName.c_str()) != 0) {
    WPLOG(ERROR) << "Could not unlink " << fullLogName;
  }
}

void TransferLogManager::renameBuggyLog() {
  WDT_CHECK_LT(fd_, 0) << "renameBuggyLog called before closeLog!";
  WLOG(INFO) << "Renaming " << kWdtLogName << " to " << kWdtBuggyLogName;
  if (::rename(getFullPath(kWdtLogName).c_str(),
               getFullPath(kWdtBuggyLogName).c_str()) != 0) {
    WPLOG(ERROR) << "log rename failed " << kWdtLogName << " "
                 << kWdtBuggyLogName;
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
    const string &recoveryId, int64_t config,
    std::vector<FileChunksInfo> &fileChunksInfo) {
  recoveryId_ = recoveryId;
  config_ = config;
  return parseVerifyAndFix(recoveryId_, config, false, fileChunksInfo);
}

ErrorCode TransferLogManager::parseVerifyAndFix(
    const string &recoveryId, int64_t config, bool parseOnly,
    std::vector<FileChunksInfo> &parsedInfo) {
  if (fd_ < 0) {
    return INVALID_LOG;
  }
  LogParser parser(options_, encoderDecoder_, rootDir_, recoveryId, config,
                   parseOnly);
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
  WLOG(INFO) << "Transfer log parsing finished "
             << errorCodeToStr(resumptionStatus_);
  return resumptionStatus_;
}

void TransferLogManager::compactLog() {
  auto fullLogPath = getFullPath(kWdtLogName);

  WLOG(INFO) << "Started compacting transfer log " << fullLogPath;

  if (fd_ < 0) {
    WLOG(ERROR) << "Failed to compact transfer log because log handle has "
                << "been closed";
    return;
  }

  // Shutdown writer thread to avoid race conditon against fd_
  shutdownThread();

  // Make sure log data is flushed.
  fsyncLog();

  if (::lseek(fd_, 0, SEEK_SET) < 0) {
    WPLOG(ERROR) << "lseek failed for fd " << fd_;
    return;
  }

  std::vector<FileChunksInfo> fileChunksInfoVec;
  auto code = parseAndMatch(recoveryId_, config_, fileChunksInfoVec);
  if (code != OK) {
    WLOG(ERROR) << "Failed to parse " << fullLogPath << " "
                << errorCodeToStr(code);
    return;
  }

  for (const auto &fileChunksInfo : fileChunksInfoVec) {
    // Found multiple chunks for a file.
    if (fileChunksInfo.getChunks().size() != 1) {
      WLOG(ERROR) << "File " << fileChunksInfo.getFileName()
                  << " has fragemented log entries";
      return;
    }

    if (fileChunksInfo.getTotalChunkSize() != fileChunksInfo.getFileSize()) {
      WLOG(ERROR) << "File " << fileChunksInfo.getFileName()
                  << " has mismatched total chunk size and file size";
      return;
    }
  }

  WLOG(INFO) << "Successfully verified transfer log integrity";

  if (::ftruncate(fd_, 0) != 0) {
    WPLOG(ERROR) << "ftruncate failed for fd " << fd_;
    return;
  }

  if (::lseek(fd_, 0, SEEK_SET) < 0) {
    WPLOG(ERROR) << "lseek failed for fd " << fd_;
    return;
  }

  writeLogHeader();

  for (const auto &fileChunksInfo : fileChunksInfoVec) {
    addFileCreationEntry(fileChunksInfo.getFileName(),
                         fileChunksInfo.getSeqId(),
                         fileChunksInfo.getFileSize());
    addBlockWriteEntry(fileChunksInfo.getSeqId(), 0,
                       fileChunksInfo.getFileSize());
  }

  std::vector<string> entries;

  {
    std::lock_guard<std::mutex> lock(mutex_);
    entries = entries_;
    entries_.clear();
  }

  bool writeSuccess = writeEntriesToDiskNoLock(entries);
  if (writeSuccess) {
    WLOG(INFO) << "Finished compacting transfer log";
  } else {
    WLOG(ERROR) << "Failed compacting transfer log";
  }
}

LogParser::LogParser(const WdtOptions &options,
                     LogEncoderDecoder &encoderDecoder, const string &rootDir,
                     const string &recoveryId, int64_t config, bool parseOnly)
    : options_(options),
      encoderDecoder_(encoderDecoder),
      rootDir_(rootDir),
      recoveryId_(recoveryId),
      config_(config),
      parseOnly_(parseOnly) {
}

bool LogParser::writeFileInvalidationEntries(int fd,
                                             const std::set<int64_t> &seqIds) {
  char buf[TransferLogManager::kMaxEntryLength];
  for (auto seqId : seqIds) {
    int64_t size =
        encoderDecoder_.encodeFileInvalidationEntry(buf, sizeof(buf), seqId);
    int64_t written = ::write(fd, buf, size);
    if (written != size) {
      WPLOG(ERROR) << "Disk write error while writing invalidation entry to "
                      "transfer log "
                   << written << " " << size;
      return false;
    }
  }
  return true;
}

bool LogParser::truncateExtraBytesAtEnd(int fd, int64_t extraBytes) {
  WLOG(INFO) << "Removing extra " << extraBytes
             << " bytes from the end of transfer log";
  struct stat statBuffer;
  if (fstat(fd, &statBuffer) != 0) {
    WPLOG(ERROR) << "fstat failed on fd " << fd;
    return false;
  }
  off_t fileSize = statBuffer.st_size;
  if (::ftruncate(fd, fileSize - extraBytes) != 0) {
    WPLOG(ERROR) << "ftruncate failed for fd " << fd;
    return false;
  }
  // ftruncate does not change the offset, so change the offset to the end of
  // the log
  if (::lseek(fd, fileSize - extraBytes, SEEK_SET) < 0) {
    WPLOG(ERROR) << "lseek failed for fd " << fd;
    return false;
  }
  return true;
}

void LogParser::clearParsedData() {
  fileInfoMap_.clear();
  seqIdToSizeMap_.clear();
  invalidSeqIds_.clear();
}

string LogParser::getFormattedTimestamp(int64_t timestampMicros) {
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

ErrorCode LogParser::processHeaderEntry(char *buf, int64_t max, int64_t size,
                                        string &senderIp) {
  if (size > max) {
    WLOG(ERROR) << "Bad size " << size << " vs max " << max;
    return INVALID_LOG;
  }
  int64_t timestamp;
  int logVersion;
  string logRecoveryId;
  int64_t logConfig;
  if (!encoderDecoder_.decodeLogHeader(buf, size, timestamp, logVersion,
                                       logRecoveryId, senderIp, logConfig)) {
    WLOG(ERROR) << "Couldn't decode the log header";
    return INVALID_LOG;
  }
  if (logVersion != TransferLogManager::WLOG_VERSION) {
    WLOG(ERROR) << "Can not parse log version " << logVersion
                << ", parser version " << TransferLogManager::WLOG_VERSION;
    return INVALID_LOG;
  }
  if (senderIp.empty()) {
    WLOG(ERROR) << "Log header has empty sender ip";
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
    WLOG(ERROR) << "Current recovery-id does not match with log recovery-id "
                << recoveryId_ << " " << logRecoveryId;
    return INCONSISTENT_DIRECTORY;
  }
  if (config_ != logConfig) {
    WLOG(ERROR) << "Current config does not match with log config " << config_
                << " " << logConfig;
    return INCONSISTENT_DIRECTORY;
  }
  headerParsed_ = true;
  return OK;
}

ErrorCode LogParser::processFileCreationEntry(char *buf, int64_t size) {
  if (!headerParsed_) {
    WLOG(ERROR)
        << "Invalid log: File creation entry found before transfer log header";
    return INVALID_LOG;
  }
  int64_t timestamp, seqId, fileSize;
  string fileName;
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
  if (options_.resume_using_dir_tree) {
    WLOG(ERROR) << "Can not have a file creation entry in directory based "
                   "resumption mode "
                << fileName << " " << seqId << " " << fileSize;
    return INVALID_LOG;
  }
  if (fileInfoMap_.find(seqId) != fileInfoMap_.end() ||
      invalidSeqIds_.find(seqId) != invalidSeqIds_.end()) {
    WLOG(ERROR) << "Multiple FILE_CREATION entry for same sequence-id "
                << fileName << " " << seqId << " " << fileSize;
    return INVALID_LOG;
  }
  // verify size
  bool sizeVerificationSuccess = false;
  struct stat buffer;
  string fullPath;
  folly::toAppend(rootDir_, fileName, &fullPath);
  if (stat(fullPath.c_str(), &buffer) != 0) {
    WPLOG(ERROR) << "stat failed for " << fileName;
  } else {
    if (options_.shouldPreallocateFiles()) {
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
    WLOG(INFO) << "Sanity check failed for " << fileName << " seq-id " << seqId
               << " file-size " << fileSize;
    invalidSeqIds_.insert(seqId);
  }
  return OK;
}

ErrorCode LogParser::processFileResizeEntry(char *buf, int64_t size) {
  if (!headerParsed_) {
    WLOG(ERROR)
        << "Invalid log: File resize entry found before transfer log header";
    return INVALID_LOG;
  }
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
  if (options_.resume_using_dir_tree) {
    WLOG(ERROR) << "Can not have a file resize entry in directory based "
                   "resumption mode "
                << seqId << " " << fileSize;
    return INVALID_LOG;
  }
  auto it = fileInfoMap_.find(seqId);
  if (it == fileInfoMap_.end()) {
    WLOG(ERROR) << "File resize entry for unknown sequence-id " << seqId << " "
                << fileSize;
    return INVALID_LOG;
  }
  FileChunksInfo &chunksInfo = it->second;
  const string &fileName = chunksInfo.getFileName();
  auto sizeIt = seqIdToSizeMap_.find(seqId);
  WDT_CHECK(sizeIt != seqIdToSizeMap_.end());
  if (fileSize < sizeIt->second) {
    WLOG(ERROR) << "File size can not reduce during resizing " << fileName
                << " " << seqId << " " << fileSize << " " << sizeIt->second;
    return INVALID_LOG;
  }

  if (options_.shouldPreallocateFiles() &&
      fileSize > chunksInfo.getFileSize()) {
    WLOG(ERROR) << "Size on the disk is less than the resized size for "
                << fileName << " seq-id " << seqId << " disk-size "
                << chunksInfo.getFileSize() << " resized-size " << fileSize;
    return INVALID_LOG;
  }
  sizeIt->second = fileSize;
  return OK;
}

ErrorCode LogParser::processBlockWriteEntry(char *buf, int64_t size) {
  if (!headerParsed_) {
    WLOG(ERROR)
        << "Invalid log: Block write entry found before transfer log header";
    return INVALID_LOG;
  }
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
  if (options_.resume_using_dir_tree) {
    WLOG(ERROR) << "Can not have a block write entry in directory based "
                   "resumption mode "
                << seqId << " " << offset << " " << blockSize;
    return INVALID_LOG;
  }
  if (invalidSeqIds_.find(seqId) != invalidSeqIds_.end()) {
    WLOG(INFO) << "Block entry for an invalid sequence-id " << seqId
               << ", ignoring";
    return OK;
  }
  auto it = fileInfoMap_.find(seqId);
  if (it == fileInfoMap_.end()) {
    WLOG(ERROR) << "Block entry for unknown sequence-id " << seqId << " "
                << offset << " " << blockSize;
    return INVALID_LOG;
  }
  FileChunksInfo &chunksInfo = it->second;
  // check whether the block is within disk size
  if (offset + blockSize > chunksInfo.getFileSize()) {
    WLOG(ERROR) << "Block end point is greater than file size in disk "
                << chunksInfo.getFileName() << " seq-id " << seqId << " offset "
                << offset << " block-size " << blockSize
                << " file size in disk " << chunksInfo.getFileSize();
    return INVALID_LOG;
  }
  chunksInfo.addChunk(Interval(offset, offset + blockSize));
  return OK;
}

ErrorCode LogParser::processFileInvalidationEntry(char *buf, int64_t size) {
  if (!headerParsed_) {
    WLOG(ERROR) << "Invalid log: File invalidation entry found before transfer "
                   "log header";
    return INVALID_LOG;
  }
  int64_t timestamp, seqId;
  if (!encoderDecoder_.decodeFileInvalidationEntry(buf, size, timestamp,
                                                   seqId)) {
    return INVALID_LOG;
  }
  if (parseOnly_) {
    std::cout << getFormattedTimestamp(timestamp)
              << " Invalidation entry for seq-id " << seqId << std::endl;
    return OK;
  }
  if (options_.resume_using_dir_tree) {
    WLOG(ERROR) << "Can not have a file invalidation entry in directory based "
                   "resumption mode "
                << seqId;
    return INVALID_LOG;
  }
  if (fileInfoMap_.find(seqId) == fileInfoMap_.end() &&
      invalidSeqIds_.find(seqId) == invalidSeqIds_.end()) {
    WLOG(ERROR) << "Invalidation entry for an unknown sequence id " << seqId;
    return INVALID_LOG;
  }
  fileInfoMap_.erase(seqId);
  invalidSeqIds_.erase(seqId);
  return OK;
}

ErrorCode LogParser::processDirectoryInvalidationEntry(char *buf,
                                                       int64_t size) {
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

ErrorCode LogParser::parseLog(int fd, string &senderIp,
                              std::vector<FileChunksInfo> &fileChunksInfo) {
  char entry[TransferLogManager::kMaxEntryLength];
  // empty log is valid
  ErrorCode status = OK;
  while (true) {
    int16_t entrySize;
    int64_t toRead = sizeof(int16_t);
    int64_t numRead = ::read(fd, &entrySize, toRead);
    if (numRead < 0) {
      WPLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                   << toRead;
      return INVALID_LOG;
    }
    if (numRead == 0) {
      WVLOG(1) << "got EOF, toRead " << toRead;
      break;
    }
    if (numRead != toRead) {
      // extra bytes at the end, most likely part of the previous write
      // succeeded partially
      if (parseOnly_) {
        WLOG(INFO) << "Extra " << numRead << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(fd, numRead)) {
        return INVALID_LOG;
      }
      break;
    }
    if (entrySize <= 0 || entrySize > TransferLogManager::kMaxEntryLength) {
      WLOG(ERROR) << "Transfer log parse error, invalid entry length "
                  << entrySize;
      return INVALID_LOG;
    }
    numRead = ::read(fd, entry, entrySize);
    if (numRead < 0) {
      WPLOG(ERROR) << "Error while reading transfer log " << numRead << " "
                   << entrySize;
      return INVALID_LOG;
    }
    if (numRead != entrySize) {
      // extra bytes also includes the size entry
      int64_t extraBytes = numRead + sizeof(int16_t);
      if (parseOnly_) {
        WLOG(INFO) << "Extra " << extraBytes << " bytes at the end of the log";
      } else if (!truncateExtraBytesAtEnd(fd, extraBytes)) {
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
    char *buf = entry + 1;
    const int64_t bufSize = sizeof(entry) - 1;
    const int64_t entryLen = entrySize - 1;
    switch (type) {
      case TransferLogManager::HEADER:
        status = processHeaderEntry(buf, bufSize, entryLen, senderIp);
        break;
      case TransferLogManager::FILE_CREATION:
        status = processFileCreationEntry(buf, entryLen);
        break;
      case TransferLogManager::BLOCK_WRITE:
        status = processBlockWriteEntry(buf, entryLen);
        break;
      case TransferLogManager::FILE_RESIZE:
        status = processFileResizeEntry(buf, entryLen);
        break;
      case TransferLogManager::FILE_INVALIDATION:
        status = processFileInvalidationEntry(buf, entryLen);
        break;
      case TransferLogManager::DIRECTORY_INVALIDATION:
        status = processDirectoryInvalidationEntry(buf, entryLen);
        break;
      default:
        WLOG(ERROR) << "Invalid entry type found " << type;
        return INVALID_LOG;
    }
    if (status == INVALID_LOG) {
      WLOG(ERROR) << "Invalid transfer log";
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
