/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "Protocol.h"

#include <string>
#include <set>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <iostream>

namespace facebook {
namespace wdt {

/**
 * This class manages reads and writes to receiver side transfer log. This
 * class buffers writes to transfer log and starts a writer thread to
 * periodically write log entries to the disk. This also has a function to read
 * all the entries and correct the log if needed.
 */
class TransferLogManager {
 public:
  /**
   * Opens the log for writing and also starts a writer thread
   *
   * @param curSenderIp     current sender ip
   *
   * @return          If successful, true, else false
   */
  bool openAndStartWriter(const std::string &curSenderIp);

  /**
   * Verifies sender ip and opens the log for writing
   *
   * @param curSenderIp     current sender ip
   *
   * @return         whether sender-ip matched log ip
   */
  bool verifySenderIpAndOpen(const std::string &curSenderIp);

  /**
   * Adds a log header to the buffer
   *
   * @param config    transfer config encoded as int
   */
  void addLogHeader(int64_t config);

  /**
   * Writes log header to the transfer log
   *
   * @param config    transfer config encoded as int
   *
   * @return    whether the write was successful
   */
  bool writeLogHeader(int64_t config);

  /**
   * Adds a file creation entry to the log buffer
   *
   * @param fileName  Name of the file
   * @param seqId     seq-id of the file
   * @param fileSize  size of the file
   */
  void addFileCreationEntry(const std::string &fileName, int64_t seqId,
                            int64_t fileSize);
  /**
   * Adds a block write entry to the log buffer
   *
   * @param seqId     seq-id of the file
   * @param offset    block offset
   * @param blockSize size of the block
   */
  void addBlockWriteEntry(int64_t seqId, int64_t offset, int64_t blockSize);

  /**
   * Adds a file resize entry to the log buffer
   *
   * @param seqId     seq-id of the file
   * @param fileSize  size of the file
   */
  void addFileResizeEntry(int64_t seqId, int64_t fileSize);

  /**
   * Adds an invalidation entry to the log buffer
   *
   * @param seqId     seq-id of the file
   */
  void addInvalidationEntry(int64_t seqId);

  /** parses the transfer log and prints entries
   *
   *  @return       Whether the log is valid or not
   */
  bool parseAndPrint();

  /**
   * parses transfer log, does validation and fixes the log in case of partial
   * writes from previous transfer. Also parsed info is cached for later use and
   * can be accessed through getParsedFileChunksInfo
   *
   * @param recoveryId      recovery-id of the current transfer
   * @param config          transfer config encoded as int
   * @param fileChunksInfo  this vector is populated with parsed chunks info
   *
   * @return       Whether the parsing was successful or not
   */
  bool parseAndMatch(const std::string &recoveryId, int64_t config,
                     std::vector<FileChunksInfo> &fileChunksInfo);

  /**
   * Signals to the writer thread to finish. Waits for the writer thread to
   * finish. Closes the transfer log.
   *
   * @return          If successful, true, else false
   */
  bool closeAndStopWriter();

  /// @return   whether the log was successfully closed or not
  bool close();

  /**
   * Unlinks wdt transfer log
   *
   * @return          If successful, true, else false
   */
  bool unlink();

  /**
   * Rename the log to BUGGY_LOG_NAME
   *
   * @return          If successful, true, else false
   */
  bool renameBuggyLog();

  /// @param rootDir        root directory of the receiver
  void setRootDir(const std::string &rootDir);

 private:
  const int LOG_VERSION = 2;
  const std::string LOG_NAME = ".wdt.log";
  const std::string BUGGY_LOG_NAME = ".wdt.log.bug";
  /// 2 bytes for entry size, 1 byte for entry-type, PATH_MAX for file-name, 10
  /// bytes for seq-id, 10 bytes for file-size, 10 bytes for timestamp
  static const int64_t kMaxEntryLength = 2 + 1 + 10 + PATH_MAX + 2 * 10;
  enum EntryType {
    HEADER,              // log header
    FILE_CREATION,       // File created and space allocated
    BLOCK_WRITE,         // Complete block fsynced to disk
    ENTRY_INVALIDATION,  // Missing file
    FILE_RESIZE,         // File Resized
  };

  /**
   * opens the file in write mode
   *
   * @return          If successful, true, else false
   */
  int open();

  /**
   * Truncates the log
   *
   * @param fd          file descriptor
   * @param extraBytes  extra bytes at the end of the file
   *
   * @return          If successful, true, else false
   */
  bool truncateExtraBytesAtEnd(int fd, int extraBytes);

  std::string getFullPath(const std::string &relPath);

  /**
   * entry point for the writer thread. This thread periodically writes buffer
   * contents to disk
   */
  void writeEntriesToDisk();

  /**
   * Encodes log header
   *
   * @param dest    buffer to encode into
   * @param off     offset in the buffer, this is moved to end of the encoding
   * @param config  transfer config encoded as int
   */
  void encodeLogHeader(char *dest, int64_t &off, int64_t config);

  /**
   * Encodes invalidation entry
   *
   * @param dest    buffer to encode into
   * @param off     offset in the buffer, this is moved to end of the encoding
   * @param seqId   sequence-id
   */
  void encodeInvalidationEntry(char *dest, int64_t &off, int64_t seqId);

  /**
   * writes invalidation entries to the disk.
   *
   * @param seqIds    Invalid seq-ids
   */
  bool writeInvalidationEntries(const std::set<int64_t> &seqIds);

  /// Parses log header
  bool parseLogHeader(char *buf, int16_t entrySize, int64_t &timestamp,
                      int &version, std::string &recoveryId,
                      std::string &senderIp, int64_t &config);

  /// Parses file creation entry
  bool parseFileCreationEntry(char *buf, int16_t entrySize, int64_t &timestamp,
                              std::string &fileName, int64_t &seqId,
                              int64_t &fileSize);

  /// Parses block write entry
  bool parseBlockWriteEntry(char *buf, int16_t entrySize, int64_t &timestamp,
                            int64_t &seqId, int64_t &offset,
                            int64_t &blockSize);

  /// Parses file resize entry
  bool parseFileResizeEntry(char *buf, int16_t entrySize, int64_t &timestamp,
                            int64_t &seqId, int64_t &fileSize);

  /// Parses invalidation entry
  bool parseInvalidationEntry(char *buf, int16_t entrySize, int64_t &timestamp,
                              int64_t &seqId);

  /**
   * Parses the transfer log. Verifies if all the file exists or not(This is
   * done to verify whether directory entries were synced to disk before or
   * not). Also writes invalidation entries for files with verification failure.
   *
   * @param recoveryId        recovery-id, this is verified against the logged
   *                          recovery-id
   * @param config            config of the current transfer
   * @param parseOnly         If true, all parsed entries are logged, and the
   *                          log is not modified or verified
   * @param parsedInfo        vector to populate with parsed data, only
   *                          populated if parseOnly is false
   *
   * @return                  If successful, true, else false
   */
  bool parseVerifyAndFix(const std::string &recoveryId, int64_t config,
                         bool parseOnly,
                         std::vector<FileChunksInfo> &parsedInfo);

  int64_t timestampInMicroseconds() const;

  std::string getFormattedTimestamp(int64_t timestamp);

  /// File handler for writing
  int fd_{-1};
  /// root directory
  std::string rootDir_;
  /// whether the log has been unlinked or not
  bool unlinked_{true};
  /// recovery id
  std::string recoveryId_;
  /// sender ip
  std::string senderIp_;
  /// whether logging is enabled or not
  bool loggingEnabled_{false};
  /// Entry buffer
  std::vector<std::string> entries_;
  /// Flag to signal end to the writer thread
  bool finished_{false};
  /// Writer thread
  std::thread writerThread_;
  std::mutex mutex_;
  std::condition_variable conditionFinished_;
};
}
}
