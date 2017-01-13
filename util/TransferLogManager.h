/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/Protocol.h>
#include <wdt/WdtOptions.h>

#include <condition_variable>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>

namespace facebook {
namespace wdt {

/**
 * Download Resumption in WDT:
 * WDT can resume download in two modes.
 *
 * 1) Log based resumption : In this mode, WDT writes all important events like
 * start of a new transfer, file creation, file resize, block write to .wdt.log
 * in the destination directory. Before the transfer is started, this log is
 * parsed and a list of FileChunksInfo is created. This information is passed to
 * the sender which does a diff and sends only the missing portions.
 *
 * 2) Size based resumption : In this mode, WDT traverses the destination
 * directory and trusts the size of the file. Sender only sends the extra
 * portions of the file. Before traversing the directory .wdt.log is checked to
 * see if previous transfers were also done in this mode or not. If the mode has
 * changed, we add an INCONSISTENT_DIRECTORY entry to the log and transfer
 * everything. This mode enabled by setting resume_using_dir_tree option to
 * true.
 */

constexpr char kWdtLogName[] = ".wdt.log";
constexpr char kWdtBuggyLogName[] = ".wdt.log.bug";
/**
 * class responsible for encoding and decoding transfer log entries
 */
class LogEncoderDecoder {
 public:
  /// encodes header entry
  int64_t encodeLogHeader(char *dest, int64_t max,
                          const std::string &recoveryId,
                          const std::string &senderIp, int64_t config);

  /// decodes header entry
  bool decodeLogHeader(char *buf, int16_t size, int64_t &timestamp,
                       int &version, std::string &recoveryId,
                       std::string &senderIp, int64_t &config);

  /// encodes file creation entry
  int64_t encodeFileCreationEntry(char *dest, int64_t max,
                                  const std::string &fileName,
                                  const int64_t seqId, const int64_t fileSize);

  /// decodes file creation entry
  bool decodeFileCreationEntry(char *buf, int16_t size, int64_t &timestamp,
                               std::string &fileName, int64_t &seqId,
                               int64_t &fileSize);

  /// encodes block write entry
  int64_t encodeBlockWriteEntry(char *dest, int64_t max, const int64_t seqId,
                                const int64_t offset, const int64_t blockSize);

  /// decodes block write entry
  bool decodeBlockWriteEntry(char *buf, int16_t size, int64_t &timestamp,
                             int64_t &seqId, int64_t &offset,
                             int64_t &blockSize);

  /// encodes file resize entry
  int64_t encodeFileResizeEntry(char *dest, int64_t max, const int64_t seqId,
                                const int64_t fileSize);

  /// decodes file resize entry
  bool decodeFileResizeEntry(char *buf, int16_t size, int64_t &timestamp,
                             int64_t &seqId, int64_t &fileSize);

  /// encodes file invalidation entry
  int64_t encodeFileInvalidationEntry(char *dest, int64_t max,
                                      const int64_t &seqId);

  /// decodes file invalidation entry
  bool decodeFileInvalidationEntry(char *buf, int16_t size, int64_t &timestamp,
                                   int64_t &seqId);

  /// encodes directory invalidation entry
  int64_t encodeDirectoryInvalidationEntry(char *buf, int64_t max);

  /// decodes directory invalidation entry
  bool decodeDirectoryInvalidationEntry(char *buf, int16_t size,
                                        int64_t &timestamp);

 private:
  /// @return     timestamp in microseconds
  int64_t timestampInMicroseconds() const;
};

/**
 * This class manages reads and writes to receiver side transfer log. This
 * class buffers writes to transfer log and starts a writer thread to
 * periodically write log entries to the disk. This also has a function to read
 * all the entries and correct the log if needed.
 */
class TransferLogManager {
 public:
  const static int WLOG_VERSION;

  enum EntryType {
    HEADER,                  // log header
    FILE_CREATION,           // File created and space allocated
    BLOCK_WRITE,             // Complete block fsynced to disk
    FILE_INVALIDATION,       // Missing file
    FILE_RESIZE,             // File Resized
    DIRECTORY_INVALIDATION,  // Directory content is invalid
  };

  /// 2 bytes for entry size, 1 byte for entry-type, PATH_MAX for file-name, 10
  /// bytes for seq-id, 10 bytes for file-size, 10 bytes for timestamp
  static const int64_t kMaxEntryLength = 2 + 1 + 10 + PATH_MAX + 2 * 10;

  TransferLogManager(const WdtOptions &options, const std::string &rootDir)
      : options_(options) {
    rootDir_ = rootDir;
    if (rootDir_.back() != '/') {
      rootDir_.push_back('/');
    }
  }

  /**
   * Opens the log for reading and writing. In case of log based
   * resumption, starts writer thread. If the log is not present, a new one is
   * created
   */
  ErrorCode openLog();

  /// Start the log writer thread
  ErrorCode startThread();

  /**
   * In case of log based resumption, signals to the writer thread to finish.
   * Waits for the writer thread to finish. Closes the transfer log.
   */
  void closeLog();

  /**
   * Verifies sender ip
   *
   * @param curSenderIp     current sender ip
   *
   * @return         whether sender-ip matched log ip
   */
  bool verifySenderIp(const std::string &curSenderIp);

  /**
   * If resumption is based on directory tree, writes log header to the
   * transfer log. Else, adds a file creation entry to the log buffer
   */
  void writeLogHeader();

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
  void addFileInvalidationEntry(int64_t seqId);

  /// Writes an invalidation entry for the directory to the transfer log.
  void invalidateDirectory();

  /**
   * parses the transfer log and prints entries
   *
   * @return       Whether the log is valid or not
   */
  bool parseAndPrint();

  /// renames .wdt.log to .wdt.log.bug
  void renameBuggyLog();

  /// Compacts transfer log
  void compactLog();

  /**
   * parses transfer log, does validation and fixes the log in case of partial
   * writes from previous transfer. Also parsed info is cached for later use and
   * can be accessed through getParsedFileChunksInfo
   *
   * @param recoveryId      recovery-id of the current transfer
   * @param config          transfer config encoded as int
   * @param fileChunksInfo  this vector is populated with parsed chunks info
   *
   * @return      status of the parsing
   */
  ErrorCode parseAndMatch(const std::string &recoveryId, int64_t config,
                          std::vector<FileChunksInfo> &fileChunksInfo);

  /// @return     returns current resumption status
  ErrorCode getResumptionStatus();

  /**
   * Unlinks wdt transfer log
   */
  void unlink();

  /// destructor
  ~TransferLogManager();

 private:
  /// Shutdown the log writer thread
  void shutdownThread();

  std::string getFullPath(const std::string &relPath);

  /**
   * entry point for the writer thread. This thread periodically writes buffer
   * contents to disk
   */
  void threadProcWriteEntriesToDisk();

  /// Write 'entries' to disk synchronously
  /// return true if entries are written successfully, otherwise false.
  bool writeEntriesToDiskNoLock(const std::vector<std::string> &entries);

  /// fsync transfer log
  void fsyncLog();

  /// Check log or directory hasn't been removed under us
  /// TODO: consider calling periodically from the thread for early warning
  ErrorCode checkLog();

  /// closes transfer log
  void close();

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
   * @return                  Log parsing status
   */
  ErrorCode parseVerifyAndFix(const std::string &recoveryId, int64_t config,
                              bool parseOnly,
                              std::vector<FileChunksInfo> &parsedInfo);

  LogEncoderDecoder encoderDecoder_;

  const WdtOptions &options_;

  /// File handler for writing
  int fd_{-1};
  /// root directory
  std::string rootDir_;
  /// recovery id
  std::string recoveryId_;
  /// sender ip
  std::string senderIp_;
  /// transfer config
  int64_t config_;
  /// Entry buffer
  std::vector<std::string> entries_;
  /// Flag to signal end to the writer thread
  bool finished_{false};
  /// current resumption status
  ErrorCode resumptionStatus_;
  /// Whether the header is written or not. If it is not written, that means the
  /// sender is has not asked for file chunks, which means that sender does not
  /// want to resume. If this is false, file-creation, block-write and
  /// file-resize entries are not written.
  bool headerWritten_{false};
  /// Writer thread
  std::thread writerThread_;
  std::mutex mutex_;
  std::condition_variable conditionFinished_;
};

/// class responsible for parsing and fixing transfer log
class LogParser {
 public:
  LogParser(const WdtOptions &options, LogEncoderDecoder &encoderDecoder,
            const std::string &rootDir, const std::string &recoveryId,
            int64_t config, bool parseOnly);

  ErrorCode parseLog(int fd, std::string &senderIp,
                     std::vector<FileChunksInfo> &fileChunksInfo);

 private:
  std::string getFormattedTimestamp(int64_t timestamp);

  void clearParsedData();

  /**
   * Truncates the log
   *
   * @param fd          file descriptor
   * @param extraBytes  extra bytes at the end of the file
   *
   * @return          If successful, true, else false
   */
  bool truncateExtraBytesAtEnd(int fd, int64_t extraBytes);

  /**
   * writes invalidation entries to the disk.
   *
   * @param seqIds    Invalid seq-ids
   */
  bool writeFileInvalidationEntries(int fd, const std::set<int64_t> &seqIds);

  ErrorCode processHeaderEntry(char *buf, int64_t max, int64_t size,
                               std::string &senderIp);

  // TODO: switch to ByteRange
  ErrorCode processFileCreationEntry(char *buf, int64_t size);

  ErrorCode processBlockWriteEntry(char *buf, int64_t size);

  ErrorCode processFileResizeEntry(char *buf, int64_t size);

  ErrorCode processFileInvalidationEntry(char *buf, int64_t size);

  ErrorCode processDirectoryInvalidationEntry(char *buf, int64_t size);

  const WdtOptions &options_;
  LogEncoderDecoder &encoderDecoder_;
  std::string rootDir_;
  std::string recoveryId_;
  int64_t config_;
  bool parseOnly_;
  /// whether header is parsed or not
  bool headerParsed_{false};

  /// seq-id to chunks map
  std::map<int64_t, FileChunksInfo> fileInfoMap_;
  /// seq-id to file-size map
  std::map<int64_t, int64_t> seqIdToSizeMap_;
  /// set of invalid seq-ids
  std::set<int64_t> invalidSeqIds_;
};
}
}
