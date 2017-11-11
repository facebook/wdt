/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ErrorCodes.h>
#include <wdt/util/EncryptionUtils.h>

#include <folly/Range.h>
#include <limits.h>
#include <stddef.h>
#include <string>
#include <vector>

namespace facebook {
namespace wdt {

// Note: we use int64_t internally for most things - it helps for arithmetic
// and not getting accidental overflow when substracting, it helps comparaison
// and also idendtifying errors as negative values.
// BUT we made a mistake in early version of wdt where we used an encoding that
// doesn't efficiently represent negative values - so in term of serializing
// ints on the wire we expect all numbers to actually be positive (which is
// more efficient when only positive numbers are indeed encoded)
// For future fields where small negative value occurs, do use the I64 functions
// without the trailing C for compatibility or use U64 when you know for sure
// the data encoded is >= 0 (in util/SerializationUtil.h)

/// Checkpoint consists of port number, number of successfully transferred
/// blocks and number of bytes received for the last block
struct Checkpoint {
  int32_t port{0};
  /// number of complete blocks received
  int64_t numBlocks{0};
  /// Next three fields are only set if a block is received partially
  /// seq-id of the partially received block (and we don't use encryption
  /// which doesn't allow using partial blocks as they can't be authenticated)
  int64_t lastBlockSeqId{0};  // was -1 in 1.26
  /// block offset of the partially received block
  int64_t lastBlockOffset{0};
  /// number of bytes received for the partially received block
  int64_t lastBlockReceivedBytes{0};
  bool hasSeqId{false};
  Checkpoint() {
  }

  explicit Checkpoint(int32_t port) {
    this->port = port;
  }

  bool hasPartialBlockInfo() const {
    return (hasSeqId && lastBlockSeqId >= 0 && lastBlockReceivedBytes > 0);
  }

  void resetLastBlockDetails() {
    lastBlockReceivedBytes = 0;
    lastBlockSeqId = 0;  // was -1 in 1.26
    lastBlockOffset = 0;
  }

  void setLastBlockDetails(int64_t seqId, int64_t offset,
                           int64_t receivedBytes) {
    this->lastBlockSeqId = seqId;
    this->lastBlockOffset = offset;
    this->lastBlockReceivedBytes = receivedBytes;
  }

  void incrNumBlocks() {
    numBlocks++;
  }
};

std::ostream &operator<<(std::ostream &os, const Checkpoint &checkpoint);

/// structure representing a single chunk of a file
struct Interval {
  /// start offset
  int64_t start_{0};
  /// end offset
  int64_t end_{0};

  Interval() {
  }

  Interval(int64_t start, int64_t end) : start_(start), end_(end) {
    WDT_CHECK(end_ >= start_);
  }

  /// @return   size of the chunk
  int64_t size() const {
    return end_ - start_;
  }

  bool operator<(const Interval &chunk) const {
    return this->start_ < chunk.start_;
  }

  bool operator==(const Interval &chunk) const {
    return this->start_ == chunk.start_ && this->end_ == chunk.end_;
  }
};

/// class representing chunks in a file
class FileChunksInfo {
 public:
  /// making the object noncopyable
  FileChunksInfo(const FileChunksInfo &) = delete;
  FileChunksInfo &operator=(const FileChunksInfo &) = delete;
  FileChunksInfo(FileChunksInfo &&) = default;
  FileChunksInfo &operator=(FileChunksInfo &&) = default;

  FileChunksInfo() {
  }

  /**
   * @param seqId     seq-id of the file
   * @param fileName  file-name
   * @param fileSize  file-size
   */
  FileChunksInfo(int64_t seqId, std::string &fileName, int64_t fileSize)
      : seqId_(seqId), fileName_(fileName), fileSize_(fileSize) {
  }

  /// @return   file-name
  const std::string &getFileName() const {
    return fileName_;
  }

  /// @param fileName   file-name to be set
  void setFileName(const std::string &fileName) {
    fileName_ = fileName;
  }

  /// @return   seq-id of the file
  int64_t getSeqId() const {
    return seqId_;
  }

  /// @param seqId      seq-id to be set
  void setSeqId(int64_t seqId) {
    seqId_ = seqId;
  }

  /// @return         file-size
  int64_t getFileSize() const {
    return fileSize_;
  }

  /// @param fileSize   file-size to be set
  void setFileSize(int64_t fileSize) {
    fileSize_ = fileSize;
  }

  /// @return         chunks of the file
  const std::vector<Interval> &getChunks() const {
    return chunks_;
  }

  /// @param chunk    chunk to be added
  void addChunk(const Interval &chunk);

  /// merges all the chunks
  void mergeChunks();

  int64_t getTotalChunkSize() const;

  /// @return   list of chunks which are not part of the chunks-list
  std::vector<Interval> getRemainingChunks(int64_t curFileSize);

  bool operator==(const FileChunksInfo &fileChunksInfo) const {
    return this->seqId_ == fileChunksInfo.seqId_ &&
           this->fileName_ == fileChunksInfo.fileName_ &&
           this->chunks_ == fileChunksInfo.chunks_ &&
           this->fileSize_ == fileChunksInfo.fileSize_;
  }

  friend std::ostream &operator<<(std::ostream &os,
                                  const FileChunksInfo &fileChunksInfo);

 private:
  /// seq-id of the file
  int64_t seqId_{0};
  /// name of the file
  std::string fileName_;
  /// size of the file
  int64_t fileSize_{0};
  /// list of chunk info
  std::vector<Interval> chunks_;
};

/// enum representing file allocation status at the receiver side
enum FileAllocationStatus {
  NOT_EXISTS,           // file does not exist
  EXISTS_CORRECT_SIZE,  // file exists with correct size
  EXISTS_TOO_LARGE,     // file exists, but too large
  EXISTS_TOO_SMALL,     // file exists, but too small
  TO_BE_DELETED,        // file not needed, should be deleted
};

/// structure representing details of a block
struct BlockDetails {
  /// name of the file
  std::string fileName;
  /// sequence-id of the file
  int64_t seqId{0};
  /// size of the file
  int64_t fileSize{0};
  /// offset of the block from the start of the file
  int64_t offset{0};
  /// size of the block
  int64_t dataSize{0};
  /// receiver side file allocation status
  FileAllocationStatus allocationStatus{NOT_EXISTS};
  /// seq-id of previous transfer, only valid if there is a size mismatch
  int64_t prevSeqId{0};
};

/// structure representing settings cmd
struct Settings {
  /// sender side read timeout
  int readTimeoutMillis{0};
  /// sender side write timeout
  int writeTimeoutMillis{0};
  /// transfer-id
  std::string transferId{0};
  /// whether checksum in enabled or not
  bool enableChecksum{0};
  /// whether sender wants to read previously transferred chunks or not
  bool sendFileChunks{0};
  /// whether block mode is disabled
  bool blockModeDisabled{false};
  /// whether heart-beat is enabled
  bool enableHeartBeat{false};
};

class Protocol {
 public:
  /// current protocol version
  static const int protocol_version;

  // list of feature versions
  /// version from which receiver side progress reporting is supported
  static const int RECEIVER_PROGRESS_REPORT_VERSION;
  /// version from which checksum is supported
  static const int CHECKSUM_VERSION;
  /// version from which download resumption is supported
  static const int DOWNLOAD_RESUMPTION_VERSION;

  // list of encoding/decoding versions
  /// version from which flags are sent with settings cmd
  static const int SETTINGS_FLAG_VERSION;
  /// version from which flags and prevSeqId are sent with header cmd
  static const int HEADER_FLAG_AND_PREV_SEQ_ID_VERSION;
  /// version from which checkpoint started including file offset
  static const int CHECKPOINT_OFFSET_VERSION;
  /// version from which checkpoint started including seq-id
  static const int CHECKPOINT_SEQ_ID_VERSION;
  /// version from which wdt supports encryption
  static const int ENCRYPTION_V1_VERSION;
  /// version from which GCM tags were verified incrementally
  static const int INCREMENTAL_TAG_VERIFICATION_VERSION;
  /// version from which file deletion was supported for resumption
  static const int DELETE_CMD_VERSION;
  /// version from which we switched varint to better one
  static const int VARINT_CHANGE;
  /// version from which heart-beat was introduced
  static const int HEART_BEAT_VERSION;
  /// version from which wdt started to change encryption iv periodically
  static const int PERIODIC_ENCRYPTION_IV_CHANGE_VERSION;

  /// Both version, magic number and command byte
  enum CMD_MAGIC {
    DONE_CMD = 0x44,      // D)one
    FILE_CMD = 0x4C,      // L)oad
    WAIT_CMD = 0x57,      // W)ait
    ERR_CMD = 0x45,       // E)rr
    SETTINGS_CMD = 0x53,  // S)ettings
    ABORT_CMD = 0x41,     // A)bort
    CHUNKS_CMD = 0x43,    // C)hunk
    ACK_CMD = 0x61,       // a)ck
    SIZE_CMD = 0x5A,      // Si(Z)e
    FOOTER_CMD = 0x46,    // F)ooter
    LOCAL_CHECKPOINT_CMD =
        0x01,  // Local checkpoint cmd. This is a hack to ensure backward
               // compatibility. Since, the format of checkpoints is
               // <num_checkpoints><checkpoint1><checkpoint2>..., and since the
               // number of checkpoints for local checkpoint is 1, we can treat
               // 0x01 to be a separate cmd
    ENCRYPTION_CMD = 0x65,  // (e)ncryption
    HEART_BEAT_CMD = 0x48,  // (H)eart-beat
  };

  // TODO: move the rest of those definitions closer to where they need to be
  // correct, ie in cpp like kAbortLength and kChunksCmdLen

  /// Max size of sender or receiver id
  static constexpr int64_t kMaxTransferIdLength = 1024;
  /// 1 byte for cmd, 2 bytes for file-name length, Max size of filename, 4
  /// variants(seq-id, data-size, offset, file-size), 1 byte for flag, 10 bytes
  /// prev seq-id
  static constexpr int64_t kMaxHeader = 1 + 2 + PATH_MAX + 4 * 10 + 1 + 10;
  /// min number of bytes that must be send to unblock receiver
  static constexpr int64_t kMinBufLength = 256;
  /// max size of done command encoding(1 byte for cmd, 1 for status, 10 for
  /// number of blocks, 10 for number of bytes sent)
  static constexpr int64_t kMaxDone = 2 + 2 * 10;
  /// max length of the size cmd encoding
  static constexpr int64_t kMaxSize = 1 + 10;
  /// max size of settings command encoding
  static constexpr int64_t kMaxSettings = 1 + 3 * 10 + kMaxTransferIdLength + 1;
  /// max length of the footer cmd encoding, 10 byte for checksum
  static constexpr int64_t kMaxFooter = 1 + 10;
  /// max size of chunks cmd(4 bytes for buffer size and 4 bytes for number of
  /// files)
  static constexpr int64_t kChunksCmdLen = 2 * sizeof(int64_t);
  /// max size of chunkInfo encoding length
  static constexpr int64_t kMaxChunkEncodeLen = 20;
  /// abort cmd length(4 bytes for protocol, 1 byte for error-code and 8 bytes
  /// for checkpoint)
  static constexpr int64_t kAbortLength = sizeof(int32_t) + 1 + sizeof(int64_t);
  /// max size of version encoding
  static constexpr int64_t kMaxVersion = 10;
  /// max size of encryption cmd(1 byte for cmd, 1 byte for
  /// encryption type, rest for initialization vector and tag interval)
  static constexpr int64_t kEncryptionCmdLen =
      1 + 1 + 1 + kAESBlockSize + sizeof(int32_t);

  static_assert(kMinBufLength <= kMaxHeader && kMaxSettings <= kMaxHeader,
                "Minimum buffer size is kMaxHeader. Header and Settings cmd "
                "must fit within the buffer");
  /**
   * Return the library version, including protocol.
   * For debugging/identification purpose.
   */
  static const std::string getFullVersion();

  /**
   * Decides whether the current running wdt version can support the request
   * protocol version or not
   *
   * @param requestedProtocolVersion    protocol version requested
   * @param curProtocolVersion          current protocol version
   *
   * @return    If current wdt supports the requested version or some lower
   *            version, that version is returned. If it can not support the
   *            requested version, 0 is returned
   */
  static int negotiateProtocol(int requestedProtocolVersion,
                               int curProtocolVersion = protocol_version);

  /// @return     max local checkpoint length for a specific version
  static int getMaxLocalCheckpointLength(int protocolVersion);

  /// encodes blockDetails into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeHeader(int senderProtocolVersion, char *dest, int64_t &off,
                           int64_t max, const BlockDetails &blockDetails);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets BlockDetails
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeHeader(int receiverProtocolVersion, char *src, int64_t &off,
                           int64_t max, BlockDetails &blockDetails);

  /// encodes checkpoints into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeCheckpoints(int protocolVersion, char *dest, int64_t &off,
                                int64_t max,
                                const std::vector<Checkpoint> &checkpoints);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets checkpoints
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeCheckpoints(int protocolVersion, char *src, int64_t &off,
                                int64_t max,
                                std::vector<Checkpoint> &checkpoints);

  /// encodes numBlocks, totalBytes into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeDone(int protocolVersion, char *dest, int64_t &off,
                         int64_t max, int64_t numBlocks, int64_t totalBytes);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets numBlocks, totalBytes
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeDone(int protocolVersion, char *src, int64_t &off,
                         int64_t max, int64_t &numBlocks, int64_t &totalBytes);

  /// encodes settings into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeSettings(int senderProtocolVersion, char *dest,
                             int64_t &off, int64_t max,
                             const Settings &settings);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets senderProtocolVersion
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeVersion(char *src, int64_t &off, int64_t max,
                            int &senderProtocolVersion);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets settings
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeSettings(int protocolVersion, char *src, int64_t &off,
                             int64_t max, Settings &settings);

  /// encodes encryption info into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeEncryptionSettings(char *dest, int64_t &off, int64_t max,
                                       const EncryptionType encryptionType,
                                       const std::string &iv,
                                       int32_t tagInterval);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets encryption type, initializaion vector and tag interval
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeEncryptionSettings(char *src, int64_t &off, int64_t max,
                                       EncryptionType &encryptionType,
                                       std::string &iv, int32_t &tagInterval);

  /// encodes totalNumBytes into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeSize(char *dest, int64_t &off, int64_t max,
                         int64_t totalNumBytes);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets totalNumBytes
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeSize(char *src, int64_t &off, int64_t max,
                         int64_t &totalNumBytes);

  /// encodes checksum or tag into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeFooter(char *dest, int64_t &off, int64_t max,
                           int32_t checksum);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets checksum or tag
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeFooter(char *src, int64_t &off, int64_t max,
                           int32_t &checksum);

  /// encodes protocolVersion, errCode and checkpoint into dest+off
  /// moves the off into dest pointer
  static bool encodeAbort(char *dest, int64_t &off, int64_t max,
                          int32_t protocolVersion, ErrorCode errCode,
                          int64_t checkpoint);

  /// decodes from src+off and consumes/moves off
  /// sets protocolversion, errcode, checkpoint
  static bool decodeAbort(char *src, int64_t &off, int64_t max,
                          int32_t &protocolVersion, ErrorCode &errCode,
                          int64_t &checkpoint);

  /// encodes bufSize and numFiles into dest+off
  /// moves the off into dest pointer
  static bool encodeChunksCmd(char *dest, int64_t &off, int64_t max,
                              int64_t bufSize, int64_t numFiles);

  /// decodes from src+off and consumes/moves off
  /// sets bufSize and numFiles
  static bool decodeChunksCmd(char *src, int64_t &off, int64_t max,
                              int64_t &bufSize, int64_t &numFiles);

  /// encodes chunk into dest+off
  /// moves the off into dest pointer
  static bool encodeChunkInfo(char *dest, int64_t &off, int64_t max,
                              const Interval &chunk);

  /// decodes from src+off and consumes/moves off
  /// sets chunk
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeChunkInfo(folly::ByteRange &br, Interval &chunk);

  /// encodes fileChunksInfo into dest+off
  /// moves the off into dest pointer
  static bool encodeFileChunksInfo(char *dest, int64_t &off, int64_t max,
                                   const FileChunksInfo &fileChunksInfo);

  /// decodes from src+off and consumes/moves off
  /// sets fileChunksInfo
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeFileChunksInfo(folly::ByteRange &br,
                                   FileChunksInfo &fileChunksInfo);

  /**
   * returns maximum number of bytes to encode a given FileChunksInfo
   *
   * @param fileChunkInfo    FileChunksInfo to encode
   *
   * @return                 max number of bytes to encode
   */
  static int64_t maxEncodeLen(const FileChunksInfo &fileChunkInfo);

  /// encodes fileChunksInfo into dest+off
  /// moves the off into dest pointer
  /// returns number of fileChunks encoded
  static int64_t encodeFileChunksInfoList(
      char *dest, int64_t &off, int64_t bufSize, int64_t startIndex,
      const std::vector<FileChunksInfo> &fileChunksInfoList);

  /// decodes from src+off and consumes/moves off
  /// sets fileChunksInfoList
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeFileChunksInfoList(
      char *src, int64_t &off, int64_t dataSize,
      std::vector<FileChunksInfo> &fileChunksInfoList);
};
}
}  // namespace facebook::wdt
