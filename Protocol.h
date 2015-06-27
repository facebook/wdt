#pragma once

#include "ErrorCodes.h"

#include <folly/Range.h>
#include <stddef.h>
#include <string>
#include <vector>
#include <limits.h>

namespace facebook {
namespace wdt {

/// checkpoint is a pair of port number and number of successfully transferred
/// blocks
typedef std::pair<int32_t, int64_t> Checkpoint;

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

  /// @return   list of chunks which are not part of the chunks-list
  std::vector<Interval> getRemainingChunks();

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
  int64_t seqId_;
  /// name of the file
  std::string fileName_;
  /// size of the file
  int64_t fileSize_;
  /// list of chunk info
  std::vector<Interval> chunks_;
};

/// enum representing file allocation status at the receiver side
enum FileAllocationStatus {
  NOT_EXISTS,           // file does not exist
  EXISTS_CORRECT_SIZE,  // file exists with correct size
  EXISTS_TOO_LARGE,     // file exists, but too large
  EXISTS_TOO_SMALL,     // file exists, but too small
};

/// structure representing details of a block
struct BlockDetails {
  /// name of the file
  std::string fileName;
  /// sequence-id of the file
  int64_t seqId;
  /// size of the file
  int64_t fileSize;
  /// offset of the block from the start of the file
  int64_t offset;
  /// size of the block
  int64_t dataSize;
  /// receiver side file allocation status
  FileAllocationStatus allocationStatus;
  /// seq-id of previous transfer, only valid if there is a size mismatch
  int64_t prevSeqId;
};

/// structure representing settings cmd
struct Settings {
  /// protocol version used by the sender
  int senderProtocolVersion;
  /// sender side read timeout
  int readTimeoutMillis;
  /// sender side write timeout
  int writeTimeoutMillis;
  /// transfer-id
  std::string transferId;
  /// whether checksum in enabled or not
  bool enableChecksum;
  /// whether sender wants to read previously transferred chunks or not
  bool sendFileChunks;
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
    EXIT_CMD = 0x65,      // e)xit
    SIZE_CMD = 0x5A,      // Si(Z)e
    FOOTER_CMD = 0x46,    // F)ooter
  };

  /// Max size of sender or receiver id
  static const int64_t kMaxTransferIdLength = 50;
  /// 1 byte for cmd, 2 bytes for file-name length, Max size of filename, 4
  /// variants(seq-id, data-size, offset, file-size), 1 byte for flag, 10 bytes
  /// prev seq-id
  static const int64_t kMaxHeader = 1 + 2 + PATH_MAX + 4 * 10 + 1 + 10;
  /// min number of bytes that must be send to unblock receiver
  static const int64_t kMinBufLength = 256;
  /// max size of local checkpoint encoding
  static const int64_t kMaxLocalCheckpoint = 10 + 2 * 10;
  /// max size of done command encoding(1 byte for cmd, 1 for status, 10 for
  /// number of blocks)
  static const int64_t kMaxDone = 2 + 10;
  /// max length of the size cmd encoding
  static const int64_t kMaxSize = 1 + 10;
  /// max size of settings command encoding
  static const int64_t kMaxSettings = 1 + 4 * 10 + kMaxTransferIdLength + 1;
  /// max length of the footer cmd encoding
  static const int64_t kMaxFooter = 1 + 10;
  /// max size of chunks cmd
  static const int64_t kChunksCmdLen = sizeof(int64_t) + sizeof(int64_t);
  /// max size of chunkInfo encoding length
  static const int64_t kMaxChunkEncodeLen = 20;
  /// abort cmd length
  static const int64_t kAbortLength = sizeof(int32_t) + 1 + sizeof(int64_t);

  /**
   * Decides whether the current running wdt version can support the request
   * protocol version or not
   *
   * @param requestedProtocolVersion    protocol version requested
   *
   * @return    If current wdt supports the requested version or some lower
   *            version, that version is returned. If it can not support the
   *            requested version, 0 is returned
   */
  static int negotiateProtocol(int requestedProtocolVersion);

  /// encodes blockDetails into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeHeader(int senderProtocolVersion, char *dest, int64_t &off,
                           int64_t max, const BlockDetails &blockDetails);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets BlockDetails
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeHeader(int receiverProtocolVersion, char *src, int64_t &off,
                           int64_t max, BlockDetails &blockDetails);

  /// encodes checkpoints into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeCheckpoints(char *dest, int64_t &off, int64_t max,
                                const std::vector<Checkpoint> &checkpoints);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets checkpoints
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeCheckpoints(char *src, int64_t &off, int64_t max,
                                std::vector<Checkpoint> &checkpoints);

  /// encodes numBlocks into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeDone(char *dest, int64_t &off, int64_t max,
                         int64_t numBlocks);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets numBlocks
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeDone(char *src, int64_t &off, int64_t max,
                         int64_t &numBlocks);

  /// encodes settings into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeSettings(char *dest, int64_t &off, int64_t max,
                             const Settings &settings);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets settings
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeSettings(int receiverProtocolVersion, char *src,
                             int64_t &off, int64_t max, Settings &settings);

  /// encodes totalNumBytes into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeSize(char *dest, int64_t &off, int64_t max,
                         int64_t totalNumBytes);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets totalNumBytes
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeSize(char *src, int64_t &off, int64_t max,
                         int64_t &totalNumBytes);

  /// encodes checksum into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static void encodeFooter(char *dest, int64_t &off, int64_t max,
                           int32_t checksum);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets checksum
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeFooter(char *src, int64_t &off, int64_t max,
                           int32_t &checksum);

  /// encodes protocolVersion, errCode and checkpoint into dest+off
  /// moves the off into dest pointer
  static void encodeAbort(char *dest, int64_t &off, int32_t protocolVersion,
                          ErrorCode errCode, int64_t checkpoint);

  /// decodes from src+off and consumes/moves off
  /// sets protocolversion, errcode, checkpoint
  static void decodeAbort(char *src, int64_t &off, int32_t &protocolVersion,
                          ErrorCode &errCode, int64_t &checkpoint);

  /// encodes bufSize and numFiles into dest+off
  /// moves the off into dest pointer
  static void encodeChunksCmd(char *dest, int64_t &off, int64_t bufSize,
                              int64_t numFiles);

  /// decodes from src+off and consumes/moves off
  /// sets bufSize and numFiles
  static void decodeChunksCmd(char *src, int64_t &off, int64_t &bufSize,
                              int64_t &numFiles);

  /// encodes chunk into dest+off
  /// moves the off into dest pointer
  static void encodeChunkInfo(char *dest, int64_t &off, int64_t max,
                              const Interval &chunk);

  /// decodes from src+off and consumes/moves off
  /// sets chunk
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeChunkInfo(folly::ByteRange &br, char *src, int64_t max,
                              Interval &chunk);

  /// encodes fileChunksInfo into dest+off
  /// moves the off into dest pointer
  static void encodeFileChunksInfo(char *dest, int64_t &off, int64_t max,
                                   const FileChunksInfo &fileChunksInfo);

  /// decodes from src+off and consumes/moves off
  /// sets fileChunksInfo
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeFileChunksInfo(folly::ByteRange &br, char *src, int64_t max,
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
