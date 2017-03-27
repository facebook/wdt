/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Protocol.h>

#include <wdt/ErrorCodes.h>
#include <wdt/WdtOptions.h>
#include <wdt/util/SerializationUtil.h>

namespace facebook {
namespace wdt {

using std::string;
using folly::ByteRange;

const int Protocol::protocol_version = WDT_PROTOCOL_VERSION;

const int Protocol::RECEIVER_PROGRESS_REPORT_VERSION = 11;
const int Protocol::CHECKSUM_VERSION = 12;
const int Protocol::DOWNLOAD_RESUMPTION_VERSION = 13;

const int Protocol::SETTINGS_FLAG_VERSION = 12;
const int Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION = 13;
const int Protocol::CHECKPOINT_OFFSET_VERSION = 16;
const int Protocol::CHECKPOINT_SEQ_ID_VERSION = 21;
const int Protocol::ENCRYPTION_V1_VERSION = 23;
const int Protocol::INCREMENTAL_TAG_VERIFICATION_VERSION = 25;
const int Protocol::DELETE_CMD_VERSION = 26;
const int Protocol::VARINT_CHANGE = 27;
const int Protocol::HEART_BEAT_VERSION = 29;
const int Protocol::PERIODIC_ENCRYPTION_IV_CHANGE_VERSION = 30;

/* All methods of Protocol class are static (functions) */

const string Protocol::getFullVersion() {
  string fullVersion(WDT_VERSION_STR);
  fullVersion.append(" p ");
  fullVersion.append(std::to_string(protocol_version));
  return fullVersion;
}

int Protocol::negotiateProtocol(int requestedProtocolVersion,
                                int curProtocolVersion) {
  if (requestedProtocolVersion < 10) {
    WLOG(WARNING) << "Can not handle protocol " << requestedProtocolVersion;
    return 0;
  }
  return std::min<int>(curProtocolVersion, requestedProtocolVersion);
}

std::ostream &operator<<(std::ostream &os, const Checkpoint &checkpoint) {
  os << "checkpoint-port: " << checkpoint.port
     << " num-blocks: " << checkpoint.numBlocks
     << " seq-id: " << checkpoint.lastBlockSeqId
     << " block-offset: " << checkpoint.lastBlockOffset
     << " received-bytes: " << checkpoint.lastBlockReceivedBytes;
  return os;
}

int64_t FileChunksInfo::getTotalChunkSize() const {
  int64_t totalChunkSize = 0;
  for (const auto &chunk : chunks_) {
    totalChunkSize += chunk.size();
  }
  return totalChunkSize;
}

void FileChunksInfo::addChunk(const Interval &chunk) {
  chunks_.emplace_back(chunk);
}

void FileChunksInfo::mergeChunks() {
  if (chunks_.empty()) {
    return;
  }
  std::sort(chunks_.begin(), chunks_.end());
  std::vector<Interval> mergedChunks;
  Interval curChunk = chunks_[0];
  const size_t numChunks = chunks_.size();
  for (size_t i = 1; i < numChunks; i++) {
    if (chunks_[i].start_ > curChunk.end_) {
      mergedChunks.emplace_back(curChunk);
      curChunk = chunks_[i];
    } else {
      curChunk.end_ = std::max(curChunk.end_, chunks_[i].end_);
    }
  }
  mergedChunks.emplace_back(curChunk);
  chunks_ = mergedChunks;
}

std::vector<Interval> FileChunksInfo::getRemainingChunks(int64_t curFileSize) {
  std::vector<Interval> remainingChunks;
  int64_t curStart = 0;
  for (const auto &chunk : chunks_) {
    if (chunk.start_ > curStart) {
      remainingChunks.emplace_back(curStart, chunk.start_);
    }
    curStart = chunk.end_;
  }
  if (curStart < curFileSize) {
    remainingChunks.emplace_back(curStart, curFileSize);
  }
  return remainingChunks;
}

std::ostream &operator<<(std::ostream &os,
                         FileChunksInfo const &fileChunksInfo) {
  os << "name " << fileChunksInfo.getFileName() << " seqId "
     << fileChunksInfo.getSeqId() << " file-size "
     << fileChunksInfo.getFileSize() << " number of chunks "
     << fileChunksInfo.getChunks().size();
  for (const auto &chunk : fileChunksInfo.getChunks()) {
    os << " (" << chunk.start_ << ", " << chunk.end_ << ") ";
  }
  return os;
}

int Protocol::getMaxLocalCheckpointLength(int protocolVersion) {
  // add 10 for the size of the vector(local checkpoint is a vector of
  // checkpoints with size 1). Even though, it only takes 1 byte to encode this,
  // previous version of code assumes this to be 10. So, keeping this as 10
  int length = 10;
  // port & number of blocks
  length += 2 * 10;
  if (protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
    // number of bytes in the last block
    length += 10;
  }
  if (protocolVersion >= CHECKPOINT_SEQ_ID_VERSION) {
    // seq-id and block offset
    length += 2 * 10;
  }
  return length;
}

bool Protocol::encodeHeader(int senderProtocolVersion, char *dest, int64_t &off,
                            const int64_t max,
                            const BlockDetails &blockDetails) {
  WDT_CHECK_GE(max, 0);
  const size_t umax = static_cast<size_t>(max);  // we made sure it's not < 0
  bool ok = encodeString(dest, max, off, blockDetails.fileName) &&
            encodeVarI64C(dest, umax, off, blockDetails.seqId) &&
            encodeVarI64C(dest, umax, off, blockDetails.dataSize) &&
            encodeVarI64C(dest, umax, off, blockDetails.offset) &&
            encodeVarI64C(dest, umax, off, blockDetails.fileSize);
  if (ok && senderProtocolVersion >= HEADER_FLAG_AND_PREV_SEQ_ID_VERSION) {
    uint8_t flags = blockDetails.allocationStatus;
    if (off >= max) {
      ok = false;
    } else {
      dest[off++] = static_cast<char>(flags);
      if (flags == EXISTS_TOO_SMALL || flags == EXISTS_TOO_LARGE) {
        // prev seq-id is only used in case the size is less on the sender side
        ok = encodeVarI64C(dest, umax, off, blockDetails.prevSeqId);
      }
    }
  }
  if (!ok) {
    WLOG(ERROR) << "Failed to encode header, ran out of space, " << off << " "
                << max;
  }
  return ok;
}

bool Protocol::decodeHeader(int receiverProtocolVersion, char *src,
                            int64_t &off, const int64_t max,
                            BlockDetails &blockDetails) {
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;

  bool ok = decodeString(br, blockDetails.fileName) &&
            decodeInt64C(br, blockDetails.seqId) &&
            decodeInt64C(br, blockDetails.dataSize) &&
            decodeInt64C(br, blockDetails.offset) &&
            decodeInt64C(br, blockDetails.fileSize);
  if (ok && receiverProtocolVersion >= HEADER_FLAG_AND_PREV_SEQ_ID_VERSION) {
    if (br.empty()) {
      WLOG(ERROR) << "Invalid (too short) input len " << max << " at offset "
                  << (max - obr.size());
      return false;
    }
    uint8_t flags = br.front();
    // first 3 bits are used to represent allocation status
    blockDetails.allocationStatus = (FileAllocationStatus)(flags & 7);
    br.pop_front();
    if (blockDetails.allocationStatus == EXISTS_TOO_SMALL ||
        blockDetails.allocationStatus == EXISTS_TOO_LARGE) {
      ok = decodeInt64C(br, blockDetails.prevSeqId);
    }
  }
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeCheckpoints(int protocolVersion, char *dest, int64_t &off,
                                 int64_t max,
                                 const std::vector<Checkpoint> &checkpoints) {
  WDT_CHECK_GE(max, 0);
  const size_t umax = static_cast<size_t>(max);
  bool ok = encodeVarU64(dest, umax, off, checkpoints.size());
  for (const auto &checkpoint : checkpoints) {
    if (!ok) {
      break;
    }
    ok = encodeVarI64C(dest, umax, off, checkpoint.port) &&
         encodeVarI64C(dest, umax, off, checkpoint.numBlocks);
    if (ok && protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
      ok = encodeVarI64C(dest, umax, off, checkpoint.lastBlockReceivedBytes);
    }
    if (ok && protocolVersion >= CHECKPOINT_SEQ_ID_VERSION) {
      ok = encodeVarI64C(dest, umax, off, checkpoint.lastBlockSeqId) &&
           encodeVarI64C(dest, umax, off, checkpoint.lastBlockOffset);
    }
  }
  if (!ok) {
    WLOG(ERROR) << "encodeCheckpoints " << off << " " << max;
  }
  return ok;
}

bool Protocol::decodeCheckpoints(int protocolVersion, char *src, int64_t &off,
                                 int64_t max,
                                 std::vector<Checkpoint> &checkpoints) {
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;
  uint64_t len;
  bool ok = decodeUInt64(br, len);
  for (uint64_t i = 0; ok && i < len; i++) {
    Checkpoint checkpoint;
    ok = decodeInt32C(br, checkpoint.port) &&
         decodeInt64C(br, checkpoint.numBlocks);
    if (ok && protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
      ok = decodeInt64C(br, checkpoint.lastBlockReceivedBytes);
    }
    if (ok && protocolVersion >= CHECKPOINT_SEQ_ID_VERSION) {
      // Deal with -1 encoded by pre 1.27 version
      uint64_t uv = 0;
      ok = decodeUInt64(br, uv);
      checkpoint.lastBlockSeqId = static_cast<int64_t>(uv);
      if (ok && protocolVersion < VARINT_CHANGE) {
        // pre 1.27 encodes -1 for invalid and use 9 0xff bytes and 1 0x01 byte
        // 1.27+ decodes the 9 0xff as max uint64_t (all FFs) so we check and
        // consume the leftover 0x01
        if (uv == 0xffffffffffffffff && !br.empty()) {
          if (br.front() != 0x01) {
            WLOG(ERROR) << "Unexpected decoding of pre1.27 -1 : " << br.front();
            ok = false;
          }
          br.advance(1);  // 1.26 used 10 bytes
          WLOG(INFO) << "Fixed v" << protocolVersion << " chkpt to -1 seqid";
          checkpoint.lastBlockSeqId = -1;
        }
      }
      ok = ok && decodeInt64C(br, checkpoint.lastBlockOffset);
      checkpoint.hasSeqId = true;
    }
    if (ok) {
      checkpoints.emplace_back(checkpoint);
    }
  }
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeDone(int protocolVersion, char *dest, int64_t &off,
                          int64_t max, int64_t numBlocks, int64_t bytesSent) {
  bool ok = encodeVarI64C(dest, max, off, numBlocks);
  if (ok && protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
    ok = encodeVarI64C(dest, max, off, bytesSent);
  }
  return ok;
}

bool Protocol::decodeDone(int protocolVersion, char *src, int64_t &off,
                          int64_t max, int64_t &numBlocks, int64_t &bytesSent) {
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;
  bool ok = decodeInt64C(br, numBlocks);
  if (ok && protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
    ok = decodeInt64C(br, bytesSent);
  }
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeSize(char *dest, int64_t &off, int64_t max,
                          int64_t totalNumBytes) {
  return encodeVarI64C(dest, max, off, totalNumBytes);
}

bool Protocol::decodeSize(char *src, int64_t &off, int64_t max,
                          int64_t &totalNumBytes) {
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;
  bool ok = decodeInt64C(br, totalNumBytes);
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeAbort(char *dest, int64_t &off, const int64_t max,
                           int32_t protocolVersion, ErrorCode errCode,
                           int64_t checkpoint) {
  if (off + kAbortLength > max) {
    WLOG(ERROR) << "Trying to encode abort in too small of a buffer sz " << max
                << " off " << off;
    return false;
  }
  bool ok = encodeInt32FixedLength(dest, max, off, protocolVersion);
  if (!ok) {
    return false;
  }
  dest[off++] = errCode;
  return encodeInt64FixedLength(dest, max, off, checkpoint);
}

bool Protocol::decodeAbort(char *src, int64_t &off, int64_t max,
                           int32_t &protocolVersion, ErrorCode &errCode,
                           int64_t &checkpoint) {
  if (off + kAbortLength > max) {
    WLOG(ERROR) << "Trying to decode abort, not enough to read sz " << max
                << " at off " << off;
    return false;
  }
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;
  bool ok = decodeInt32FixedLength(br, protocolVersion);
  if (!ok) {
    return false;
  }
  errCode = (ErrorCode)br.front();
  br.pop_front();
  ok = decodeInt64FixedLength(br, checkpoint);
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeChunksCmd(char *dest, int64_t &off, int64_t max,
                               int64_t bufSize, int64_t numFiles) {
  return encodeInt64FixedLength(dest, max, off, bufSize) &&
         encodeInt64FixedLength(dest, max, off, numFiles);
}

bool Protocol::decodeChunksCmd(char *src, int64_t &off, int64_t max,
                               int64_t &bufSize, int64_t &numFiles) {
  ByteRange br = makeByteRange(src, max, off);  // will check for off>0 max>0
  const ByteRange obr = br;
  bool ok = decodeInt64FixedLength(br, bufSize) &&
            decodeInt64FixedLength(br, numFiles);
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeChunkInfo(char *dest, int64_t &off, int64_t max,
                               const Interval &chunk) {
  return encodeVarI64C(dest, max, off, chunk.start_) &&
         encodeVarI64C(dest, max, off, chunk.end_);
}

bool Protocol::decodeChunkInfo(ByteRange &br, Interval &chunk) {
  return decodeInt64C(br, chunk.start_) && decodeInt64C(br, chunk.end_);
}

bool Protocol::encodeFileChunksInfo(char *dest, int64_t &off, int64_t max,
                                    const FileChunksInfo &fileChunksInfo) {
  bool ok = encodeVarI64C(dest, max, off, fileChunksInfo.getSeqId()) &&
            encodeString(dest, max, off, fileChunksInfo.getFileName()) &&
            encodeVarI64C(dest, max, off, fileChunksInfo.getFileSize()) &&
            encodeVarI64C(dest, max, off, fileChunksInfo.getChunks().size());
  if (!ok) {
    return false;
  }
  for (const auto &chunk : fileChunksInfo.getChunks()) {
    if (!encodeChunkInfo(dest, off, max, chunk)) {
      return false;
    }
  }
  return true;
}

bool Protocol::decodeFileChunksInfo(ByteRange &br,
                                    FileChunksInfo &fileChunksInfo) {
  int64_t seqId, fileSize, numChunks;
  string fileName;
  bool ok = decodeInt64C(br, seqId) && decodeString(br, fileName) &&
            decodeInt64C(br, fileSize) && decodeInt64C(br, numChunks);
  if (!ok) {
    return false;
  }
  fileChunksInfo.setSeqId(seqId);
  fileChunksInfo.setFileName(fileName);
  fileChunksInfo.setFileSize(fileSize);
  if (numChunks < 0) {
    WLOG(ERROR) << "Negative number of chunks decoded " << numChunks;
    return false;
  }
  for (int64_t i = 0; i < numChunks; i++) {
    Interval chunk;
    if (!decodeChunkInfo(br, chunk)) {
      return false;
    }
    fileChunksInfo.addChunk(chunk);
  }
  return true;
}

int64_t Protocol::maxEncodeLen(const FileChunksInfo &fileChunkInfo) {
  return 10 + 2 + fileChunkInfo.getFileName().size() + 10 + 10 +
         fileChunkInfo.getChunks().size() * kMaxChunkEncodeLen;
}

int64_t Protocol::encodeFileChunksInfoList(
    char *dest, int64_t &off, int64_t bufSize, int64_t startIndex,
    const std::vector<FileChunksInfo> &fileChunksInfoList) {
  int64_t oldOffset = off;
  int64_t numEncoded = 0;
  const int64_t numFileChunks = fileChunksInfoList.size();
  for (int64_t i = startIndex; i < numFileChunks; i++) {
    const FileChunksInfo &fileChunksInfo = fileChunksInfoList[i];
    int64_t maxLength = maxEncodeLen(fileChunksInfo);
    if (maxLength + oldOffset > bufSize) {
      WLOG(WARNING) << "Chunk info for " << fileChunksInfo.getFileName()
                    << " can not be encoded in a buffer of size " << bufSize
                    << ", Ignoring.";
      continue;
    }
    if (maxLength + off >= bufSize) {
      break;
    }
    encodeFileChunksInfo(dest, off, bufSize, fileChunksInfo);
    numEncoded++;
  }
  return numEncoded;
}

bool Protocol::decodeFileChunksInfoList(
    char *src, int64_t &off, int64_t dataSize,
    std::vector<FileChunksInfo> &fileChunksInfoList) {
  ByteRange br = makeByteRange(src, dataSize, off);
  const ByteRange obr = br;
  while (!br.empty()) {
    FileChunksInfo fileChunkInfo;
    if (!decodeFileChunksInfo(br, fileChunkInfo)) {
      return false;
    }
    fileChunksInfoList.emplace_back(std::move(fileChunkInfo));
  }
  off += offset(br, obr);
  return true;
}

bool Protocol::encodeSettings(int senderProtocolVersion, char *dest,
                              int64_t &off, int64_t max,
                              const Settings &settings) {
  bool ok = encodeVarI64C(dest, max, off, senderProtocolVersion) &&
            encodeVarI64C(dest, max, off, settings.readTimeoutMillis) &&
            encodeVarI64C(dest, max, off, settings.writeTimeoutMillis) &&
            encodeString(dest, max, off, settings.transferId);
  if (ok && senderProtocolVersion >= SETTINGS_FLAG_VERSION) {
    uint8_t flags = 0;
    if (settings.enableChecksum) {
      flags |= 1;
    }
    if (settings.sendFileChunks) {
      flags |= (1 << 1);
    }
    if (settings.blockModeDisabled) {
      flags |= (1 << 2);
    }
    if (settings.enableHeartBeat) {
      flags |= (1 << 3);
    }
    if (off >= max) {
      return false;
    }
    dest[off++] = flags;
  }
  return ok;
}

bool Protocol::decodeVersion(char *src, int64_t &off, int64_t max,
                             int &senderProtocolVersion) {
  ByteRange br = makeByteRange(src, max, off);
  const ByteRange obr = br;
  bool ok = decodeInt32C(br, senderProtocolVersion);
  off += offset(br, obr);
  return ok;
}

bool Protocol::decodeSettings(int protocolVersion, char *src, int64_t &off,
                              int64_t max, Settings &settings) {
  settings.enableChecksum = settings.sendFileChunks = false;
  if (off < 0) {
    WLOG(ERROR) << "Invalid negative start offset for decodeSettings " << off;
    return false;
  }
  if (off >= max) {
    WLOG(ERROR) << "Invalid start offset at the end for decodeSettings " << off;
    return false;
  }
  ByteRange br = makeByteRange(src, max, off);
  const ByteRange obr = br;
  bool ok = decodeInt32C(br, settings.readTimeoutMillis) &&
            decodeInt32C(br, settings.writeTimeoutMillis) &&
            decodeString(br, settings.transferId);
  if (ok && protocolVersion >= SETTINGS_FLAG_VERSION) {
    if (br.empty()) {
      return false;
    }
    uint8_t flags = br.front();
    settings.enableChecksum = flags & 1;
    settings.sendFileChunks = flags & (1 << 1);
    settings.blockModeDisabled = flags & (1 << 2);
    settings.enableHeartBeat = flags & (1 << 3);
    br.pop_front();
  }
  off += offset(br, obr);
  return ok;
}

/* static */
bool Protocol::encodeEncryptionSettings(char *dest, int64_t &off, int64_t max,
                                        const EncryptionType encryptionType,
                                        const string &iv,
                                        const int32_t tagInterval) {
  return encodeVarI64C(dest, max, off, encryptionType) &&
         encodeString(dest, max, off, iv) &&
         encodeInt32FixedLength(dest, max, off, tagInterval);
}

/* static */
bool Protocol::decodeEncryptionSettings(char *src, int64_t &off, int64_t max,
                                        EncryptionType &encryptionType,
                                        string &iv, int32_t &tagInterval) {
  ByteRange br = makeByteRange(src, max, off);
  const ByteRange obr = br;
  int64_t v;
  bool ok = decodeInt64C(br, v) && decodeString(br, iv) &&
            decodeInt32FixedLength(br, tagInterval);
  if (ok) {
    encryptionType = static_cast<EncryptionType>(v);
  }
  off += offset(br, obr);
  return ok;
}

bool Protocol::encodeFooter(char *dest, int64_t &off, int64_t max,
                            int32_t checksum) {
  return encodeVarI64(dest, max, off, checksum);
}

bool Protocol::decodeFooter(char *src, int64_t &off, int64_t max,
                            int32_t &checksum) {
  ByteRange br = makeByteRange(src, max, off);
  const ByteRange obr = br;
  bool ok = decodeInt32(br, checksum);
  off += offset(br, obr);
  return ok;
}
}
}
