/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Protocol.h"

#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "SerializationUtil.h"

#include <folly/String.h>  // exceptionStr
#include <algorithm>
#include <folly/Bits.h>

namespace facebook {
namespace wdt {

using std::string;

const int Protocol::protocol_version = WDT_PROTOCOL_VERSION;

const int Protocol::RECEIVER_PROGRESS_REPORT_VERSION = 11;
const int Protocol::CHECKSUM_VERSION = 12;
const int Protocol::DOWNLOAD_RESUMPTION_VERSION = 13;

const int Protocol::SETTINGS_FLAG_VERSION = 12;
const int Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION = 13;
const int Protocol::CHECKPOINT_OFFSET_VERSION = 16;
const int Protocol::CHECKPOINT_SEQ_ID_VERSION = 21;

const std::string Protocol::getFullVersion() {
  std::string fullVersion(WDT_VERSION_STR);
  fullVersion.append(" p ");
  fullVersion.append(std::to_string(protocol_version));
  return fullVersion;
}

int Protocol::negotiateProtocol(int requestedProtocolVersion,
                                int curProtocolVersion) {
  if (requestedProtocolVersion < 10) {
    LOG(WARNING) << "Can not handle protocol " << requestedProtocolVersion;
    return 0;
  }
  return std::min<int>(curProtocolVersion, requestedProtocolVersion);
}

std::ostream &operator<<(std::ostream &os, const Checkpoint &checkpoint) {
  os << "num-blocks: " << checkpoint.numBlocks
     << " seq-id: " << checkpoint.lastBlockSeqId
     << " block-offset: " << checkpoint.lastBlockOffset
     << " received-bytes: " << checkpoint.lastBlockReceivedBytes;
  return os;
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
  const int64_t numChunks = chunks_.size();
  for (int64_t i = 1; i < numChunks; i++) {
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

void Protocol::encodeHeader(int senderProtocolVersion, char *dest, int64_t &off,
                            int64_t max, const BlockDetails &blockDetails) {
  encodeString(dest, off, blockDetails.fileName);
  encodeInt(dest, off, blockDetails.seqId);
  encodeInt(dest, off, blockDetails.dataSize);
  encodeInt(dest, off, blockDetails.offset);
  encodeInt(dest, off, blockDetails.fileSize);
  if (senderProtocolVersion >= HEADER_FLAG_AND_PREV_SEQ_ID_VERSION) {
    uint8_t flags = blockDetails.allocationStatus;
    dest[off++] = flags;
    if (blockDetails.allocationStatus == EXISTS_TOO_SMALL ||
        blockDetails.allocationStatus == EXISTS_TOO_LARGE) {
      // prev seq-id is only used in case the size is less on the sender side
      encodeInt(dest, off, blockDetails.prevSeqId);
    }
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeHeader(int receiverProtocolVersion, char *src,
                            int64_t &off, int64_t max,
                            BlockDetails &blockDetails) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    if (!decodeString(br, src, max, blockDetails.fileName)) {
      return false;
    }
    blockDetails.seqId = decodeInt(br);
    blockDetails.dataSize = decodeInt(br);
    blockDetails.offset = decodeInt(br);
    blockDetails.fileSize = decodeInt(br);
    if (receiverProtocolVersion >= HEADER_FLAG_AND_PREV_SEQ_ID_VERSION) {
      if (!(br.size() >= 1)) {
        LOG(ERROR) << "Invalid (too short) input " << string(src + off, max);
        return false;
      }
      uint8_t flags = br.front();
      // first 2 bytes are used to represent allocation status
      blockDetails.allocationStatus = (FileAllocationStatus)(flags & 3);
      br.pop_front();
      if (blockDetails.allocationStatus == EXISTS_TOO_SMALL ||
          blockDetails.allocationStatus == EXISTS_TOO_LARGE) {
        blockDetails.prevSeqId = decodeInt(br);
      }
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

void Protocol::encodeCheckpoints(int protocolVersion, char *dest, int64_t &off,
                                 int64_t max,
                                 const std::vector<Checkpoint> &checkpoints) {
  encodeInt(dest, off, checkpoints.size());
  for (auto &checkpoint : checkpoints) {
    encodeInt(dest, off, checkpoint.port);
    encodeInt(dest, off, checkpoint.numBlocks);
    if (protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
      encodeInt(dest, off, checkpoint.lastBlockReceivedBytes);
    }
    if (protocolVersion >= CHECKPOINT_SEQ_ID_VERSION) {
      encodeInt(dest, off, checkpoint.lastBlockSeqId);
      encodeInt(dest, off, checkpoint.lastBlockOffset);
    }
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeCheckpoints(int protocolVersion, char *src, int64_t &off,
                                 int64_t max,
                                 std::vector<Checkpoint> &checkpoints) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    int64_t len;
    len = decodeInt(br);
    for (int64_t i = 0; i < len; i++) {
      Checkpoint checkpoint;
      checkpoint.port = decodeInt(br);
      checkpoint.numBlocks = decodeInt(br);
      if (protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
        checkpoint.lastBlockReceivedBytes = decodeInt(br);
      }
      if (protocolVersion >= CHECKPOINT_SEQ_ID_VERSION) {
        checkpoint.lastBlockSeqId = decodeInt(br);
        checkpoint.lastBlockOffset = decodeInt(br);
        checkpoint.hasSeqId = true;
      }
      off = br.start() - (uint8_t *)src;
      if (checkForOverflow(off, max)) {
        return false;
      }
      checkpoints.emplace_back(checkpoint);
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return true;
}

void Protocol::encodeDone(int protocolVersion, char *dest, int64_t &off,
                          int64_t max, int64_t numBlocks, int64_t bytesSent) {
  encodeInt(dest, off, numBlocks);
  if (protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
    encodeInt(dest, off, bytesSent);
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeDone(int protocolVersion, char *src, int64_t &off,
                          int64_t max, int64_t &numBlocks, int64_t &bytesSent) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    numBlocks = decodeInt(br);
    if (protocolVersion >= CHECKPOINT_OFFSET_VERSION) {
      bytesSent = decodeInt(br);
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

void Protocol::encodeSize(char *dest, int64_t &off, int64_t max,
                          int64_t totalNumBytes) {
  encodeInt(dest, off, totalNumBytes);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeSize(char *src, int64_t &off, int64_t max,
                          int64_t &totalNumBytes) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    totalNumBytes = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

void Protocol::encodeAbort(char *dest, int64_t &off, int32_t protocolVersion,
                           ErrorCode errCode, int64_t checkpoint) {
  folly::storeUnaligned<int32_t>(dest + off,
                                 folly::Endian::little(protocolVersion));
  off += sizeof(int32_t);
  dest[off++] = errCode;
  folly::storeUnaligned<int64_t>(dest + off, folly::Endian::little(checkpoint));
  off += sizeof(int64_t);
}

void Protocol::decodeAbort(char *src, int64_t &off, int32_t &protocolVersion,
                           ErrorCode &errCode, int64_t &checkpoint) {
  protocolVersion = folly::loadUnaligned<int32_t>(src + off);
  protocolVersion = folly::Endian::little(protocolVersion);
  off += sizeof(int32_t);
  errCode = (ErrorCode)src[off++];
  checkpoint = folly::loadUnaligned<int64_t>(src + off);
  checkpoint = folly::Endian::little(checkpoint);
  off += sizeof(int64_t);
}

void Protocol::encodeChunksCmd(char *dest, int64_t &off, int64_t bufSize,
                               int64_t numFiles) {
  folly::storeUnaligned<int64_t>(dest + off, folly::Endian::little(bufSize));
  off += sizeof(int64_t);
  folly::storeUnaligned<int64_t>(dest + off, folly::Endian::little(numFiles));
  off += sizeof(int64_t);
}

void Protocol::decodeChunksCmd(char *src, int64_t &off, int64_t &bufSize,
                               int64_t &numFiles) {
  bufSize = folly::loadUnaligned<int64_t>(src + off);
  bufSize = folly::Endian::little(bufSize);
  off += sizeof(int64_t);
  numFiles = folly::loadUnaligned<int64_t>(src + off);
  numFiles = folly::Endian::little(numFiles);
  off += sizeof(int64_t);
}

void Protocol::encodeChunkInfo(char *dest, int64_t &off, int64_t max,
                               const Interval &chunk) {
  encodeInt(dest, off, chunk.start_);
  encodeInt(dest, off, chunk.end_);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeChunkInfo(folly::ByteRange &br, char *src, int64_t max,
                               Interval &chunk) {
  try {
    chunk.start_ = decodeInt(br);
    chunk.end_ = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  int64_t off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

void Protocol::encodeFileChunksInfo(char *dest, int64_t &off, int64_t max,
                                    const FileChunksInfo &fileChunksInfo) {
  encodeInt(dest, off, fileChunksInfo.getSeqId());
  encodeString(dest, off, fileChunksInfo.getFileName());
  encodeInt(dest, off, fileChunksInfo.getFileSize());
  encodeInt(dest, off, fileChunksInfo.getChunks().size());
  for (const auto &chunk : fileChunksInfo.getChunks()) {
    encodeChunkInfo(dest, off, max, chunk);
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeFileChunksInfo(folly::ByteRange &br, char *src,
                                    int64_t max,
                                    FileChunksInfo &fileChunksInfo) {
  try {
    int64_t seqId, fileSize;
    string fileName;
    seqId = decodeInt(br);
    if (!decodeString(br, src, max, fileName)) {
      return false;
    }
    fileSize = decodeInt(br);
    fileChunksInfo.setSeqId(seqId);
    fileChunksInfo.setFileName(fileName);
    fileChunksInfo.setFileSize(fileSize);
    int64_t numChunks;
    numChunks = decodeInt(br);
    for (int64_t i = 0; i < numChunks; i++) {
      Interval chunk;
      if (!decodeChunkInfo(br, src, max, chunk)) {
        return false;
      }
      fileChunksInfo.addChunk(chunk);
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  int64_t off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
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
      LOG(WARNING) << "Chunk info for " << fileChunksInfo.getFileName()
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
  folly::ByteRange br((uint8_t *)(src + off), off + dataSize);
  while (!br.empty()) {
    FileChunksInfo fileChunkInfo;
    if (!decodeFileChunksInfo(br, src, off + dataSize, fileChunkInfo)) {
      return false;
    }
    fileChunksInfoList.emplace_back(std::move(fileChunkInfo));
  }
  off = br.start() - (uint8_t *)src;
  return true;
}

void Protocol::encodeSettings(int senderProtocolVersion, char *dest,
                              int64_t &off, int64_t max,
                              const Settings &settings) {
  encodeInt(dest, off, senderProtocolVersion);
  encodeInt(dest, off, settings.readTimeoutMillis);
  encodeInt(dest, off, settings.writeTimeoutMillis);
  encodeString(dest, off, settings.transferId);
  if (senderProtocolVersion >= SETTINGS_FLAG_VERSION) {
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
    dest[off++] = flags;
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeVersion(char *src, int64_t &off, int64_t max,
                             int &senderProtocolVersion) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    senderProtocolVersion = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return ERROR;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

bool Protocol::decodeSettings(int protocolVersion, char *src, int64_t &off,
                              int64_t max, Settings &settings) {
  settings.enableChecksum = settings.sendFileChunks = false;
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    settings.readTimeoutMillis = decodeInt(br);
    settings.writeTimeoutMillis = decodeInt(br);
    if (!decodeString(br, src, max, settings.transferId)) {
      return false;
    }
    if (protocolVersion >= SETTINGS_FLAG_VERSION) {
      uint8_t flags = br.front();
      settings.enableChecksum = flags & 1;
      settings.sendFileChunks = flags & (1 << 1);
      settings.blockModeDisabled = flags & (1 << 2);
      br.pop_front();
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}

void Protocol::encodeFooter(char *dest, int64_t &off, int64_t max,
                            int32_t checksum) {
  encodeInt(dest, off, checksum);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
}

bool Protocol::decodeFooter(char *src, int64_t &off, int64_t max,
                            int32_t &checksum) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    checksum = decodeInt(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  return !checkForOverflow(off, max);
}
}
}
