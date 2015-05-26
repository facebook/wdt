#include "Protocol.h"

#include "ErrorCodes.h"
#include "WdtOptions.h"

#include "folly/Range.h"
#include "folly/String.h"  // exceptionStr
#include "folly/Varint.h"
#include <algorithm>

using namespace facebook::wdt;

const int Protocol::protocol_version = 11;

int Protocol::negotiateProtocol(int requestedProtocolVersion) {
  if (requestedProtocolVersion < 10) {
    LOG(WARNING) << "Can not handle protocol " << requestedProtocolVersion;
    return 0;
  }
  return std::min<int>(protocol_version, requestedProtocolVersion);
}

/* static */
bool Protocol::encodeHeader(char *dest, size_t &off, size_t max, std::string id,
                            uint64_t seqId, int64_t size, int64_t offset,
                            int64_t fileSize) {
  // TODO: add version and/or magic number
  size_t idLen = id.size();
  off += folly::encodeVarint(idLen, (uint8_t *)dest + off);
  memcpy(dest + off, id.data(), idLen);
  off += idLen;
  off += folly::encodeVarint(seqId, (uint8_t *)dest + off);
  off += folly::encodeVarint(size, (uint8_t *)dest + off);
  off += folly::encodeVarint(offset, (uint8_t *)dest + off);
  off += folly::encodeVarint(fileSize, (uint8_t *)dest + off);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
  return true;
}

bool Protocol::decodeHeader(char *src, size_t &off, size_t max, std::string &id,
                            uint64_t &seqId, int64_t &size, int64_t &offset,
                            int64_t &fileSize) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  size_t idLen = folly::decodeVarint(br);
  if (idLen + off + 1 >= max) {
    LOG(ERROR) << "Not enough room with " << max << " to decode " << idLen
               << " at " << off;
    return false;
  }
  id.assign((const char *)(br.start()), idLen);
  br.advance(idLen);
  try {
    seqId = folly::decodeVarint(br);
    size = folly::decodeVarint(br);
    offset = folly::decodeVarint(br);
    fileSize = folly::decodeVarint(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}

bool Protocol::encodeCheckpoints(char *dest, size_t &off, size_t max,
                                 const std::vector<Checkpoint> &checkpoints) {
  off += folly::encodeVarint(checkpoints.size(), (uint8_t *)dest + off);
  for (auto &checkpoint : checkpoints) {
    off += folly::encodeVarint(checkpoint.first, (uint8_t *)dest + off);
    off += folly::encodeVarint(checkpoint.second, (uint8_t *)dest + off);
  }
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
  return true;
}

bool Protocol::decodeCheckpoints(char *src, size_t &off, size_t max,
                                 std::vector<Checkpoint> &checkpoints) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    uint64_t len = folly::decodeVarint(br);
    for (int i = 0; i < len; i++) {
      uint16_t port = folly::decodeVarint(br);
      uint64_t numReceivedSources = folly::decodeVarint(br);
      off = br.start() - (uint8_t *)src;
      if (off > max) {
        LOG(ERROR) << "Read past the end:" << off << " " << max;
        return false;
      }
      checkpoints.emplace_back(port, numReceivedSources);
    }
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  return true;
}

bool Protocol::encodeDone(char *dest, size_t &off, size_t max,
                          int64_t numBlocks) {
  off += folly::encodeVarint(numBlocks, (uint8_t *)dest + off);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
  return true;
}

bool Protocol::decodeDone(char *src, size_t &off, size_t max,
                          int64_t &numBlocks) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    numBlocks = folly::decodeVarint(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}

bool Protocol::encodeSize(char *dest, size_t &off, size_t max,
                          int64_t totalNumBytes) {
  off += folly::encodeVarint(totalNumBytes, (uint8_t *)dest + off);
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
  return true;
}

bool Protocol::decodeSize(char *src, size_t &off, size_t max,
                          int64_t &totalNumBytes) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    totalNumBytes = folly::decodeVarint(br);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}

bool Protocol::encodeSettings(char *dest, size_t &off, size_t max,
                              int32_t protocolVersion,
                              int64_t readTimeoutMillis,
                              int64_t writeTimeoutMillis,
                              const std::string &senderId) {
  off += folly::encodeVarint(protocolVersion, (uint8_t *)dest + off);
  off += folly::encodeVarint(readTimeoutMillis, (uint8_t *)dest + off);
  off += folly::encodeVarint(writeTimeoutMillis, (uint8_t *)dest + off);
  size_t idLen = senderId.size();
  off += folly::encodeVarint(idLen, (uint8_t *)dest + off);
  memcpy(dest + off, senderId.data(), idLen);
  off += idLen;
  WDT_CHECK(off <= max) << "Memory corruption:" << off << " " << max;
  return true;
}

bool Protocol::decodeSettings(char *src, size_t &off, size_t max,
                              int32_t &protocolVersion,
                              int64_t &readTimeoutMillis,
                              int64_t &writeTimeoutMillis,
                              std::string &senderId) {
  folly::ByteRange br((uint8_t *)(src + off), max);
  try {
    protocolVersion = folly::decodeVarint(br);
    readTimeoutMillis = folly::decodeVarint(br);
    writeTimeoutMillis = folly::decodeVarint(br);
    size_t idLen = folly::decodeVarint(br);
    senderId.assign((const char *)(br.start()), idLen);
    br.advance(idLen);
  } catch (const std::exception &ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t *)src;
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}

bool Protocol::isReceiverProgressReportingSupported(int protocolVersion) {
  return protocolVersion >= 11;
}
