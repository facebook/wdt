#include "Protocol.h"

#include "folly/Range.h"
#include "folly/String.h" // exceptionStr
#include "folly/Varint.h"

using namespace facebook::wdt;

/* static */
bool Protocol::encode(
  char* dest, size_t& off, size_t max, std::string id, int64_t size) {
  // TODO: add version and/or magic number
  size_t idLen = id.size();
  off += folly::encodeVarint(idLen, (uint8_t*)dest + off);
  memcpy(dest + off, id.data(), idLen);
  off += idLen;
  off += folly::encodeVarint(size, (uint8_t*)dest + off);
  if (off > max) {
    LOG(FATAL) << "Memory corruption:" << off << " " << max;
    return false;
  }
  return true;
}

bool Protocol::decode(
  char* src, size_t& off, size_t max, std::string& id, int64_t& size) {
  folly::ByteRange br((uint8_t*)(src + off), max);
  size_t idLen = folly::decodeVarint(br);
  if (idLen + off + 1 >= max) {
    LOG(ERROR) << "Not enough room with " << max << " to decode " << idLen
               << " at " << off;
    return false;
  }
  id.assign((const char*)(br.start()), idLen);
  br.advance(idLen);
  try {
    size = folly::decodeVarint(br);
  }
  catch (const std::exception& ex) {
    LOG(ERROR) << "got exception " << folly::exceptionStr(ex);
    return false;
  }
  off = br.start() - (uint8_t*)src;
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
};
