#include "SerializationUtil.h"
#include <folly/Varint.h>

namespace facebook {
namespace wdt {
void encodeInt(char *dest, size_t &off, int64_t value) {
  off += folly::encodeVarint(value, (uint8_t *)dest + off);
}

int64_t decodeInt(folly::ByteRange &br) {
  return folly::decodeVarint(br);
}

void encodeString(char *dest, size_t &off, const std::string &str) {
  size_t strLen = str.length();
  off += folly::encodeVarint(strLen, (uint8_t *)dest + off);
  memcpy(dest + off, str.data(), strLen);
  off += strLen;
}

bool decodeString(folly::ByteRange &br, char *src, size_t max,
                  std::string &str) {
  size_t strLen = folly::decodeVarint(br);
  size_t off = br.start() - (uint8_t *)src;
  if (off + strLen > max) {
    LOG(ERROR) << "Not enough room with " << max << " to decode " << strLen
               << " at " << off;
    return false;
  }
  str.assign((const char *)(br.start()), strLen);
  br.advance(strLen);
  return true;
}

bool checkForOverflow(size_t off, size_t max) {
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}
}
}
