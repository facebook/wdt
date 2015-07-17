/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "SerializationUtil.h"
#include <folly/Varint.h>

namespace facebook {
namespace wdt {
void encodeInt(char *dest, int64_t &off, int64_t value) {
  off += folly::encodeVarint(value, (uint8_t *)dest + off);
}

int64_t decodeInt(folly::ByteRange &br) {
  return folly::decodeVarint(br);
}

void encodeString(char *dest, int64_t &off, const std::string &str) {
  int64_t strLen = str.length();
  off += folly::encodeVarint(strLen, (uint8_t *)dest + off);
  memcpy(dest + off, str.data(), strLen);
  off += strLen;
}

bool decodeString(folly::ByteRange &br, char *src, int64_t max,
                  std::string &str) {
  int64_t strLen = folly::decodeVarint(br);
  int64_t off = br.start() - (uint8_t *)src;
  if (off + strLen > max) {
    LOG(ERROR) << "Not enough room with " << max << " to decode " << strLen
               << " at " << off;
    return false;
  }
  str.assign((const char *)(br.start()), strLen);
  br.advance(strLen);
  return true;
}

bool checkForOverflow(int64_t off, int64_t max) {
  if (off > max) {
    LOG(ERROR) << "Read past the end:" << off << " " << max;
    return false;
  }
  return true;
}
}
}
