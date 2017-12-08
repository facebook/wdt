/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/SerializationUtil.h>

#include <folly/lang/Bits.h>

using folly::ByteRange;
using std::string;

namespace facebook {
namespace wdt {

ByteRange makeByteRange(string str) {
  return ByteRange((uint8_t *)str.data(), str.size());
}

ByteRange makeByteRange(char *dest, int64_t sz, int64_t off) {
  WDT_CHECK_GE(off, 0);
  WDT_CHECK_GE(sz, 0);
  WDT_CHECK(dest != nullptr);
  return ByteRange((uint8_t *)(dest + off), sz - off);
}

int64_t offset(const folly::ByteRange &newRange,
               const folly::ByteRange &oldRange) {
  WDT_CHECK_EQ(newRange.end(), oldRange.end());
  return newRange.start() - oldRange.start();
}

bool decodeInt32(ByteRange &br, int32_t &res32) {
  int64_t res64;
  ByteRange obr = br;
  bool ok = decodeInt64(br, res64);
  if (!ok) {
    return false;
  }
  if (res64 > INT32_MAX || res64 < INT32_MIN) {
    WLOG(ERROR) << "var int32 decoded value " << res64
                << " does not fit in a 32 bit number as expected";
    br = obr;
    return false;
  }
  res32 = static_cast<int32_t>(res64);
  return true;
}

bool decodeInt64(ByteRange &br, int64_t &res) {
  int64_t pos = 0;
  bool ret = decodeVarI64((const char *)(br.start()), br.size(), pos, res);
  if (!ret) {
    return false;
  }
  WDT_CHECK_GE(pos, 1);
  br.advance(pos);
  return true;
}

bool decodeUInt64(ByteRange &br, uint64_t &res) {
  int64_t pos = 0;
  bool ret = decodeVarU64((const char *)(br.start()), br.size(), pos, res);
  if (!ret) {
    return false;
  }
  WDT_CHECK_GE(pos, 1);
  br.advance(pos);
  return true;
}

bool decodeInt64C(ByteRange &br, int64_t &sres) {
  uint64_t ures;
  bool ret = decodeUInt64(br, ures);
  if (!ret) {
    return false;
  }
  if (ures > INT64_MAX) {
    WLOG(ERROR) << "Decoded as unsigned into signed, too large " << ures;
    return false;
  }
  sres = ures;
  return true;
}

bool decodeInt32C(ByteRange &br, int32_t &res32) {
  int64_t res64;
  ByteRange obr = br;
  bool ok = decodeInt64C(br, res64);
  if (!ok) {
    return false;
  }
  if (res64 > INT32_MAX || res64 < 0) {
    WLOG(ERROR) << "var int32 decoded value " << res64
                << " does not fit in a 31 bit positive number as expected";
    br = obr;
    return false;
  }
  res32 = static_cast<int32_t>(res64);
  return true;
}

template <typename T>
bool decodeIntFixedLength(folly::ByteRange &br, T &res) {
  if (br.size() < sizeof(T)) {
    WLOG(ERROR) << "Not enough to read to decode fixed length encoded int";
    return false;
  }
  res = folly::loadUnaligned<T>(br.start());
  res = folly::Endian::little(res);
  br.advance(sizeof(T));
  if (res < 0) {
    WLOG(ERROR) << "negative int decoded " << res;
    return false;
  }
  return true;
}

bool decodeInt16FixedLength(folly::ByteRange &br, int16_t &res) {
  bool success = decodeIntFixedLength<int16_t>(br, res);
  return success;
}

bool decodeInt32FixedLength(folly::ByteRange &br, int32_t &res) {
  return decodeIntFixedLength<int32_t>(br, res);
}

bool decodeInt64FixedLength(folly::ByteRange &br, int64_t &res) {
  return decodeIntFixedLength<int64_t>(br, res);
}

template <typename T>
bool encodeIntFixedLength(char *dest, int64_t sz, int64_t &off, const T val) {
  constexpr int intLen = sizeof(T);
  if (off + intLen > sz) {
    WLOG(ERROR) << "Not enough room to encode fixed length int " << val
                << " off: " << off << " buffer size: " << sz;
    return false;
  }
  folly::storeUnaligned<T>(dest + off, folly::Endian::little(val));
  off += intLen;
  return true;
}

bool encodeInt16FixedLength(char *dest, int64_t sz, int64_t &off, int16_t val) {
  return encodeIntFixedLength<int16_t>(dest, sz, off, val);
}

bool encodeInt32FixedLength(char *dest, int64_t sz, int64_t &off, int32_t val) {
  return encodeIntFixedLength<int32_t>(dest, sz, off, val);
}

bool encodeInt64FixedLength(char *dest, int64_t sz, int64_t &off, int64_t val) {
  return encodeIntFixedLength<int64_t>(dest, sz, off, val);
}

bool encodeString(char *dest, int64_t sz, int64_t &off, const string &str) {
  if (!encodeVarU64(dest, sz, off, str.length())) {
    return false;
  }
  const int64_t strLen = str.length();
  if ((off + strLen) > sz) {
    WLOG(ERROR) << "Not enough room to encode \"" << str << "\" in buf of size "
                << sz;
    return false;
  }
  memcpy(dest + off, str.data(), strLen);
  off += strLen;
  return true;
}

bool decodeString(ByteRange &br, string &str) {
  uint64_t strLen;
  if (!decodeUInt64(br, strLen)) {
    return false;
  }
  if (strLen > br.size()) {
    WLOG(ERROR) << "Not enough room with " << br.size() << " to decode "
                << strLen;
    return false;
  }
  str.assign((const char *)(br.start()), strLen);
  br.advance(strLen);
  return true;
}
}
}
