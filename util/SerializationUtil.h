/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <folly/Range.h>
#include <wdt/ErrorCodes.h>
#include <string>

namespace facebook {
namespace wdt {

/// make a byterange from ptr size and offset (byte range starts at
/// dest + offset and ends at dest + size)
folly::ByteRange makeByteRange(char *dest, int64_t sz, int64_t off = 0);
/// make a byterange from a string
folly::ByteRange makeByteRange(std::string);
/// Delta between 2 byteranges (assumes newRange was advanced and we want to
/// know the offset change)
int64_t offset(const folly::ByteRange &newR, const folly::ByteRange &oldR);

/// decodes from br and consumes (advances the byte range)
/// returns value in the result param and returns true
/// if the varint is malformed (would read past the end of br), returns false
bool decodeInt64(folly::ByteRange &br, int64_t &result);
/// will also return false if the number decoded doesn't fit in an int32_t
bool decodeInt32(folly::ByteRange &br, int32_t &result);
/// Unsigned (not zigzaged encoded version)
bool decodeUInt64(folly::ByteRange &br, uint64_t &res);
/// Unsigned but result in a signed - @see encodeVarI64C
bool decodeInt64C(folly::ByteRange &br, int64_t &res);
/// Encoded with encodeVarI64C (aka as unsigned) and fits in 32 bits
bool decodeInt32C(folly::ByteRange &br, int32_t &result);
/// Decodes fixed length int16
bool decodeInt16FixedLength(folly::ByteRange &br, int16_t &res);
/// Decodes fixed length int32
bool decodeInt32FixedLength(folly::ByteRange &br, int32_t &res);
/// Decodes fixed length int64
bool decodeInt64FixedLength(folly::ByteRange &br, int64_t &res);
/// Encodes fixed length int16
bool encodeInt16FixedLength(char *dest, int64_t sz, int64_t &off, int16_t val);
/// Encodes fixed length int32
bool encodeInt32FixedLength(char *dest, int64_t sz, int64_t &off, int32_t val);
/// Encodes fixed length int64
bool encodeInt64FixedLength(char *dest, int64_t sz, int64_t &off, int64_t val);

/// encodes str into dest + off; not writing past dest + sz
/// moves the off into the dest pointer, returns true if successful, false if
/// off would need to be moved past sz
bool encodeString(char *dest, int64_t sz, int64_t &off, const std::string &str);

/// decodes from br and consumes/moves off
/// sets str
/// @return false if there isn't enough data in br to decode the length or
///         the string
bool decodeString(folly::ByteRange &br, std::string &str);

/// ------- Encoding (varint) building blocks:  ---------

// ZigZag is originally from folly/Varint.h :
inline uint64_t encodeZigZag(int64_t val) {
  // Bit-twiddling magic stolen from the Google protocol buffer document;
  // val >> 63 is an arithmetic shift because val is signed
  auto uval = static_cast<uint64_t>(val);
  return (uval << 1) ^ static_cast<uint64_t>(val >> 63);
}

inline int64_t decodeZigZag(uint64_t val) {
  return static_cast<int64_t>((val >> 1) ^ -(val & 1));
}

// Rest is laurent/wormhole/wdt's version:

// checks don't incur performance cost so leave them in
#ifndef WDT_EDI64_DO_CHECKS
#define WDT_EDI64_DO_CHECKS 1
#endif

/**
 * Unsigned version of @see encodeVarI64 - if you are sure you values
 * are unsigned, use this version and save 1 bit. The ranges beccome
 * [0 - 128] -> 1 byte, [128 - 16384] -> 2 bytes, etc...
 */
inline size_t encodeVarU64(std::string &buffer, uint64_t v) {
  size_t count = 0;
  while ((++count < 9) && (v >= 128)) {
    buffer.push_back(static_cast<char>(0x80 | (v & 0x7f)));
    v >>= 7;
  }
  buffer.push_back(static_cast<char>(v));
  return count;
}

/**
 * Encodes a signed 64 bit integer into a variable length format - taking
 * from 1 byte up to 9 bytes; 1 byte for [-64,63], 2 bytes for [-8192,8191]
 * 3 bytes for [-1048576, 1048575] etc up to 9 bytes (1 continuation bit
 * is used for first 8 bytes if needed - last byte if in use all 8 bits are
 * used, unlike protocol buffers/thrift serialization which would yield 10
 * bytes in the worse case)
 *
 * std::string version - see next one for pointer version
 */
inline size_t encodeVarI64(std::string &buffer, int64_t i64) {
  return encodeVarU64(buffer, encodeZigZag(i64));
}

/**
 * Unsigned buffer based version of @see encodeVarI64
 */
inline bool encodeVarU64(char *data, size_t datasz, int64_t &pos, uint64_t v) {
#if WDT_EDI64_DO_CHECKS
  if (pos < 0) {
    return false;
  }
#endif
  char *p = data + pos;
  char *const end = data + datasz;
  int count = 0;
  while ((++count < 9) && (v >= 128)) {
#if WDT_EDI64_DO_CHECKS
    if (p >= end) {
      WLOG(WARNING) << "not enough space to store full value";
      return false;
    }
#endif
    *p++ = static_cast<char>(0x80 | (v & 0x7f));
    v >>= 7;
  }
#if WDT_EDI64_DO_CHECKS
  if (p >= end) {
    WLOG(WARNING) << "not enough space to store full value";
    return false;
  }
#endif
  *p++ = static_cast<char>(v);
  pos += count;
  return true;
}

/**
 * Encodes a signed 64 bit integer into a variable length format - taking
 * from 1 byte up to 9 bytes; 1 byte for [-64,63], 2 bytes for [-8192,8191]
 * 3 bytes for [-1048576, 1048575] etc up to 9 bytes (1 continuation bit
 * is used for first 8 bytes if needed - last byte if in use all 8 bits are
 * used, unlike protocol buffers/thrift serialization which would yield 10
 * bytes in the worse case)
 *
 * @return true if ok; false for errors (if the buffer is too short)
 * @param datalen  to be sure all value fit, must be at least pos+9 (but at
 *                 minimum pos+n, n>=1 if data fits in pos+n)
 */
inline bool encodeVarI64(char *data, size_t datalen, int64_t &pos, int64_t sv) {
  return encodeVarU64(data, datalen, pos, encodeZigZag(sv));
}

/**
 * Decodes a variable length unsigned int64
 *
 * @param  data    pointer to a buffer
 * @param  datalen number of valid bytes in the buffer pointed to by datat
 * @param  pos     where to start reading (data+pos) ; pos < datalen as input
 *                 pos is updated to next byte after the read varint
 * @param  res     result (decoded int64)
 * @return true if successful, false if an error occurred (bad/short data)
 * Example of use reading from string buffer; offset is updated:
 * uint64_t result;
 * decodeVar64(buffer.data(), buffer.length(), offset, result);
 */
inline bool decodeVarU64(const char *data, size_t datalen, int64_t &pos,
                         uint64_t &res) {
#if WDT_EDI64_DO_CHECKS
  if (pos < 0) {
    WLOG(WARNING) << "negative writing offset " << pos;
    return false;
  }
#endif
  const char *p = data + pos;
  const char *const end = data + datalen;
  uint64_t val = 0;
  int shift = 0;
  int count = 0;
#if WDT_EDI64_DO_CHECKS
  if (p >= end) {
    WLOG(WARNING) << "not enough space to store full value at start, l="
                  << datalen << " p=" << pos;
    return false;
  }
#endif
  while ((++count < 9) && (*p & 0x80)) {
    val |= static_cast<uint64_t>(*p++ & 0x7f) << shift;
    shift += 7;
#if WDT_EDI64_DO_CHECKS
    if (p >= end) {
      WLOG(WARNING) << "not enough space to store full value l=" << datalen;
      return false;
    }
#endif
  }
  val |= static_cast<uint64_t>(*p++) << shift;
  res = val;
  pos = p - data;
  return true;
}

/// This is necessary because wdt 1.x encodes signed value without zigzag
/// (ie potentially 10 bytes for small negative values) - to preserve backward
/// compatibility:
inline bool encodeVarI64C(char *data, size_t datasz, int64_t &pos, int64_t v) {
#if WDT_EDI64_DO_CHECKS
  WDT_CHECK_GE(v, 0);  // to crash/find bugs/cases where we use negative values
#endif
  return encodeVarU64(data, datasz, pos, static_cast<uint64_t>(v));
}

/**
 * Decodes a variable length signed int64
 *
 * @param  data    pointer to a buffer
 * @param  datalen number of valid bytes in the buffer pointed to by datat
 * @param  pos     where to start reading (data+pos) ; pos < datalen as input
 *                 pos is updated to next byte after the read varint
 * @param  res     result (decoded int64)
 * @return true if successful, false if an error occurred (bad/short data)
 * Example of use reading from string buffer; offset is updated:
 * int64_t result;
 * decodeVarI64(buffer.data(), buffer.length(), offset, result);
 */
inline bool decodeVarI64(const char *data, size_t datalen, int64_t &pos,
                         int64_t &res) {
  uint64_t v;
  bool ok = decodeVarU64(data, datalen, pos, v);
  if (ok) {
    res = decodeZigZag(v);
  }
  return ok;
}
}
}
