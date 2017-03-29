/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/**
* Functional tests for varint encoding/decoding
*
* @author ldemailly
*/
#include <wdt/util/SerializationUtil.h>

#include "TestCommon.h"

using std::string;

namespace facebook {
namespace wdt {

/* when testing with old version (10 bytes)
enc:
  char *p = data + pos;
  facebook::Varint::encode(facebook::ZigZag::encode(i64), &p);
  pos = p - data;
dec:
  const char *p = data + pos;
  res = facebook::ZigZag::decode(facebook::Varint::decode(&p, datalen-pos));
  pos = p - data;
#define EDI64_10
 */

#ifdef EDI64_10
#define EDI64_MAX 10
#else
#define EDI64_MAX 9
#endif

void testZigZag(int64_t v) {
  uint64_t u = encodeZigZag(v);
  int64_t x = decodeZigZag(u);
  VLOG(2) << "v " << v << " -> " << u << " -> " << x;
  EXPECT_EQ(x, v);
}

void testEDI64(size_t numBytes, int64_t value) {
  VLOG(1) << "testing " << value << " fitting into " << numBytes;
  testZigZag(value);
  // string version
  string buf;
  EXPECT_EQ(encodeVarI64(buf, value), numBytes);
  EXPECT_EQ(buf.length(), numBytes);
  int64_t x;
  int64_t pos = 0;
  EXPECT_TRUE(decodeVarI64(buf.data(), buf.length(), pos, x));
  EXPECT_EQ(pos, numBytes);
  EXPECT_EQ(x, value);
  // buffer version
  const size_t offset = 23;  // not aligned
  char tmp[offset + EDI64_MAX];
  pos = offset;
  EXPECT_TRUE(encodeVarI64(tmp, sizeof(tmp), pos, value));
  EXPECT_EQ(pos - offset, numBytes);
  x = ~value;
  pos = offset;
  EXPECT_TRUE(decodeVarI64(tmp, sizeof(tmp), pos, x));
  int64_t delta = pos - offset;
  EXPECT_EQ(delta, numBytes);
  EXPECT_EQ(x, value);
}

// run with -v=1 at least once to check the numbers printed to match
// (some compiler can do funny things to large constants)
TEST(encDecI64, encodeDecodeI64Boundaries) {
  testEDI64(5, 2147483647);  // max 32 bits signed
  testEDI64(5, -2147483647);
  testEDI64(5, 2147483648LL);  // 33 bits signed
  testEDI64(5, -2147483648);   // min 32 bits signed
  testEDI64(5, 4294967295LL);  // max unsigned int
  testEDI64(5, -4294967295LL);
  testEDI64(5, 4294967296LL);
  testEDI64(5, -4294967296LL);
  testEDI64(5, 4294967297LL);
  testEDI64(5, -4294967297LL);
  // compiler is odd ULL needed somehow
  testEDI64(EDI64_MAX, -9223372036854775808ULL);  // min int64
  testEDI64(EDI64_MAX, -9223372036854775807LL);
  testEDI64(EDI64_MAX, -9223372036854775806LL);
  testEDI64(EDI64_MAX, 9223372036854775806LL);
  testEDI64(EDI64_MAX, 9223372036854775807LL);  // max int64
}

TEST(encDecI64, encodeDecodeI64Random) {
  char tmp[EDI64_MAX];
  for (int i = 0; i < 1000000; ++i) {
    int64_t v = rand64();
    int64_t pos1 = 0;
    EXPECT_TRUE(encodeVarI64(tmp, sizeof(tmp), pos1, v));
    EXPECT_TRUE(pos1 >= 1 && pos1 <= EDI64_MAX);  // will be 7-9 actually...
    int64_t x = 12345;
    int64_t pos2 = 0;
    EXPECT_TRUE(decodeVarI64(tmp, sizeof(tmp), pos2, x));
    EXPECT_EQ(pos1, pos2);
    EXPECT_EQ(v, x);
  }
}

TEST(encDecI64, encodeDecodeI64Errors) {
  const int64_t v1 = -33;
  const int64_t v2 = 513;
  const int64_t v4 = 1048576;  // first one that takes 4 bytes
  const size_t len = 1 + 2 + 4;
  char tmp[len];
  int64_t pos = 0;
  // normal encode/decode of more than 1 cycle:
  EXPECT_TRUE(encodeVarI64(tmp, len, pos, v2));
  EXPECT_EQ(pos, 2);
  EXPECT_TRUE(encodeVarI64(tmp, len, pos, v1));
  EXPECT_EQ(pos, 2 + 1);
  EXPECT_TRUE(encodeVarI64(tmp, len, pos, v4));
  EXPECT_EQ(pos, 2 + 1 + 4);
  int64_t x;
  pos = 0;
  EXPECT_TRUE(decodeVarI64(tmp, len, pos, x));
  EXPECT_EQ(pos, 2);
  EXPECT_EQ(x, v2);
  EXPECT_TRUE(decodeVarI64(tmp, len, pos, x));
  EXPECT_EQ(pos, 2 + 1);
  EXPECT_EQ(x, v1);
  EXPECT_TRUE(decodeVarI64(tmp, len, pos, x));
  EXPECT_EQ(pos, 2 + 1 + 4);
  EXPECT_EQ(x, v4);
#ifdef EDI64_DO_CHECKS
  // now past the end:
  LOG(WARNING) << "Expect 6 logged errors for remaining of this test";
  // decode errors
  pos = 2 + 1;
  x = 12345;
  EXPECT_FALSE(decodeVarI64(tmp, len - 1, pos, x));
  EXPECT_FALSE(decodeVarI64(tmp, pos - 1, pos, x));
  pos = 0;
  EXPECT_FALSE(decodeVarI64(tmp, 0, pos, x));
  // encode errors
  pos = 2 + 1;
  EXPECT_FALSE(encodeVarI64(tmp, len - 1, pos, v4));
  pos = 2 + 1;
  EXPECT_FALSE(encodeVarI64(tmp, len - 2, pos, v4));
  pos = len;
  EXPECT_FALSE(encodeVarI64(tmp, len, pos, v4));
#endif
}

TEST(encDecI64, encodeDecodeI64SmallRange) {
  // for small values let's check the whole range:
  // first row = 1 byte; 2nd row 2 bytes...
  int testRanges[6] = {// range first val, range last val
                       -64, 63, -8192, 8191, -1048576, 1048575};
  int prevA = 0, prevB = 0;
  for (int i = 0; i < 3; ++i) {
    size_t numB = i + 1;
    int a = testRanges[i * 2];
    int b = testRanges[i * 2 + 1];
    for (int j = a; j <= b; ++j) {
      // skip previous range
      if (i > 0 && (j == prevA)) {
        j = prevB;
        continue;
      }
      testEDI64(numB, j);
    }
    prevA = a;
    prevB = b;
  }
}
}
}  // end of namespace facebook::wdt
// -- main

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
