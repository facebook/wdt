/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/**
* Performance benchmark/tests for varint encoding/decoding
*
* @author ldemailly
*/
#include <wdt/util/SerializationUtil.h>

#include <common/encode/Coding.h>  // this won't work outside of fb
#include <folly/Benchmark.h>
#include "TestCommon.h"

using namespace facebook::wdt;

using std::string;

#ifdef EDI64_10
#define EDI64_MAX 10
#else
#define EDI64_MAX 9
#endif

void BM_encodeDecodeI64(int iters, int vin) {
  VLOG(1) << "called with " << iters << " " << vin;
  char tmp[EDI64_MAX];
  // use 0 to test max int64_t (9 bytes encoding)
  int64_t v = vin ? vin : 9223372036854775807LL;
  // string buf(9, '\0');
  for (int i = 0; i < iters; ++i) {
    ssize_t pos = 0;
    encodeVarI64(tmp, sizeof(tmp), pos, v);
    int64_t x = 12345;
    pos = 0;
    decodeVarI64(tmp, sizeof(tmp), pos, x);
    CHECK_EQ(x, v);
  }
}
void BM_encodeDecodeI64_str(int iters, int vin) {
  VLOG(1) << "called with " << iters << " " << vin;
  // use 0 to test max int64_t (9 bytes encoding)
  int64_t v = vin ? vin : 9223372036854775807LL;
  string buf(9, '\0');
  for (int i = 0; i < iters; ++i) {
    buf.clear();
    ssize_t pos = 0;
    encodeVarI64(buf, v);
    int64_t x = 12345;
    pos = 0;
    decodeVarI64(buf.data(), buf.length(), pos, x);
    CHECK_EQ(x, v);
  }
}

void BM_encodeDecodeI64_vi_str(int iters, int vin) {
  VLOG(1) << "called with " << iters << " " << vin;
  // use 0 to test max int64_t (9 bytes encoding)
  int64_t v = vin ? vin : 9223372036854775807LL;
  string buf(9, '\0');
  facebook::strings::StringByteSink bs(&buf);
  for (int i = 0; i < iters; ++i) {
    buf.clear();
    facebook::Varint::encodeToByteSink(facebook::ZigZag::encode(v), &bs);
    int64_t x = 12345;
    const char *pos2 = buf.data();
    x = facebook::ZigZag::decode(facebook::Varint::decode(&pos2, buf.length()));
    CHECK_EQ(x, v);
  }
}

void BM_encodeDecodeI64_vi(int iters, int vin) {
  VLOG(1) << "called with " << iters << " " << vin;
  char tmp[vin ? 5 : 10];
  // use 0 to test max int64_t (9 bytes encoding)
  int64_t v = vin ? vin : 9223372036854775807LL;
  for (int i = 0; i < iters; ++i) {
    char *pos = tmp;
    facebook::Varint::encode(facebook::ZigZag::encode(v), &pos);
    int64_t x = 12345;
    const char *pos2 = tmp;
    x = facebook::ZigZag::decode(facebook::Varint::decode(&pos2, sizeof(tmp)));
    CHECK_EQ(x, v);
  }
}

void BM_encodeDecodeI64_notvar(int iters, int vin) {
  VLOG(1) << "called with " << iters << " " << vin;
  char tmp[10];
  // use 0 to test max int64_t (9 bytes encoding)
  int64_t v = vin ? vin : 9223372036854775807LL;
  for (int i = 0; i < iters; ++i) {
    memcpy(tmp, &v, sizeof(v));
    int64_t x = 12345;
    memcpy(&x, tmp, sizeof(v));
    CHECK_EQ(x, v);
  }
}

BENCHMARK_PARAM(BM_encodeDecodeI64, 0);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64, m10k, -10000);
BENCHMARK_PARAM(BM_encodeDecodeI64, 10000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64, m1k, -1000);
BENCHMARK_PARAM(BM_encodeDecodeI64, 1000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64, m100, -100);
BENCHMARK_PARAM(BM_encodeDecodeI64, 100);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64, m10, -10);
BENCHMARK_PARAM(BM_encodeDecodeI64, 10);

BENCHMARK_PARAM(BM_encodeDecodeI64_vi, 0);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi, m10k, -10000);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi, 10000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi, m1k, -1000);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi, 1000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi, m100, -100);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi, 100);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi, m10, -10);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi, 10);

BENCHMARK_PARAM(BM_encodeDecodeI64_str, 0);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_str, m10k, -10000);
BENCHMARK_PARAM(BM_encodeDecodeI64_str, 10000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_str, m1k, -1000);
BENCHMARK_PARAM(BM_encodeDecodeI64_str, 1000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_str, m100, -100);
BENCHMARK_PARAM(BM_encodeDecodeI64_str, 100);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_str, m10, -10);
BENCHMARK_PARAM(BM_encodeDecodeI64_str, 10);

BENCHMARK_PARAM(BM_encodeDecodeI64_vi_str, 0);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi_str, m10k, -10000);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi_str, 10000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi_str, m1k, -1000);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi_str, 1000);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi_str, m100, -100);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi_str, 100);
BENCHMARK_NAMED_PARAM(BM_encodeDecodeI64_vi_str, m10, -10);
BENCHMARK_PARAM(BM_encodeDecodeI64_vi_str, 10);

// -- main

int main(int /* unused */, char ** /* unused */) {
  folly::runBenchmarks();
  return 0;
}
