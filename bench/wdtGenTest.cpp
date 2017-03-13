/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
// #include <folly/Benchmark.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "Bigram.h"

using std::string;

// BENCHMARK(copy)

#if !defined(NDEBUG)
TEST(Bigram, BadAccess) {
  Bigram b('a', 'b');
  const string assertRegex("Assertion `idx >= 0 && idx < 2' failed");
  EXPECT_DEATH(b[-1], assertRegex);
  EXPECT_DEATH(b[2], assertRegex);
}
#endif

TEST(Bigram, GoodAccess) {
  Bigram b('a', 'b');
  EXPECT_EQ('b', b[1]);
  EXPECT_EQ('a', b[0]);
}

TEST(Bigram, BasicEqNeq) {
  Bigram b1('a', 'b');
  Bigram b2("ab");
  EXPECT_EQ(b1, b2);
  EXPECT_EQ(sizeof(b1), 2);  // if Bigram gets virtual this changes
  Bigram ba[3]{"ab", "ac", "db"};
  EXPECT_EQ(sizeof(ba), 3 * 2);  // check they are packable with char alignment
  EXPECT_EQ(b2, ba[0]);
  EXPECT_NE(b2, ba[1]);
  EXPECT_NE(b2, ba[2]);
  Bigram b3 = b1;
  EXPECT_EQ(b3, b2);
  Bigram b4(b1);
  EXPECT_EQ(b4, b2);
  EXPECT_LT(ba[0], ba[1]);
  EXPECT_LT(ba[0], ba[2]);
  EXPECT_FALSE(ba[1] < ba[0]);
  EXPECT_FALSE(ba[2] < ba[0]);
}

TEST(Bigram, ToString) {
  Bigram b1(0, '"');
  Bigram b2('\\', 0x1b);
  EXPECT_EQ(b1.toPrintableString(), "{\"\\00\\\"\"}");
  EXPECT_EQ(b2.toPrintableString(), "{\"\\\\\\033\"}");
  string res("abc");
  Bigram b3("de");
  b3.toPrintableString(res);
  EXPECT_EQ(res, "abc{\"de\"}");
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  //  if (FLAGS_benchmark) {
  //    folly::runBenchmarks();
  //  }
  return RUN_ALL_TESTS();
}
