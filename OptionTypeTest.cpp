/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtFlags.h"
#include "WdtOptions.h"
#include <glog/logging.h>
#include <gtest/gtest.h>

#include "WdtFlagsMacros.h"

/*
 * Tests in this file can not be run in the same process. That is because we
 * rely on is_default property of gflags to determine whether a flag has been
 * specified by the user. And if a flag is ever changed, is_default becomes
 * false and can never be reset to true. So, after running one test, other
 * tests would fail. To run tests in this file, use wdt/wdt_option_type_test.sh.
 * That script runs each test in separate process and also tests for both long
 * and short flags.
 */

namespace facebook {
namespace wdt {

const std::string NUM_PORTS_FLAG = WDT_FLAG_STR(num_ports);
const std::string BLOCK_SIZE_FLAG = WDT_FLAG_STR(block_size_mbytes);
const std::string OPTION_TYPE_FLAG = WDT_FLAG_STR(option_type);

void overrideTest1(const std::string &optionType) {
  const auto &options = WdtOptions::get();
  google::SetCommandLineOption(OPTION_TYPE_FLAG.c_str(), optionType.c_str());
  google::SetCommandLineOption(NUM_PORTS_FLAG.c_str(), "4");
  google::SetCommandLineOption(BLOCK_SIZE_FLAG.c_str(), "8");
  WdtFlags::initializeFromFlags();
  EXPECT_EQ(4, options.num_ports);
  EXPECT_EQ(8, options.block_size_mbytes);
}

void overrideTest2(const std::string &optionType) {
  const auto &options = WdtOptions::get();
  google::SetCommandLineOption(OPTION_TYPE_FLAG.c_str(), optionType.c_str());
  google::SetCommandLineOption(NUM_PORTS_FLAG.c_str(), "8");
  google::SetCommandLineOption(BLOCK_SIZE_FLAG.c_str(), "16");
  WdtFlags::initializeFromFlags();
  EXPECT_EQ(8, options.num_ports);
  EXPECT_EQ(16, options.block_size_mbytes);
}

TEST(OptionType, FlashOptionTypeTest1) {
  const auto &options = WdtOptions::get();
  google::SetCommandLineOption(OPTION_TYPE_FLAG.c_str(), "flash");
  WdtFlags::initializeFromFlags();
  EXPECT_EQ(8, options.num_ports);
  EXPECT_EQ(16, options.block_size_mbytes);
}

TEST(OptionType, FlashOptionTypeTest2) {
  overrideTest1("flash");
}

TEST(OptionType, FlashOptionTypeTest3) {
  overrideTest2("flash");
}

TEST(OptionType, DiskOptionTypeTest1) {
  const auto &options = WdtOptions::get();
  google::SetCommandLineOption(OPTION_TYPE_FLAG.c_str(), "disk");
  WdtFlags::initializeFromFlags();
  EXPECT_EQ(1, options.num_ports);
  EXPECT_EQ(-1, options.block_size_mbytes);
}

TEST(OptionType, DiskOptionTypeTest2) {
  overrideTest1("disk");
}

TEST(OptionType, DiskOptionTypeTest3) {
  overrideTest2("disk");
}
}
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
