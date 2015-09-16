/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "System.h"
#include "WdtFlags.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace facebook {
namespace wdt {

using std::string;

void test1() {
  string actual = "expected";
  EXPECT_EQ("expected", actual);
}

TEST(System, Simple1) {
  test1();
}
}
}  // namespaces

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::WdtFlags::initializeFromFlags();
  int ret = RUN_ALL_TESTS();
  return ret;
}
