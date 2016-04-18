/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "TestCommon.h"

#include <wdt/Wdt.h>

using namespace std;

namespace facebook {
namespace wdt {

TEST(BasicTest, ReceiverAcceptTimeout) {
  Wdt &wdt = Wdt::initializeWdt("unit test app");
  WdtOptions &opts = wdt.getWdtOptions();
  opts.accept_timeout_millis = 1;
  opts.max_accept_retries = 1;
  opts.max_retries = 1;
  WdtTransferRequest req;
  //{
  Receiver r(0, 2, "/tmp/wdtTest");
  req = r.init();
  EXPECT_EQ(OK, r.transferAsync());
  auto report = r.finish();
  EXPECT_EQ(CONN_ERROR, report->getSummary().getErrorCode());
  //}
  // Receiver object is still alive but has given up - we should not be able
  // to connect:
  req.directory = "/bin";
  EXPECT_EQ(CONN_ERROR, wdt.wdtSend("foo", req));
}
}
}  // namespace end

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
