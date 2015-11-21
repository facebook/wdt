/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Sender.h>
#include <wdt/Receiver.h>

#include <folly/Random.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <stdio.h>


using namespace std;

namespace facebook {
namespace wdt {

void basicTest(bool resumption) {
  auto &opts = WdtOptions::getMutable();
  opts.skip_writes = true;
  opts.enable_download_resumption = false;
  // Tmpfile (deleted)
  FILE *tmp = tmpfile();
  EXPECT_NE(tmp, nullptr);
  // We keep the fd around
  int fd = fileno(tmp);
  LOG(INFO) << "tmp file fd " << fd;
  fclose(tmp);
  WdtTransferRequest req(/* start port */ 0, /* num ports */ 3, "/tmp/wdtTest");
  Receiver r(req);
  req = r.init();
  EXPECT_EQ(OK, req.errorCode);
  EXPECT_EQ(OK, r.transferAsync());
  // Not even using the actual name (which we don't know)
  req.fileInfo.push_back(FileInfo("notexisting23r4", 0, fd));
  Sender s(req);
  opts.enable_download_resumption = resumption;
  req = s.init();
  EXPECT_EQ(OK, req.errorCode);
  auto report = s.transfer();
  EXPECT_EQ(OK, report->getSummary().getErrorCode());
}

TEST(FdTest, FdTestBasic) {
  basicTest(false);
}

TEST(FdTest, FdTestBasicResumption) {
  basicTest(true);
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
