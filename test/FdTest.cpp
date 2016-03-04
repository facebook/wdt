/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "TestCommon.h"

#include <wdt/Receiver.h>
#include <wdt/Sender.h>
#include <wdt/util/WdtFlags.h>

#include <folly/Range.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/stat.h>

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
  std::string recvDir;
  folly::toAppend("/tmp/wdtTest/recv", rand32(), &recvDir);
  WdtTransferRequest req(/* start port */ 0, /* num ports */ 3, recvDir);
  Receiver r(req);
  req = r.init();
  EXPECT_EQ(OK, req.errorCode);
  EXPECT_EQ(OK, r.transferAsync());
  // Not even using the actual name (which we don't know)
  req.fileInfo.push_back(WdtFileInfo(fd, 0, "notexisting23r4"));
  Sender s(req);
  // setWdtOptions not needed if change of option happens before sender cstror
  // but this indirectly tests that API (but need to manually see
  // "entered READ_FILE_CHUNKS" in the logs) TODO: test that programatically
  opts.enable_download_resumption = resumption;
  s.setWdtOptions(opts);
  req = s.init();
  EXPECT_EQ(OK, req.errorCode);
  auto report = s.transfer();
  EXPECT_EQ(OK, report->getSummary().getErrorCode());
  struct stat dirStat;
  int sret = stat(recvDir.c_str(), &dirStat);
  EXPECT_EQ(sret, -1);
  EXPECT_EQ(errno, ENOENT);
}

TEST(FdTest, FdTestBasic) {
  basicTest(false);
}

TEST(FdTest, FdTestBasicResumption) {
  basicTest(true);
}

TEST(DupSend, DuplicateSend) {
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
  req.fileInfo.push_back(WdtFileInfo(fd, 0, "notexisting23r4"));
  Sender s(req);
  req = s.init();
  EXPECT_EQ(OK, req.errorCode);
  ErrorCode ret1 = s.transferAsync();
  EXPECT_EQ(OK, ret1);
  ErrorCode ret2 = s.transferAsync();
  EXPECT_EQ(ALREADY_EXISTS, ret2);
  auto report = s.finish();
  EXPECT_EQ(OK, report->getSummary().getErrorCode());
}
}
}  // namespace end

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::WdtFlags::initializeFromFlags();
  int ret = RUN_ALL_TESTS();
  return ret;
}
