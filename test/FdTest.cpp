/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Wdt.h>
#include <wdt/test/TestCommon.h>

#include <folly/Conv.h>
#include <folly/Range.h>
#include <folly/ScopeGuard.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdio.h>
#include <sys/stat.h>

using namespace std;

namespace facebook {
namespace wdt {

void basicTest(bool resumption, bool nosize) {
  auto &opts = WdtOptions::getMutable();
  opts.enable_download_resumption = false;
  // Tmpfile (deleted)
  FILE *tmp = tmpfile();
  ASSERT_NE(tmp, nullptr);
  SCOPE_EXIT { fclose(tmp); };
  std::string contents = "foobar";
  ASSERT_EQ(contents.size(), fwrite(contents.data(), 1, contents.size(), tmp));
  ASSERT_EQ(0, fflush(tmp));

  // Keep the fd around.
  int fd = fileno(tmp);
  WLOG(INFO) << "tmp file fd " << fd;

  TemporaryDirectory tmpDir;
  auto recvDir = tmpDir.dir();

  WdtTransferRequest req(/* start port */ 0, /* num ports */ 3, recvDir);
  Receiver r(req);
  req = r.init();
  EXPECT_EQ(OK, req.errorCode);
  EXPECT_EQ(OK, r.transferAsync());
  // Not even using the actual name (which we don't know)
  std::string filename = "notexisting23r4";
  req.fileInfo.push_back(
      WdtFileInfo(fd, nosize ? -1 : contents.size(), filename));
  Sender s(req);
  // setWdtOptions not needed if change of option happens before sender cstror
  // but this indirectly tests that API (but need to manually see
  // "entered READ_FILE_CHUNKS" in the logs) TODO: test that programatically
  opts.enable_download_resumption = resumption;
  s.setWdtOptions(opts);
  req = s.init();
  EXPECT_EQ(OK, req.errorCode);
  auto report = s.transfer();

  if (!nosize) {
    EXPECT_EQ(OK, report->getSummary().getErrorCode());

    struct stat recvStat;
    int sret = stat((tmpDir.dir() + "/" + filename).c_str(), &recvStat);
    ASSERT_EQ(sret, 0);
    EXPECT_EQ(recvStat.st_size, contents.size());
  } else {
    // If no size is passed and the file does not exist the transfer
    // should fail.
    EXPECT_EQ(BYTE_SOURCE_READ_ERROR, report->getSummary().getErrorCode());
  }
}

TEST(FdTest, FdTestBasic) {
  basicTest(false, false);
}

TEST(FdTest, FdTestBasicResumption) {
  basicTest(true, false);
}

TEST(FdTest, FdTestNosize) {
  basicTest(false, true);
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
  WLOG(INFO) << "tmp file fd " << fd;
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
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt");
  int ret = RUN_ALL_TESTS();
  return ret;
}
