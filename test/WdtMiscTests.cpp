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
#include <thread>

using namespace std;

namespace facebook {
namespace wdt {

TEST(BasicTest, ReceiverAcceptTimeout) {
  Wdt &wdt = Wdt::initializeWdt("unit test ReceiverAcceptTimeout");
  WdtOptions &opts = wdt.getWdtOptions();
  opts.accept_timeout_millis = 1;
  opts.max_accept_retries = 1;
  opts.max_retries = 1;
  WdtTransferRequest req(0, 2, "/tmp/wdtTest");
  EXPECT_EQ(OK, wdt.wdtReceiveStart("foo", req));
  EXPECT_EQ(CONN_ERROR, wdt.wdtReceiveFinish("foo"));
  // Receiver object is still alive but has given up - we should not be able
  // to connect:
  req.directory = "/bin";
  EXPECT_EQ(CONN_ERROR, wdt.wdtSend("foo", req));
}

// TODO: should move temp dir making etc to wdt test common or use
// python or bash for this kind of test
TEST(BasicTest, MultiWdtSender) {
  char baseDir[] = "/tmp/wdtTest/XXXXXX";
  if (!mkdtemp(baseDir)) {
    PLOG(FATAL) << "unable to make " << baseDir;
  }
  LOG(INFO) << "Testing in " << baseDir;
  string srcDir(baseDir);
  srcDir.append("/src");
  string srcFile = "file1";
  string targetDir(baseDir);
  targetDir.append("/dst");
  string srcFileFullPath = srcDir + "/" + srcFile;

  Wdt &wdt = Wdt::initializeWdt("unit test MultiWdtSender");
  WdtOptions &options = wdt.getWdtOptions();
  options.avg_mbytes_per_sec = 100;
  WdtTransferRequest req(/* start port */ 0, /* num ports */ 1, targetDir);
  mkdir(srcDir.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  {
    // Create 400mb srcFile
    const int32_t size = 1024 * 1024;
    uint a[size];
    FILE *pFile;
    pFile = fopen(srcFileFullPath.c_str(), "wb");
    for (int i = 0; i < 100; ++i) {
      fwrite(a, 1, sizeof(a), pFile);
    }
    fclose(pFile);
  }
  EXPECT_EQ(OK, wdt.wdtReceiveStart("foo", req));
  req.directory = string(srcDir);
  auto sender1Thread = thread([&wdt, &req]() {
    EXPECT_EQ(OK, wdt.wdtSend("foo", req, nullptr, true));
  });
  auto sender2Thread = thread([&wdt, &req]() {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(ALREADY_EXISTS, wdt.wdtSend("foo", req, nullptr, true));
  });
  sender1Thread.join();
  sender2Thread.join();

  EXPECT_EQ(OK, wdt.wdtReceiveFinish("foo"));
  unlink(srcFileFullPath.c_str());
  rmdir(srcDir.c_str());
  string dstFile = targetDir + "/" + srcFile;
  unlink(dstFile.c_str());
  rmdir(targetDir.c_str());
  rmdir(baseDir);
}

TEST(BasicTest, ThrottlerWithoutReporting) {
  const double avgThrottlerRate = 1 * kMbToB;
  shared_ptr<Throttler> throttler =
      Throttler::makeThrottler(avgThrottlerRate, 0, 0, 0);
  const int toWrite = 2 * kMbToB;
  const int blockSize = 1024;
  int written = 0;
  throttler->startTransfer();
  const auto startTime = Clock::now();
  while (written < toWrite) {
    throttler->limit(blockSize);
    written += blockSize;
  }
  const auto endTime = Clock::now();
  throttler->endTransfer();
  int durationMs = durationMillis(endTime - startTime);
  EXPECT_GT(durationMs, 1900);
  EXPECT_LT(durationMs, 2200);
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
