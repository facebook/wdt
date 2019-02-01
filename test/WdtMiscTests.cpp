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
  req.wdtNamespace = "foo";
  EXPECT_EQ(OK, wdt.wdtReceiveStart("foo", req));
  EXPECT_EQ(CONN_ERROR, wdt.wdtReceiveFinish("foo"));
  // Receiver object is still alive but has given up - we should not be able
  // to connect:
  req.directory = "/bin";
  EXPECT_EQ(CONN_ERROR, wdt.wdtSend(req));
}

TEST(BasicTest, MultiWdtSender) {
  TemporaryDirectory tmpDir;
  auto baseDir = tmpDir.dir();

  LOG(INFO) << "Testing in " << baseDir;
  string srcDir = baseDir + "/src";
  string srcFile = "file1";
  string targetDir = baseDir + "/dst";
  string srcFileFullPath = srcDir + "/" + srcFile;

  Wdt &wdt = Wdt::initializeWdt("unit test MultiWdtSender");
  WdtOptions &options = wdt.getWdtOptions();
  options.avg_mbytes_per_sec = 100;
  WdtTransferRequest req(/* start port */ 0,
                         /* num ports */ 1, targetDir);
  req.wdtNamespace = "foo";
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
  auto sender1Thread = thread(
      [&wdt, &req]() { EXPECT_EQ(OK, wdt.wdtSend(req, nullptr, true)); });
  auto sender2Thread = thread([&wdt, &req]() {
    /* sleep override */
    std::this_thread::sleep_for(std::chrono::seconds(1));
    EXPECT_EQ(ALREADY_EXISTS, wdt.wdtSend(req, nullptr, true));
  });
  sender1Thread.join();
  sender2Thread.join();

  EXPECT_EQ(OK, wdt.wdtReceiveFinish("foo"));
}

TEST(BasicTest, ThrottlerWithoutReporting) {
  WdtOptions options;
  options.avg_mbytes_per_sec = 1;
  shared_ptr<Throttler> throttler =
      std::make_shared<Throttler>(options.getThrottlerOptions());
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
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
