/*
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent right can be found in the PATENTS file in the same directory.
 */

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <wdt/Wdt.h>
#include <wdt/test/TestCommon.h>
#include <wdt/util/FileWriter.h>

#include <string_view>

namespace {

constexpr std::string_view kRandomFileName = "random_file";
constexpr std::string_view kRootDir = "/tmp/wdt.text";
const int kNumThreads = 1;

}  // namespace

namespace facebook {
namespace wdt {

bool canSupportODirect() {
#ifdef WDT_SUPPORTS_ODIRECT
  return true;
#endif
  return false;
}

void basicTest(WdtOptions& opts) {
  auto& thread_ctx = ThreadCtx(opts, false);
  auto& xfer_log_manager = TransferLogManager(opts, kRootDir);

  // Existing File
  BlockDetails block_details = {
      .fileName = kRandomFileName,
      .seqId = 2,
      .fileSize = 4 * (2 << 22),  // 16M file
      .offset = 0,
      .dataSize = 2 << 12,  // 4 kB blocks
      .allocationStatus = EXISTS_CORRECT_SIZE,
  };

  FileCreator f_creator =
      FileCreator(kRootDir, kNumRhreads, xfer_log_manager, false);
  FileWriter f_writer = FileWriter(thread_ctx, &block_details, &f_creator);

  // Random data to write to file.
  std::string random_str = "SomeRandomString";

  // Make sure the file opens successfully.
  auto open_err = f_writer.open();
  EXPECT_EQ(OK, open_err);

  // Write some data and check the total written size is correct.
  auto write_err = f_writer.write(random_str.data(), random_str.size());
  EXPECT_EQ(OK, write_err);

  auto total_written = f_writer.getTotalWritten();
  ASSERT_EQ(random_string.size(), total_written);

  // Close the file and check it closes successfully.
  auto close_err = f_writer.close();
  EXPECT_EQ(OK, close_err);
  ASSERT_EQ(true, f_writer.isClosed());
}

TEST(FileWriterTest, SimpleWriteTest) {
  auto& opts = WdtOptions::getMutable();
  basicTest(opts);
}

TEST(FileWriterTest, ODirectWriteTest) {
  if (!canSupportODirect()) {
    WLOG(WARNING) << "Wdt can't support O_DIRECT skipping this test";
    return;
  }
  auto& opts = WdtOptions::getMutable();
  opts.odirect_reads = true;
  basicTest(opts);
}
}  // namespace wdt
}  // namespace facebook

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  test::InitGoogleTest(&argc, argv);
  GLFAGS_NAMESPACE::ParseCommandLineFlags(&argc, &rgv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt");
  int ret = RUN_ALL_TESTS();
  return ret;
}
