/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <algorithm>

#include <boost/filesystem.hpp>
#include <folly/FileUtil.h>
#include <folly/Random.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <wdt/Receiver.h>
#include <wdt/Sender.h>
#include <wdt/Wdt.h>

namespace facebook {
namespace wdt {

namespace {
boost::filesystem::path createTmpDir() {
  boost::filesystem::path dir = "/tmp";
  dir /= boost::filesystem::unique_path("wdt_sender_test-%%%-%%%%-%%%");
  if (boost::filesystem::exists(dir)) {
    boost::filesystem::remove_all(dir);
  }
  boost::filesystem::create_directories(dir);
  return dir;
}

void createFile(const std::string& path, size_t size) {
  auto data = std::vector<char>(size, 0);
  std::generate(data.begin(), data.end(),
                []() -> char { return (folly::Random::rand32() % 10) + '0'; });
  folly::writeFile(data, path.data());
}

}  // namespace

TEST(SenderTest, FileInfoGenerator) {
  auto senderDir = createTmpDir();
  auto receiverDir = createTmpDir();

  // create 10 files
  std::vector<WdtFileInfo> fileInfo;
  std::vector<size_t> cumulativeSize;
  cumulativeSize.push_back(0);
  const size_t numFiles = 3;
  const uint32_t maxFileSize = 1000000;
  for (size_t i = 0; i < numFiles; i++) {
    auto file = senderDir / std::to_string(i);
    size_t fileSz = folly::Random::rand32() % maxFileSize;
    createFile(file.c_str(), fileSz);
    fileInfo.push_back({std::to_string(i), -1, false});
    cumulativeSize.push_back(cumulativeSize.back() + fileSz);
  }

  auto receiver = std::make_unique<Receiver>(0, 3, receiverDir.c_str());
  auto req = receiver->init();
  receiver->transferAsync();

  std::unique_ptr<Sender> sender;
  size_t nextFile = 0;
  req.disableDirectoryTraversal = true;
  req.directory = senderDir.c_str();
  req.fileInfo = std::vector<WdtFileInfo>{fileInfo[nextFile++]};
  req.fileInfoGenerator = [&]() -> folly::Optional<std::vector<WdtFileInfo>> {
    auto stats = sender->getGlobalTransferStats();
    EXPECT_EQ(cumulativeSize[nextFile], stats.getDataBytes());
    if (nextFile < fileInfo.size()) {
      return std::vector<WdtFileInfo>{fileInfo[nextFile++]};
    }
    return folly::none;
  };
  sender = std::make_unique<Sender>(req);
  sender->transfer();
  receiver->finish();

  for (size_t i = 0; i < numFiles; i++) {
    auto sentPath = senderDir / std::to_string(i);
    std::string sent;
    EXPECT_TRUE(folly::readFile(sentPath.c_str(), sent));

    auto recvPath = receiverDir / std::to_string(i);
    std::string recv;
    EXPECT_TRUE(folly::readFile(recvPath.c_str(), recv));

    EXPECT_EQ(sent, recv);
  }

  boost::filesystem::remove_all(senderDir);
  boost::filesystem::remove_all(receiverDir);
}

}  // namespace wdt
}  // namespace facebook

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt");
  int ret = RUN_ALL_TESTS();
  return ret;
}
