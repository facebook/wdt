/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <fcntl.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <stdlib.h>
#include <wdt/Wdt.h>
#include <wdt/test/TestCommon.h>
#include <fstream>

namespace facebook {
namespace wdt {
using std::string;
class RandomFile {
 public:
  explicit RandomFile(int64_t size) {
    char genFileName[] = "/tmp/FILE_TEST_XXXXXX";
    int ret = mkstemp(genFileName);
    if (ret == -1) {
      WLOG(ERROR) << "Error creating temp file";
      return;
    }
    close(ret);
    fileName_.assign(genFileName);
    WLOG(INFO) << "Making file " << fileName_;
    std::ofstream ofs(fileName_.c_str(), std::ios::binary | std::ios::out);
    ofs.seekp(size - 1);
    ofs.write("", 1);
    ofs.flush();
    fileSize_ = size;
    metaData_ = nullptr;
  }
  SourceMetaData* getMetaData() {
    if (metaData_ == nullptr) {
      metaData_ = new SourceMetaData();
    }
    metaData_->fullPath = fileName_;
    metaData_->relPath = getShortName();
    metaData_->seqId = 0;
    metaData_->size = fileSize_;
    metaData_->directReads = true;
    return metaData_;
  }
  int64_t getSize() const {
    return fileSize_;
  }
  const string& getFileName() const {
    return fileName_;
  }
  string getShortName() const {
    return fileName_.substr(5);
  }
  ~RandomFile() {
    std::remove(fileName_.c_str());
    if (metaData_ != nullptr) {
      delete metaData_;
    }
  }

 private:
  int64_t fileSize_{-1};
  string fileName_;
  SourceMetaData* metaData_{nullptr};
};

bool canSupportODirect() {
#ifdef WDT_SUPPORTS_ODIRECT
  return true;
#endif
  return false;
}

bool alignedBufferNeeded() {
#ifdef O_DIRECT
  return true;
#else
  return false;
#endif
}

void testReadSize(int64_t fileSize, ByteSource& byteSource) {
  int64_t totalSizeRead = 0;
  while (true) {
    int64_t size;
    char* data = byteSource.read(size);
    if (size <= 0) {
      break;
    }
    WDT_CHECK(data);
    totalSizeRead += size;
  }
  EXPECT_EQ(totalSizeRead, fileSize);
}

void testFileRead(const WdtOptions& options, int64_t fileSize,
                  bool directReads) {
  RandomFile file(fileSize);
  auto metaData = file.getMetaData();
  metaData->directReads = directReads;
  ThreadCtx threadCtx(options, true);
  FileByteSource byteSource(metaData, metaData->size, 0);
  ErrorCode code = byteSource.open(&threadCtx);
  EXPECT_EQ(code, OK);
  testReadSize(file.getSize(), byteSource);
}

TEST(FileByteSource, ODIRECT_NONMULTIPLE_OFFSET) {
  if (!canSupportODirect()) {
    WLOG(WARNING) << "Wdt can't support O_DIRECT skipping this test";
    return;
  }
  ThreadCtx threadCtx(WdtOptions::get(), true);
  for (int numTests = 0; numTests < 10; ++numTests) {
    int64_t fileSize = kDiskBlockSize + (rand32() % kDiskBlockSize);
    int64_t offset = rand32() % fileSize;

    RandomFile file(fileSize);
    auto metaData = file.getMetaData();
    metaData->directReads = true;
    FileByteSource byteSource(metaData, metaData->size, offset);
    ErrorCode code = byteSource.open(&threadCtx);
    EXPECT_EQ(code, OK);
    int64_t totalSizeRead = 0;
    while (true) {
      int64_t size;
      char* data = byteSource.read(size);
      if (size <= 0) {
        break;
      }
      WDT_CHECK(data);
      totalSizeRead += size;
    }
    EXPECT_EQ(totalSizeRead, fileSize - offset);
  }
}

TEST(FileByteSource, FILEINFO_ODIRECT) {
  int64_t fileSize = kDiskBlockSize * 10 + 11;
  int64_t sizeToRead = fileSize / 10;
  WLOG(INFO) << "File size " << fileSize << "Size to read " << sizeToRead;
  RandomFile file(fileSize);
  std::atomic<bool> shouldAbort{false};
  WdtAbortChecker queueAbortChecker(shouldAbort);
  WdtOptions options;
  DirectorySourceQueue Q(options, "/tmp", &queueAbortChecker);
  std::vector<WdtFileInfo> files;
  WdtFileInfo info(file.getShortName(), sizeToRead, true);
  files.push_back(info);
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  ThreadCtx threadCtx(options, true);
  auto byteSource = Q.getNextSource(&threadCtx, code);
  testReadSize(sizeToRead, *byteSource);
}

TEST(FileByteSource, MULTIPLEFILES_ODIRECT) {
  WdtOptions options;
  const int64_t blockSize = options.block_size_mbytes * 1024 * 1024;
  const int64_t fileSize = 5 * blockSize + kDiskBlockSize / 5;
  // 5 blocks long file and reading at least 2 blocks
  int64_t sizeToRead = blockSize;
  const int64_t numFiles = 5;
  std::vector<RandomFile> randFiles;
  randFiles.reserve(numFiles);
  for (int i = 0; i < numFiles; i++) {
    randFiles.emplace_back(fileSize);
  }
  std::atomic<bool> shouldAbort{false};
  WdtAbortChecker queueAbortChecker(shouldAbort);
  DirectorySourceQueue Q(options, "/tmp", &queueAbortChecker);
  std::vector<WdtFileInfo> files;
  for (const auto& f : randFiles) {
    WdtFileInfo info(f.getShortName(), sizeToRead, true);
    files.push_back(info);
  }
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  int fileNumber = 0;
  ThreadCtx threadCtx(options, true);
  while (true) {
    auto byteSource = Q.getNextSource(&threadCtx, code);
    if (!byteSource) {
      break;
    }
    testReadSize(sizeToRead, *byteSource);
    ++fileNumber;
  }
  EXPECT_EQ(fileNumber, numFiles);
}

TEST(FileByteSource, MULTIPLEFILES_REGULAR) {
  WdtOptions options;
  int64_t blockSize = options.block_size_mbytes * 1024 * 1024;
  int64_t fileSize = 5 * blockSize + kDiskBlockSize / 5;
  // 5 blocks long file and reading at least 2 blocks
  int64_t sizeToRead = blockSize;
  const int64_t numFiles = 5;
  std::vector<RandomFile> randFiles;
  randFiles.reserve(numFiles);
  for (int i = 0; i < numFiles; i++) {
    randFiles.emplace_back(fileSize);
  }
  std::atomic<bool> shouldAbort{false};
  WdtAbortChecker queueAbortChecker(shouldAbort);
  DirectorySourceQueue Q(options, "/tmp", &queueAbortChecker);
  std::vector<WdtFileInfo> files;
  for (const auto& f : randFiles) {
    WdtFileInfo info(f.getShortName(), sizeToRead, false);
    files.push_back(info);
  }
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  ThreadCtx threadCtx(options, true);
  while (true) {
    auto byteSource = Q.getNextSource(&threadCtx, code);
    if (!byteSource) {
      break;
    }
    testReadSize(sizeToRead, *byteSource);
  }
}
}
}  // namespaces

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt");
  int ret = RUN_ALL_TESTS();
  return ret;
}
