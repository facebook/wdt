/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "DirectorySourceQueue.h"
#include "FileByteSource.h"
#include "WdtFlags.h"
#include <glog/logging.h>
#include <gtest/gtest.h>
#include <folly/Random.h>
#include <fstream>
#include <stdlib.h>
#include <fcntl.h>
DEFINE_int32(random_seed, folly::randomNumberSeed(), "random seed");

namespace facebook {
namespace wdt {
using std::string;
class RandomFile {
 public:
  explicit RandomFile(int64_t size) {
    char genFileName[] = "/tmp/FILE_TEST_XXXXXX";
    int ret = mkstemp(genFileName);
    if (ret == -1) {
      LOG(ERROR) << "Error creating temp file";
      return;
    }
    close(ret);
    fileName_.assign(genFileName);
    LOG(INFO) << "Making file " << fileName_;
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
    char *data = byteSource.read(size);
    VLOG(2) << "byteSource.read(" << size << ") returned " << (void*)data;
    if (size <= 0) {
      break;
    }
    WDT_CHECK(data);
    totalSizeRead += size;
  }
  EXPECT_EQ(totalSizeRead, fileSize);
}

void testBufferSize(int64_t originalBufferSize, bool alignedBuffer,
                    ByteSource& byteSource) {
  auto newBufferSize = originalBufferSize;
  auto remainder = newBufferSize % kDiskBlockSize;
  if (canSupportODirect() && alignedBuffer && remainder != 0) {
    newBufferSize += (kDiskBlockSize - remainder);
  }
  if (canSupportODirect() && alignedBuffer) {
    EXPECT_EQ(byteSource.getBufferSize() % kDiskBlockSize, 0);
  }
  EXPECT_EQ(byteSource.getBufferSize(), newBufferSize);
}

void testFileRead(int64_t fileSize, int64_t bufferSize, bool directReads) {
  RandomFile file(fileSize);
  auto metaData = file.getMetaData();
  metaData->directReads = directReads;
  FileByteSource byteSource(metaData, metaData->size, 0, bufferSize);
  ErrorCode code = byteSource.open();
  EXPECT_EQ(code, OK);
  testBufferSize(bufferSize, directReads && alignedBufferNeeded(), byteSource);
  testReadSize(file.getSize(), byteSource);
}

TEST(FileByteSource, ODIRECT_NONMULTIPLE) {
  if (!canSupportODirect()) {
    LOG(WARNING) << "Wdt can't support O_DIRECT skipping this test";
    return;
  }
  int64_t fileSize = kDiskBlockSize * 100 + 10;
  int64_t bufferSize = 511;
  std::thread t(&testFileRead, fileSize, bufferSize, true);
  t.join();
}

TEST(FileByteSource, SMALL_MULTIPLE_ODIRECT) {
  if (!canSupportODirect()) {
    LOG(WARNING) << "Wdt can't support O_DIRECT skipping this test";
    return;
  }
  int64_t fileSize = 512;
  int64_t bufferSize = 11;
  std::thread t(&testFileRead, fileSize, bufferSize, true);
  t.join();
}

TEST(FileByteSource, SMALL_NONMULTIPLE_ODIRECT) {
  if (!canSupportODirect()) {
    LOG(WARNING) << "Wdt can't support O_DIRECT skipping this test";
    return;
  }
  int64_t fileSize = 1;
  int64_t bufferSize = 11;
  std::thread t(&testFileRead, fileSize, bufferSize, true);
  t.join();
}

TEST(FileByteSource, FILEINFO_ODIRECT) {
  int64_t fileSize = kDiskBlockSize * 10 + 11;
  int64_t sizeToRead = fileSize / 10;
  LOG(INFO) << "File size " << fileSize << "Size to read " << sizeToRead;
  RandomFile file(fileSize);
  std::atomic<bool> shouldAbort{false};
  WdtAbortChecker queueAbortChecker(shouldAbort);
  DirectorySourceQueue Q("/tmp", &queueAbortChecker);
  std::vector<FileInfo> files;
  FileInfo info(file.getShortName(), sizeToRead);
  info.directReads = true;
  files.push_back(info);
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  std::thread t([&]() {
    auto byteSource = Q.getNextSource(code);
    EXPECT_EQ(byteSource->open(), OK);
    testReadSize(sizeToRead, *byteSource);
  });
  t.join();
}

TEST(FileByteSource, MULTIPLEFILES_ODIRECT) {
  auto& options = WdtOptions::getMutable();
  int64_t originalBufferSize = 255 * 1024;
  options.buffer_size = originalBufferSize;
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
  DirectorySourceQueue Q("/tmp", &queueAbortChecker);
  std::vector<FileInfo> files;
  for (const auto& f : randFiles) {
    FileInfo info(f.getShortName(), sizeToRead);
    info.directReads = true;
    files.push_back(info);
  }
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  std::thread t([&]() {
    int fileNumber = 0;
    while (true) {
      auto byteSource = Q.getNextSource(code);
      if (!byteSource) {
        break;
      }
      EXPECT_EQ(byteSource->open(), OK);
      testBufferSize(originalBufferSize,
                     files[fileNumber].directReads && alignedBufferNeeded(),
                     *byteSource);
      testReadSize(sizeToRead, *byteSource);
      ++fileNumber;
    }
    EXPECT_EQ(fileNumber, numFiles);
  });
  t.join();
}

TEST(FileByteSource, MIXED_FILES) {
  auto& options = WdtOptions::getMutable();
  int64_t originalBufferSize = 255 * 1024;
  options.buffer_size = originalBufferSize;
  int64_t blockSize = options.block_size_mbytes * 1024 * 1024;
  int64_t fileSize = 5 * blockSize + kDiskBlockSize / 5;
  // 5 blocks long file and reading at least 2 blocks
  int64_t sizeToRead = blockSize;
  std::vector<RandomFile> randFiles;
  const int64_t numFiles = 3;
  randFiles.reserve(numFiles);
  for (int i = 0; i < numFiles; i++) {
    randFiles.emplace_back(fileSize);
  }
  int fileNum = 0;
  std::vector<FileInfo> filesInfo;
  for (const auto& f : randFiles) {
    FileInfo info(f.getShortName(), sizeToRead);
    if (canSupportODirect() && fileNum % 2 != 0) {
      info.directReads = true;
    }
    fileNum++;
    filesInfo.push_back(info);
  }
  for (int i = 0; i < numFiles; i++) {
    auto metaData = randFiles[i].getMetaData();
    metaData->directReads = filesInfo[i].directReads;
    ByteSource* byteSource = new FileByteSource(metaData, filesInfo[i].fileSize,
                                                0, originalBufferSize);
    EXPECT_EQ(byteSource->open(), OK);
    if (!alignedBufferNeeded() || i == 0) {
      // First file is regular
      EXPECT_EQ(byteSource->getBufferSize(), originalBufferSize);
    } else {
      EXPECT_EQ(byteSource->getBufferSize() % kDiskBlockSize, 0);
    }
    delete byteSource;
  }
}

TEST(FileByteSource, REGULAR_READ) {
  int64_t fileSize = kDiskBlockSize * 10 + 10;
  int64_t bufferSize = 11;
  std::thread t(&testFileRead, fileSize, bufferSize, false);
  t.join();
}

TEST(FileByteSource, MULTIPLEFILES_REGULAR) {
  const auto& options = WdtOptions::get();
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
  DirectorySourceQueue Q("/tmp", &queueAbortChecker);
  std::vector<FileInfo> files;
  for (const auto& f : randFiles) {
    FileInfo info(f.getShortName(), sizeToRead);
    files.push_back(info);
  }
  Q.setFileInfo(files);
  Q.buildQueueSynchronously();
  ErrorCode code;
  int64_t bufferSize = options.buffer_size;
  std::thread t([&]() {
    while (true) {
      auto byteSource = Q.getNextSource(code);
      if (!byteSource) {
        break;
      }
      EXPECT_EQ(byteSource->open(), OK);
      EXPECT_EQ(byteSource->getBufferSize(), bufferSize);
      testReadSize(sizeToRead, *byteSource);
    }
  });
  t.join();
}
}
}  // namespaces

int main(int argc, char* argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::WdtFlags::initializeFromFlags();
  int ret = RUN_ALL_TESTS();
  return ret;
}
