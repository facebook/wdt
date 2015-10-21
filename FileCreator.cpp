/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "FileCreator.h"
#include "ErrorCodes.h"
#include "Reporting.h"

#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <folly/Conv.h>

namespace facebook {
namespace wdt {

bool FileCreator::setFileSize(int fd, int64_t fileSize) {
  struct stat fileStat;
  if (fstat(fd, &fileStat) != 0) {
    PLOG(ERROR) << "fstat() failed for " << fd;
    return false;
  }
  auto &options = WdtOptions::get();
  if (fileStat.st_size > fileSize) {
    // existing file is larger than required
    int64_t sizeToTruncate = (options.shouldPreallocateFiles() ? fileSize : 0);
    if (ftruncate(fd, sizeToTruncate) != 0) {
      PLOG(ERROR) << "ftruncate() failed for " << fd << " " << sizeToTruncate;
      return false;
    }
  }
  if (fileSize == 0) {
    return true;
  }
  if (!options.shouldPreallocateFiles()) {
    // pre-allocation is disabled
    return true;
  }
#ifdef HAS_POSIX_FALLOCATE
  int status = posix_fallocate(fd, 0, fileSize);
  if (status != 0) {
    LOG(ERROR) << "fallocate() failed " << strerrorStr(status);
    return false;
  }
  return true;
#else
  WDT_CHECK(false) << "Should never reach here";
#endif
}

int FileCreator::openAndSetSize(BlockDetails const *blockDetails) {
  const auto &options = WdtOptions::get();
  int fd;
  const bool doCreate = (blockDetails->allocationStatus == NOT_EXISTS);
  const bool isTooLarge = (blockDetails->allocationStatus == EXISTS_TOO_LARGE);
  if (doCreate) {
    fd = createFile(blockDetails->fileName);
  } else {
    fd = openExistingFile(blockDetails->fileName);
  }
  if (fd < 0) {
    return -1;
  }
  if (blockDetails->allocationStatus == EXISTS_CORRECT_SIZE) {
    return fd;
  }
  if (!setFileSize(fd, blockDetails->fileSize)) {
    close(fd);
    return -1;
  }
  if (options.isLogBasedResumption()) {
    if (isTooLarge) {
      LOG(WARNING) << "File size smaller in the sender side "
                   << blockDetails->fileName
                   << ", marking previous transferred chunks as invalid";
      transferLogManager_.addFileInvalidationEntry(blockDetails->prevSeqId);
    }
    if (isTooLarge || doCreate) {
      transferLogManager_.addFileCreationEntry(
          blockDetails->fileName, blockDetails->seqId, blockDetails->fileSize);
    } else {
      WDT_CHECK_EQ(EXISTS_TOO_SMALL, blockDetails->allocationStatus);
      transferLogManager_.addFileResizeEntry(blockDetails->seqId,
                                             blockDetails->fileSize);
    }
  }
  return fd;
}

int FileCreator::openForFirstBlock(int threadIndex,
                                   BlockDetails const *blockDetails) {
  int fd = openAndSetSize(blockDetails);
  {
    folly::SpinLockGuard guard(lock_);
    auto it = fileStatusMap_.find(blockDetails->seqId);
    WDT_CHECK(it != fileStatusMap_.end());
    it->second = fd >= 0 ? ALLOCATED : FAILED;
  }
  std::unique_lock<std::mutex> waitLock(allocationMutex_);
  threadConditionVariables_[threadIndex].notify_all();
  return fd;
}

bool FileCreator::waitForAllocationFinish(int allocatingThreadIndex,
                                          int64_t seqId) {
  std::unique_lock<std::mutex> waitLock(allocationMutex_);
  while (true) {
    {
      folly::SpinLockGuard guard(lock_);
      auto it = fileStatusMap_.find(seqId);
      WDT_CHECK(it != fileStatusMap_.end());
      if (it->second == ALLOCATED) {
        return true;
      }
      if (it->second == FAILED) {
        return false;
      }
    }
    threadConditionVariables_[allocatingThreadIndex].wait(waitLock);
  }
}

int FileCreator::openForBlocks(int threadIndex,
                               BlockDetails const *blockDetails) {
  lock_.lock();
  auto it = fileStatusMap_.find(blockDetails->seqId);
  if (blockDetails->allocationStatus == EXISTS_CORRECT_SIZE &&
      it == fileStatusMap_.end()) {
    it = fileStatusMap_.insert(std::make_pair(blockDetails->seqId,
                                              FileCreator::ALLOCATED)).first;
  }
  if (it == fileStatusMap_.end()) {
    // allocation has not started for this file
    fileStatusMap_.insert(std::make_pair(blockDetails->seqId, threadIndex));
    lock_.unlock();
    return openForFirstBlock(threadIndex, blockDetails);
  }
  auto status = it->second;
  lock_.unlock();
  if (status == FAILED) {
    // allocation failed previously
    return -1;
  }
  if (status != ALLOCATED) {
    // allocation in progress
    if (!waitForAllocationFinish(it->second, blockDetails->seqId)) {
      return -1;
    }
  }
  return openExistingFile(blockDetails->fileName);
}

using std::string;

int FileCreator::openExistingFile(const string &relPathStr) {
  // This should have been validated earlier and errored out
  // instead of crashing here
  WDT_CHECK(!relPathStr.empty());
  WDT_CHECK(relPathStr[0] != '/');
  WDT_CHECK(relPathStr.back() != '/');

  const string path = rootDir_ + relPathStr;

  int openFlags = O_WRONLY;
  START_PERF_TIMER
  int res = open(path.c_str(), openFlags, 0644);
  RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
  if (res < 0) {
    PLOG(ERROR) << "failed opening file " << path;
    return -1;
  }
  VLOG(1) << "successfully opened file " << path;
  return res;
}

int FileCreator::createFile(const string &relPathStr) {
  CHECK(!relPathStr.empty());
  CHECK(relPathStr[0] != '/');
  CHECK(relPathStr.back() != '/');

  const string path = rootDir_ + relPathStr;

  int p = relPathStr.size();
  while (p && relPathStr[p - 1] != '/') {
    --p;
  }
  std::string dir;
  if (p) {
    dir.assign(relPathStr.data(), p);
    START_PERF_TIMER
    const bool dirSuccess1 = createDirRecursively(dir);
    RECORD_PERF_RESULT(PerfStatReport::DIRECTORY_CREATE)
    if (!dirSuccess1) {
      // retry with force
      LOG(ERROR) << "failed to create dir " << dir << " recursively, "
                 << "trying to force directory creation";
      START_PERF_TIMER
      const bool dirSuccess2 = createDirRecursively(dir, true /* force */);
      RECORD_PERF_RESULT(PerfStatReport::DIRECTORY_CREATE)
      if (!dirSuccess2) {
        LOG(ERROR) << "failed to create dir " << dir << " recursively";
        return -1;
      }
    }
  }
  int openFlags = O_CREAT | O_WRONLY;
  auto &options = WdtOptions::get();
  // When doing download resumption we sometime open files that do already
  // exist and we need to overwrite them anyway (files which have been
  // discarded from the log for some reason)
  if (options.overwrite || options.enable_download_resumption) {
    // Make sure file size resumption will not get messed up if we
    // expect to create this file
    openFlags |= O_TRUNC;
  } else {
    // Make sure open will fail if we don't allow overwriting and
    // the file happens to already exist
    openFlags |= O_EXCL;
  }
  START_PERF_TIMER
  int res = open(path.c_str(), openFlags, 0644);
  RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
  if (res < 0) {
    if (dir.empty()) {
      PLOG(ERROR) << "failed creating file " << path;
      return -1;
    }
    PLOG(ERROR) << "failed creating file " << path << ", trying to "
                << "force directory creation";
    {
      START_PERF_TIMER
      const bool dirSuccess = createDirRecursively(dir, true /* force */);
      RECORD_PERF_RESULT(PerfStatReport::DIRECTORY_CREATE)
      if (!dirSuccess) {
        LOG(ERROR) << "failed to create dir " << dir << " recursively";
        return -1;
      }
    }
    START_PERF_TIMER
    res = open(path.c_str(), openFlags, 0644);
    RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
    if (res < 0) {
      PLOG(ERROR) << "failed creating file " << path;
      return -1;
    }
  }
  VLOG(1) << "successfully created file " << path;
  return res;
}

bool FileCreator::createDirRecursively(const std::string dir, bool force) {
  if (!force && dirCreated(dir)) {
    return true;
  }

  WDT_CHECK(dir.back() == '/');

  int64_t lastIndex = dir.size() - 1;
  while (lastIndex > 0 && dir[lastIndex - 1] != '/') {
    lastIndex--;
  }

  if (lastIndex > 0) {
    if (!createDirRecursively(dir.substr(0, lastIndex), force)) {
      return false;
    }
  }

  std::string fullDirPath;
  folly::toAppend(rootDir_, dir, &fullDirPath);
  int code = mkdir(fullDirPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
  if (code != 0 && errno != EEXIST && errno != EISDIR) {
    PLOG(ERROR) << "failed to make directory " << fullDirPath;
    return false;
  } else if (code != 0) {
    LOG(INFO) << "dir already exists " << fullDirPath;
  } else {
    LOG(INFO) << "made dir " << fullDirPath;
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    createdDirs_.insert(dir);
  }

  return true;
}

/* static */
void FileCreator::addTrailingSlash(string &path) {
  if (path.back() != '/') {
    path.push_back('/');
    VLOG(1) << "Added missing trailing / to " << path;
  }
}
}
}
