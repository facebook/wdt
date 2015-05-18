#include "FileCreator.h"
#include "ErrorCodes.h"

#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fcntl.h>
#include <folly/Conv.h>

namespace facebook {
namespace wdt {

bool FileCreator::setFileSize(int fd, size_t fileSize) {
  struct stat fileStat;
  if (fstat(fd, &fileStat) != 0) {
    PLOG(ERROR) << "fstat() failed for " << fd;
    return false;
  }
  if (fileStat.st_size > fileSize) {
    // existing file is larger than required
    if (ftruncate(fd, fileSize) != 0) {
      PLOG(ERROR) << "ftruncate() failed for " << fd;
      return false;
    }
  }
  if (fileSize == 0) {
    return true;
  }
#ifdef HAS_POSIX_FALLOCATE
  int status = posix_fallocate(fd, 0, fileSize);
  if (status != 0) {
    LOG(ERROR) << "fallocate() failed " << strerrorStr(status);
    return false;
  }
#endif
  return true;
}

int FileCreator::openAndSetSize(const std::string &relPath, size_t size) {
  int fd = createFile(relPath);
  if (fd < 0) {
    return -1;
  }
  if (!setFileSize(fd, size)) {
    close(fd);
    return -1;
  }
  return fd;
}

int FileCreator::openForFirstBlock(int threadIndex, const std::string &relPath,
                                   uint64_t seqId, size_t size) {
  int fd = openAndSetSize(relPath, size);
  {
    folly::SpinLockGuard guard(lock_);
    auto it = fileStatusMap_.find(seqId);
    WDT_CHECK(it != fileStatusMap_.end());
    it->second = fd >= 0 ? ALLOCATED : FAILED;
  }
  std::unique_lock<std::mutex> waitLock(allocationMutex_);
  threadConditionVariables_[threadIndex].notify_all();
  return fd;
}

bool FileCreator::waitForAllocationFinish(int allocatingThreadIndex,
                                          uint64_t seqId) {
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

int FileCreator::openForBlocks(int threadIndex, const std::string &relPath,
                               uint64_t seqId, size_t size) {
  lock_.lock();
  auto it = fileStatusMap_.find(seqId);
  if (it == fileStatusMap_.end()) {
    // allocation has not started for this file
    fileStatusMap_.insert(std::make_pair(seqId, threadIndex));
    lock_.unlock();
    return openForFirstBlock(threadIndex, relPath, seqId, size);
  }
  auto status = it->second;
  lock_.unlock();
  if (status == FAILED) {
    // allocation failed previously
    return -1;
  }
  if (status != ALLOCATED) {
    // allocation in progress
    if (!waitForAllocationFinish(it->second, seqId)) {
      return -1;
    }
  }
  return createFile(relPath);
}

using std::string;

int FileCreator::createFile(const string &relPathStr) {
  CHECK(!relPathStr.empty());
  CHECK(relPathStr[0] != '/');
  CHECK(relPathStr.back() != '/');

  std::string path(rootDir_);
  path.append(relPathStr);

  int p = relPathStr.size();
  while (p && relPathStr[p - 1] != '/') {
    --p;
  }
  std::string dir;
  if (p) {
    dir.assign(relPathStr.data(), p);
    if (!createDirRecursively(dir)) {
      // retry with force
      LOG(ERROR) << "failed to create dir " << dir << " recursively, "
                 << "trying to force directory creation";
      if (!createDirRecursively(dir, true /* force */)) {
        LOG(ERROR) << "failed to create dir " << dir << " recursively";
        return -1;
      }
    }
  }
  int openFlags = O_CREAT | O_WRONLY;
  int res = open(path.c_str(), openFlags, 0644);
  if (res < 0) {
    if (dir.empty()) {
      PLOG(ERROR) << "failed creating file " << path;
      return -1;
    }
    PLOG(ERROR) << "failed creating file " << path << ", trying to "
                << "force directory creation";
    if (!createDirRecursively(dir, true /* force */)) {
      LOG(ERROR) << "failed to create dir " << dir << " recursively";
      return -1;
    }
    res = open(path.c_str(), openFlags, 0644);
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

  CHECK(dir.back() == '/');

  size_t lastIndex = dir.size() - 1;
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
  if (code != 0 && errno != EEXIST) {
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
