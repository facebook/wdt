#include "DirectorySourceQueue.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "FileByteSource.h"

namespace facebook {
namespace wdt {

bool DirectorySourceQueue::init() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initCalled_) {
      return false;
    }
    initCalled_ = true;
  }
  LOG(INFO) << "starting initialization of DirectorySourceQueue";
  bool res = recurseOnPath(); // by default start on root/empty relative path
  {
    std::lock_guard<std::mutex> lock(mutex_);
    initFinished_ = true;
    if (sizeToPath_.empty()) {
      conditionNotEmpty_.notify_all();
    }
  }
  LOG(INFO) << "finished initialization of DirectorySourceQueue";
  return res;
}

bool DirectorySourceQueue::recurseOnPath(const std::string& relativePath) {
  const std::string fullPath = rootDir_ + relativePath;
  LOG(INFO) << "recursing on directory " << fullPath;
  DIR* dirPtr = opendir(fullPath.c_str());
  if (!dirPtr) {
    PLOG(ERROR) << "error opening dir " << fullPath;
    return false;
  }
  dirent dirEntry;
  dirent* dirEntryPtr = nullptr;
  while (true) {
    if (readdir_r(dirPtr, &dirEntry, &dirEntryPtr) != 0) {
      PLOG(ERROR) << "error reading dir " << fullPath;
      closedir(dirPtr);
      return false;
    }
    if (!dirEntryPtr) {
      // finished reading dir
      break;
    }
    if (dirEntry.d_name[0] == '.') {
      continue;
    }
    const std::string newRelativePath = relativePath
                                        + std::string(dirEntry.d_name);
    const std::string newFullPath = rootDir_ + newRelativePath;
    if (dirEntry.d_type == DT_REG) {
      // regular file, put size/path info in sizeToPath_ member
      struct stat fileStat;
      if (stat(newFullPath.c_str(), &fileStat) != 0) {
        PLOG(ERROR) << "stat failed on path " << newFullPath;
        return false;
      }
      LOG(INFO) << "found file " << newFullPath << " of size "
                << fileStat.st_size;
      {
        std::lock_guard<std::mutex> lock(mutex_);
        sizeToPath_.push(
          std::make_pair((uint64_t)fileStat.st_size, newRelativePath));
      }
      conditionNotEmpty_.notify_one();
    } else if (dirEntry.d_type == DT_DIR) {
      if (!recurseOnPath(newRelativePath + "/")) {
        closedir(dirPtr);
        return false;
      }
    }
  }
  closedir(dirPtr);
  return true;
}

bool DirectorySourceQueue::finished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_ && sizeToPath_.empty();
}

std::unique_ptr<ByteSource> DirectorySourceQueue::getNextSource() {
  uint64_t filesize;
  std::string filename;
  {
    std::unique_lock<std::mutex> lock(mutex_);
    while (sizeToPath_.empty() && !initFinished_) {
      conditionNotEmpty_.wait(lock);
    }
    if (sizeToPath_.empty()) {
      return nullptr;
    }
    auto pair = sizeToPath_.top();
    sizeToPath_.pop();
    if (sizeToPath_.empty() && initFinished_) {
      conditionNotEmpty_.notify_all();
    }
    filesize = pair.first;
    filename = pair.second;
  }
  LOG(INFO) << "got next source " << rootDir_ + filename << " size "
            << filesize;
  return std::unique_ptr<FileByteSource>(
    new FileByteSource(rootDir_, filename, filesize, fileSourceBufferSize_));
}
}
}
