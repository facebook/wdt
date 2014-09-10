#include "DirectorySourceQueue.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>

#include "FileByteSource.h"

namespace facebook {
namespace wdt {

DirectorySourceQueue::DirectorySourceQueue(
    const std::string &rootDir, size_t fileSourceBufferSize,
    const std::vector<FileInfo> &fileInfo)
    : rootDir_(rootDir),
      fileSourceBufferSize_(fileSourceBufferSize),
      fileInfo_(fileInfo) {
  CHECK(!rootDir_.empty() || !fileInfo_.empty());
  CHECK(fileSourceBufferSize_ > 0);
  if (rootDir_.back() != '/') {
    rootDir_.push_back('/');
  }
};

bool DirectorySourceQueue::init() {
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initCalled_) {
      return false;
    }
    initCalled_ = true;
  }
  LOG(INFO) << "starting initialization of DirectorySourceQueue";
  bool res = false;
  if (!fileInfo_.empty()) {
    res = enqueueFiles();
  } else {
    // by default start on root/empty relative path
    res = recurseOnPath();
  }
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

// TODO consider reusing the dirent buffer and the path strings
bool DirectorySourceQueue::recurseOnPath(std::string relativePath) {
  const std::string fullPath = rootDir_ + relativePath;
  LOG(INFO) << "Recursing on directory " << fullPath;
  DIR *dirPtr = opendir(fullPath.c_str());
  if (!dirPtr) {
    PLOG(ERROR) << "error opening dir " << fullPath;
    return false;
  }
  // From readdir_r(2) [seems size can change in theory
  // depending how deep we are]
  const size_t entryLen = offsetof(struct dirent, d_name) +
                          pathconf(fullPath.c_str(), _PC_NAME_MAX) + 1;
  std::unique_ptr<char[]> buf(new char[entryLen]);
  dirent *dirEntryPtr = (dirent *)buf.get();
  dirent *dirEntryRes = nullptr;

  while (true) {
    if (readdir_r(dirPtr, dirEntryPtr, &dirEntryRes) != 0) {
      PLOG(ERROR) << "error reading dir " << fullPath;
      closedir(dirPtr);
      return false;
    }
    if (!dirEntryRes) {
      VLOG(1) << "Done with " << fullPath;
      // finished reading dir
      break;
    }
    const auto dType = dirEntryRes->d_type;
    VLOG(1) << "Found entry " << dirEntryRes->d_name << " type " << (int)dType;
    if (dirEntryRes->d_name[0] == '.') {
      VLOG(1) << "Skipping entry starting with . : " << dirEntryRes->d_name;
      continue;
    }
    // Following code is a bit ugly trying to save stat() call for directories
    // yet still work for xfs which returns DT_UNKNOWN for everything
    // would be simpler to always stat()

    // if we reach DT_DIR and DT_REG directly:
    bool isDir = (dType == DT_DIR);
    if (!isDir && dType != DT_REG && dType != DT_UNKNOWN) {
      VLOG(1) << "Ignoring entry type " << (int)(dType);
      continue;
    }
    std::string newRelativePath =
        relativePath + std::string(dirEntryRes->d_name);
    if (!isDir) {
      // DT_REG or DT_UNKNOWN cases
      const std::string newFullPath = rootDir_ + newRelativePath;
      struct stat fileStat;
      if (stat(newFullPath.c_str(), &fileStat) != 0) {
        PLOG(ERROR) << "stat failed on path " << newFullPath;
        return false;
      }
      // could dcheck that if DT_REG we better be !isDir
      isDir = S_ISDIR(fileStat.st_mode);
      // if we were DT_UNKNOWN this could still be a block device etc... (xfs)
      if (S_ISREG(fileStat.st_mode)) {
        LOG(INFO) << "Found reg file " << newFullPath << " of size "
                  << fileStat.st_size;
        {
          std::lock_guard<std::mutex> lock(mutex_);
          sizeToPath_.push(
              std::make_pair((uint64_t)fileStat.st_size, newRelativePath));
        }
        conditionNotEmpty_.notify_one();
        continue;
      }
    }
    if (isDir) {
      newRelativePath.push_back('/');
      if (!recurseOnPath(std::move(newRelativePath))) {
        closedir(dirPtr);
        return false;
      }
    }
  }
  closedir(dirPtr);
  return true;
}

bool DirectorySourceQueue::enqueueFiles() {
  for (const auto &info : fileInfo_) {
    const auto &fullPath = rootDir_ + info.first;
    uint64_t filesize;
    if (info.second < 0) {
      struct stat fileStat;
      if (stat(fullPath.c_str(), &fileStat) != 0) {
        PLOG(ERROR) << "stat failed on path " << fullPath;
        return false;
      }
      filesize = fileStat.st_size;
    } else {
      filesize = info.second;
    }
    {
      std::lock_guard<std::mutex> lock(mutex_);
      sizeToPath_.push(std::make_pair(filesize, info.first));
    }
    conditionNotEmpty_.notify_one();
  }
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
