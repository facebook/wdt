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

std::thread DirectorySourceQueue::buildQueueAsynchronously() {
  // relying on RVO (and thread not copyable to avoid multiple ones)
  return std::thread(&DirectorySourceQueue::buildQueueSynchronously, this);
}

bool DirectorySourceQueue::buildQueueSynchronously() {
  VLOG(1) << "buildQueueSynchronously() called";
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (initCalled_) {
      return false;
    }
    initCalled_ = true;
  }
  bool res = false;
  // either traverse directory or we already have a fixed set of candidate
  // files
  if (!fileInfo_.empty()) {
    res = enqueueFiles();
  } else {
    res = explore();
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    initFinished_ = true;
    // TODO: comment why
    if (sizeToPath_.empty()) {
      conditionNotEmpty_.notify_all();
    }
  }
  VLOG(1) << "finished initialization of DirectorySourceQueue";
  return res;
}

bool DirectorySourceQueue::explore() {
  bool hasError = false;
  std::deque<std::string> todoList;
  todoList.push_back("");
  while (!todoList.empty()) {
    // would be nice to do those 2 in 1 call...
    auto relativePath = todoList.front();
    todoList.pop_front();
    const std::string fullPath = rootDir_ + relativePath;
    VLOG(1) << "Processing directory " << fullPath;
    DIR *dirPtr = opendir(fullPath.c_str());
    if (!dirPtr) {
      PLOG(ERROR) << "Error opening dir " << fullPath;
      hasError = true;
      continue;
    }
    // http://elliotth.blogspot.com/2012/10/how-not-to-use-readdirr3.html
    // tl;dr readdir is actually better than readdir_r ! (because of the
    // nastyness of calculating correctly buffer size and race conditions there)
    struct dirent *dirEntryRes = nullptr;
    while (true) {
      errno = 0;  // yes that's right
      dirEntryRes = readdir(dirPtr);
      if (!dirEntryRes) {
        if (errno) {
          PLOG(ERROR) << "Error reading dir " << fullPath;
          // closedir always called
          hasError = true;
        } else {
          VLOG(2) << "Done with " << fullPath;
          // finished reading dir
        }
        break;
      }
      const auto dType = dirEntryRes->d_type;
      VLOG(2) << "Found entry " << dirEntryRes->d_name << " type "
              << (int)dType;
      if (dirEntryRes->d_name[0] == '.') {
        VLOG(3) << "Skipping entry starting with . : " << dirEntryRes->d_name;
        continue;
      }
      // Following code is a bit ugly trying to save stat() call for directories
      // yet still work for xfs which returns DT_UNKNOWN for everything
      // would be simpler to always stat()

      // if we reach DT_DIR and DT_REG directly:
      bool isDir = (dType == DT_DIR);
      if (!isDir && dType != DT_REG && dType != DT_UNKNOWN) {
        VLOG(3) << "Ignoring entry type " << (int)(dType);
        continue;
      }
      std::string newRelativePath =
          relativePath + std::string(dirEntryRes->d_name);
      if (!isDir) {
        // DT_REG or DT_UNKNOWN cases
        const std::string newFullPath = rootDir_ + newRelativePath;
        struct stat fileStat;
        if (stat(newFullPath.c_str(), &fileStat) != 0) {
          PLOG(ERROR) << "stat() failed on path " << newFullPath;
          hasError = true;
          continue;
        }
        // could dcheck that if DT_REG we better be !isDir
        isDir = S_ISDIR(fileStat.st_mode);
        // if we were DT_UNKNOWN this could still be a block device etc... (xfs)
        if (S_ISREG(fileStat.st_mode)) {
          VLOG(1) << "Found reg file " << newFullPath << " of size "
                  << fileStat.st_size;
          {
            std::lock_guard<std::mutex> lock(mutex_);
            sizeToPath_.push(
                std::make_pair((uint64_t)fileStat.st_size, newRelativePath));
            ++numEntries_;
          }
          conditionNotEmpty_.notify_one();
          continue;
        }
      }
      if (isDir) {
        newRelativePath.push_back('/');
        VLOG(3) << "Adding " << newRelativePath;
        todoList.push_back(std::move(newRelativePath));
      }
    }
    closedir(dirPtr);
  }
  VLOG(1) << "All done... errors = " << hasError;
  return !hasError;
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
      ++numEntries_;
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
  VLOG(1) << "got next source " << rootDir_ + filename << " size " << filesize;
  return std::unique_ptr<FileByteSource>(
      new FileByteSource(rootDir_, filename, filesize, fileSourceBufferSize_));
}
}
}
