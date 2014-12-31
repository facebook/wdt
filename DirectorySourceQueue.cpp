#include "DirectorySourceQueue.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <set>
#include <utility>

#include <folly/Memory.h>
#include "FileByteSource.h"
#include <regex>

namespace facebook {
namespace wdt {

DirectorySourceQueue::DirectorySourceQueue(const std::string &rootDir)
    : rootDir_(rootDir), options_(WdtOptions::get()) {
  CHECK(!rootDir_.empty());
  if (rootDir_.back() != '/') {
    rootDir_.push_back('/');
  }
  fileSourceBufferSize_ = options_.bufferSize_;
};

void DirectorySourceQueue::setIncludePattern(
    const std::string &includePattern) {
  includePattern_ = includePattern;
}

void DirectorySourceQueue::setExcludePattern(
    const std::string &excludePattern) {
  excludePattern_ = excludePattern;
}

void DirectorySourceQueue::setPruneDirPattern(
    const std::string &pruneDirPattern) {
  pruneDirPattern_ = pruneDirPattern;
}

void DirectorySourceQueue::setFileSourceBufferSize(
    const size_t fileSourceBufferSize) {
  fileSourceBufferSize_ = fileSourceBufferSize;
  CHECK(fileSourceBufferSize_ > 0);
}

void DirectorySourceQueue::setFileInfo(const std::vector<FileInfo> &fileInfo) {
  fileInfo_ = fileInfo;
}

void DirectorySourceQueue::setFollowSymlinks(const bool followSymlinks) {
  followSymlinks_ = followSymlinks;
}

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
    if (sourceQueue_.empty()) {
      conditionNotEmpty_.notify_all();
    }
  }
  VLOG(1) << "finished initialization of DirectorySourceQueue";
  return res;
}

bool DirectorySourceQueue::explore() {
  bool hasError = false;
  std::set<std::string> visited;
  std::regex includeRegex(includePattern_);
  std::regex excludeRegex(excludePattern_);
  std::regex pruneDirRegex(pruneDirPattern_);
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
        if (dirEntryRes->d_name[1] == '\0' ||
            (dirEntryRes->d_name[1] == '.' && dirEntryRes->d_name[2] == '\0'))
          VLOG(3) << "Skipping entry : " << dirEntryRes->d_name;
        continue;
      }
      // Following code is a bit ugly trying to save stat() call for directories
      // yet still work for xfs which returns DT_UNKNOWN for everything
      // would be simpler to always stat()

      // if we reach DT_DIR and DT_REG directly:
      bool isDir = (dType == DT_DIR);
      bool isLink = (dType == DT_LNK);
      bool keepEntry = (isDir || dType == DT_REG || dType == DT_UNKNOWN);
      if (followSymlinks_) {
        keepEntry |= isLink;
      }
      if (!keepEntry) {
        VLOG(3) << "Ignoring entry type " << (int)(dType);
        continue;
      }
      std::string newRelativePath =
          relativePath + std::string(dirEntryRes->d_name);
      std::string newFullPath = rootDir_ + newRelativePath;
      if (!isDir) {
        // DT_REG, DT_LNK or DT_UNKNOWN cases
        struct stat fileStat;
        // Use stat since we can also have symlinks
        if (stat(newFullPath.c_str(), &fileStat) != 0) {
          PLOG(ERROR) << "stat() failed on path " << newFullPath;
          hasError = true;
          continue;
        }

        if (followSymlinks_) {
          std::string pathToResolve = newFullPath;
          if (dType == DT_UNKNOWN) {
            // Use lstat because we are checking the file itself
            // and not what it points to (if it is a link)
            struct stat linkStat;
            if (lstat(pathToResolve.c_str(), &linkStat) != 0) {
              PLOG(ERROR) << "lstat() failed on path " << pathToResolve;
              hasError = true;
              continue;
            }
            if (S_ISLNK(linkStat.st_mode)) {
              // Let's resolve it below
              isLink = true;
            }
          }
          if (isLink) {
            // Use realpath() as it resolves to a nice canonicalized
            // full path we can used for the stat() call later,
            // readlink could still give us a relative path
            // and making sure the output buffer is sized appropriately
            // can be ugly
            char *resolvedPath = realpath(pathToResolve.c_str(), nullptr);
            if (!resolvedPath) {
              hasError = true;
              PLOG(ERROR) << "Couldn't resolve " << pathToResolve.c_str();
              continue;
            }
            newFullPath.assign(resolvedPath);
            free(resolvedPath);
            VLOG(2) << "Resolved symlink " << dirEntryRes->d_name << " to "
                    << newFullPath;
          }
        }

        // could dcheck that if DT_REG we better be !isDir
        isDir = S_ISDIR(fileStat.st_mode);
        // if we were DT_UNKNOWN this could still be a symlink, block device
        // etc... (xfs)
        if (S_ISREG(fileStat.st_mode)) {
          VLOG(1) << "Found file " << newFullPath << " of size "
                  << fileStat.st_size;
          if (!excludePattern_.empty() &&
              std::regex_match(newRelativePath, excludeRegex)) {
            continue;
          }
          if (!includePattern_.empty() &&
              !std::regex_match(newRelativePath, includeRegex)) {
            continue;
          }
          createIntoQueue(newRelativePath, fileStat.st_size);
          conditionNotEmpty_.notify_one();
          continue;
        }
      }
      if (isDir) {
        if (followSymlinks_) {
          if (visited.find(newFullPath) != visited.end()) {
            LOG(ERROR) << "Attempted to visit directory twice: " << newFullPath;
            hasError = true;
            continue;
          }
          // TODO: consider custom hashing ignoring common prefix
          visited.insert(newFullPath);
        }
        newRelativePath.push_back('/');
        if (pruneDirPattern_.empty() ||
            !std::regex_match(newRelativePath, pruneDirRegex)) {
          VLOG(1) << "Adding " << newRelativePath;
          todoList.push_back(std::move(newRelativePath));
        }
      }
    }
    closedir(dirPtr);
  }
  VLOG(1) << "All done... errors = " << hasError;
  return !hasError;
}

void DirectorySourceQueue::returnToQueue(std::unique_ptr<ByteSource> &source) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t retries = source->getTransferStats().getFailedAttempts();
  if (retries >= options_.maxTransferRetries_) {
    LOG(ERROR) << source->getIdentifier() << " failed after " << retries
               << " number of tries.";
    failedSourceStats_.emplace_back(source->getTransferStats());
  } else {
    sourceQueue_.push(std::move(source));
  }
}

void DirectorySourceQueue::createIntoQueue(const std::string &relativePath,
                                           const size_t fileSize) {
  std::unique_ptr<ByteSource> source = folly::make_unique<FileByteSource>(
      rootDir_, relativePath, fileSize, fileSourceBufferSize_);
  std::lock_guard<std::mutex> lock(mutex_);
  sourceQueue_.push(std::move(source));
  numEntries_++;
}

const std::vector<TransferStats> &DirectorySourceQueue::getFailedSourceStats() {
  while (!sourceQueue_.empty()) {
    failedSourceStats_.emplace_back(sourceQueue_.top()->getTransferStats());
    sourceQueue_.pop();
  }
  return failedSourceStats_;
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
    createIntoQueue(info.first, filesize);
    conditionNotEmpty_.notify_one();
  }
  return true;
}

bool DirectorySourceQueue::finished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_ && sourceQueue_.empty();
}

std::unique_ptr<ByteSource> DirectorySourceQueue::getNextSource() {
  std::unique_lock<std::mutex> lock(mutex_);
  while (sourceQueue_.empty() && !initFinished_) {
    conditionNotEmpty_.wait(lock);
  }
  if (sourceQueue_.empty()) {
    return nullptr;
  }
  // using const_cast since priority_queue returns a const reference
  std::unique_ptr<ByteSource> source =
      std::move(const_cast<std::unique_ptr<ByteSource> &>(sourceQueue_.top()));
  sourceQueue_.pop();
  if (sourceQueue_.empty() && initFinished_) {
    conditionNotEmpty_.notify_all();
  }
  VLOG(1) << "got next source " << rootDir_ + source->getIdentifier()
          << " size " << source->getSize();
  return source;
}
}
}
