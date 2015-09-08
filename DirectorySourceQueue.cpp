/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "DirectorySourceQueue.h"

#include "Protocol.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <set>
#include <algorithm>
#include <utility>

#include <folly/Memory.h>
#include <regex>
#include <fcntl.h>

namespace facebook {
namespace wdt {

using std::string;

FileInfo::FileInfo(const string &name, int64_t size)
    : fileName(name), fileSize(size) {
  oFlags = O_RDONLY;
  const auto &options = WdtOptions::get();
  if (options.odirect_reads) {
    oFlags |= O_DIRECT;
  }
}

void FileInfo::verifyAndFixFlags() {
  if (oFlags & ~(O_DIRECT | O_RDONLY)) {
    LOG(WARNING) << "Flags apart from O_RDONLY and O_DIRECT "
                 << "provided, for " << fileName
                 << ". Wdt will ignore the extra flags";
    oFlags &= (O_RDONLY | O_DIRECT);
  }
#ifndef WDT_SUPPORTS_ODIRECT
  bool hasOdirect = oFlags & O_DIRECT;
  if (hasOdirect) {
    LOG(WARNING) << "Can't read " << fileName << " in O_DIRECT"
                 << ". Memalign not found, turning O_DIRECT flag"
                 << " off for this file";
    oFlags &= ~(O_DIRECT);
  }
#endif
}

DirectorySourceQueue::DirectorySourceQueue(
    const string &rootDir, std::unique_ptr<IAbortChecker> &abortChecker)
    : abortChecker_(std::move(abortChecker)), options_(WdtOptions::get()) {
  setRootDir(rootDir);
  fileSourceBufferSize_ = options_.buffer_size;
};

void DirectorySourceQueue::setIncludePattern(const string &includePattern) {
  includePattern_ = includePattern;
}

void DirectorySourceQueue::setExcludePattern(const string &excludePattern) {
  excludePattern_ = excludePattern;
}

void DirectorySourceQueue::setPruneDirPattern(const string &pruneDirPattern) {
  pruneDirPattern_ = pruneDirPattern;
}

void DirectorySourceQueue::setFileSourceBufferSize(
    const int64_t fileSourceBufferSize) {
  fileSourceBufferSize_ = fileSourceBufferSize;
  CHECK(fileSourceBufferSize_ > 0);
}

void DirectorySourceQueue::setFileInfo(const std::vector<FileInfo> &fileInfo) {
  fileInfo_ = fileInfo;
}

const std::vector<FileInfo> &DirectorySourceQueue::getFileInfo() const {
  return fileInfo_;
}

void DirectorySourceQueue::setFollowSymlinks(const bool followSymlinks) {
  followSymlinks_ = followSymlinks;
  if (followSymlinks_) {
    setRootDir(rootDir_);
  }
}

// const ref string param but first thing we do is make a copy because
// of logging original input vs resolved one
bool DirectorySourceQueue::setRootDir(const string &newRootDir) {
  if (newRootDir.empty()) {
    LOG(ERROR) << "Invalid empty root dir!";
    return false;
  }
  string dir(newRootDir);
  if (followSymlinks_) {
    dir.assign(resolvePath(newRootDir));
    if (dir.empty()) {
      // error already logged
      return false;
    }
    LOG(INFO) << "Following symlinks " << newRootDir << " -> " << dir;
  }
  if (dir.back() != '/') {
    dir.push_back('/');
  }
  if (dir != rootDir_) {
    rootDir_.assign(dir);
    LOG(INFO) << "Root dir now " << rootDir_;
  }
  return true;
}

void DirectorySourceQueue::setPreviouslyReceivedChunks(
    std::vector<FileChunksInfo> &previouslyTransferredChunks) {
  std::unique_lock<std::mutex> lock(mutex_);
  WDT_CHECK_EQ(0, numBlocksDequeued_);
  // reset all the queue variables
  nextSeqId_ = 0;
  totalFileSize_ = 0;
  numEntries_ = 0;
  numBlocks_ = 0;
  for (auto &chunkInfo : previouslyTransferredChunks) {
    nextSeqId_ = std::max(nextSeqId_, chunkInfo.getSeqId() + 1);
    previouslyTransferredChunks_.insert(
        std::make_pair(chunkInfo.getFileName(), std::move(chunkInfo)));
  }
  // clear current content of the queue. For some reason, priority_queue does
  // not have a clear method
  while (!sourceQueue_.empty()) {
    sourceQueue_.pop();
  }
  std::vector<SourceMetaData *> discoveredFileData = std::move(sharedFileData_);
  // recreate the queue
  for (const auto fileData : discoveredFileData) {
    FileInfo fileInfo(fileData->relPath, fileData->size);
    createIntoQueue(fileData->fullPath, fileInfo, true);
    delete fileData;
  }
}

DirectorySourceQueue::~DirectorySourceQueue() {
  for (SourceMetaData *fileData : sharedFileData_) {
    delete fileData;
  }
}

std::thread DirectorySourceQueue::buildQueueAsynchronously() {
  // relying on RVO (and thread not copyable to avoid multiple ones)
  return std::thread(&DirectorySourceQueue::buildQueueSynchronously, this);
}

bool DirectorySourceQueue::buildQueueSynchronously() {
  auto startTime = Clock::now();
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
    LOG(INFO) << "Using list of file info. Number of files "
              << fileInfo_.size();
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
  directoryTime_ = durationSeconds(Clock::now() - startTime);
  VLOG(1) << "finished initialization of DirectorySourceQueue in "
          << directoryTime_;
  return res;
}

// TODO: move this and a bunch of stuff into FileUtil and/or System class
string DirectorySourceQueue::resolvePath(const string &path) {
  // Use realpath() as it resolves to a nice canonicalized
  // full path we can used for the stat() call later,
  // readlink could still give us a relative path
  // and making sure the output buffer is sized appropriately
  // can be ugly
  string result;
  char *resolvedPath = realpath(path.c_str(), nullptr);
  if (!resolvedPath) {
    PLOG(ERROR) << "Couldn't resolve " << path;
    return result;  // empty string == error
  }
  result.assign(resolvedPath);
  free(resolvedPath);
  VLOG(3) << "resolvePath(\"" << path << "\") -> " << result;
  return result;
}

bool DirectorySourceQueue::explore() {
  LOG(INFO) << "Exploring root dir " << rootDir_
            << " include_pattern : " << includePattern_
            << " exclude_pattern : " << excludePattern_
            << " prune_dir_pattern : " << pruneDirPattern_;
  WDT_CHECK(!rootDir_.empty());
  bool hasError = false;
  std::set<string> visited;
  std::regex includeRegex(includePattern_);
  std::regex excludeRegex(excludePattern_);
  std::regex pruneDirRegex(pruneDirPattern_);
  std::deque<string> todoList;
  todoList.push_back("");
  while (!todoList.empty()) {
    if (abortChecker_->shouldAbort()) {
      LOG(ERROR) << "Directory transfer thread aborted";
      hasError = true;
      break;
    }
    // would be nice to do those 2 in 1 call...
    auto relativePath = todoList.front();
    todoList.pop_front();
    const string fullPath = rootDir_ + relativePath;
    VLOG(1) << "Processing directory " << fullPath;
    DIR *dirPtr = opendir(fullPath.c_str());
    if (!dirPtr) {
      PLOG(ERROR) << "Error opening dir " << fullPath;
      failedDirectories_.emplace_back(fullPath);
      hasError = true;
      continue;
    }
    // http://elliotth.blogspot.com/2012/10/how-not-to-use-readdirr3.html
    // tl;dr readdir is actually better than readdir_r ! (because of the
    // nastiness of calculating correctly buffer size and race conditions there)
    struct dirent *dirEntryRes = nullptr;
    while (true) {
      if (abortChecker_->shouldAbort()) {
        break;
      }
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
            (dirEntryRes->d_name[1] == '.' && dirEntryRes->d_name[2] == '\0')) {
          VLOG(3) << "Skipping entry : " << dirEntryRes->d_name;
          continue;
        }
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
      string newRelativePath = relativePath + string(dirEntryRes->d_name);
      string newFullPath = rootDir_ + newRelativePath;
      if (!isDir) {
        // DT_REG, DT_LNK or DT_UNKNOWN cases
        struct stat fileStat;
        // On XFS we don't know yet if this is a symlink, so check
        // if following symlinks is ok we will do stat() too
        if (lstat(newFullPath.c_str(), &fileStat) != 0) {
          PLOG(ERROR) << "lstat() failed on path " << newFullPath;
          hasError = true;
          continue;
        }
        isLink = S_ISLNK(fileStat.st_mode);
        VLOG(2) << "lstat for " << newFullPath << " is link ? " << isLink;
        if (followSymlinks_ && isLink) {
          // Use stat to see if the pointed file is of the right type
          // (overrides previous stat call result)
          if (stat(newFullPath.c_str(), &fileStat) != 0) {
            PLOG(ERROR) << "stat() failed on path " << newFullPath;
            hasError = true;
            continue;
          }
          newFullPath = resolvePath(newFullPath);
          if (newFullPath.empty()) {
            // already logged error
            hasError = true;
            continue;
          }
          VLOG(2) << "Resolved symlink " << dirEntryRes->d_name << " to "
                  << newFullPath;
        }

        // could dcheck that if DT_REG we better be !isDir
        isDir = S_ISDIR(fileStat.st_mode);
        // if we were DT_UNKNOWN this could still be a symlink, block device
        // etc... (xfs)
        if (S_ISREG(fileStat.st_mode)) {
          VLOG(2) << "Found file " << newFullPath << " of size "
                  << fileStat.st_size;
          if (!excludePattern_.empty() &&
              std::regex_match(newRelativePath, excludeRegex)) {
            continue;
          }
          if (!includePattern_.empty() &&
              !std::regex_match(newRelativePath, includeRegex)) {
            continue;
          }
          FileInfo fileInfo(newRelativePath, fileStat.st_size);
          createIntoQueue(newFullPath, fileInfo, false);
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
          VLOG(2) << "Adding " << newRelativePath;
          todoList.push_back(std::move(newRelativePath));
        }
      }
    }
    closedir(dirPtr);
  }
  LOG(INFO) << "Number of files explored: " << numEntries_
            << ", errors: " << std::boolalpha << hasError;
  return !hasError;
}

void DirectorySourceQueue::smartNotify(int32_t addedSource) {
  if (addedSource >= options_.num_ports) {
    conditionNotEmpty_.notify_all();
    return;
  }
  for (int i = 0; i < addedSource; i++) {
    conditionNotEmpty_.notify_one();
  }
}

void DirectorySourceQueue::returnToQueue(
    std::vector<std::unique_ptr<ByteSource>> &sources) {
  int returnedCount = 0;
  std::unique_lock<std::mutex> lock(mutex_);
  for (auto &source : sources) {
    const int64_t retries = source->getTransferStats().getFailedAttempts();
    if (retries >= options_.max_transfer_retries) {
      LOG(ERROR) << source->getIdentifier() << " failed after " << retries
                 << " number of tries.";
      failedSourceStats_.emplace_back(std::move(source->getTransferStats()));
    } else {
      sourceQueue_.push(std::move(source));
      returnedCount++;
    }
    WDT_CHECK_GT(numBlocksDequeued_, 0);
    numBlocksDequeued_--;
  }
  lock.unlock();
  smartNotify(returnedCount);
}

void DirectorySourceQueue::returnToQueue(std::unique_ptr<ByteSource> &source) {
  std::vector<std::unique_ptr<ByteSource>> sources;
  sources.emplace_back(std::move(source));
  returnToQueue(sources);
}

void DirectorySourceQueue::createIntoQueue(const string &fullPath,
                                           FileInfo &fileInfo,
                                           bool alreadyLocked) {
  // TODO: currently we are treating small files(size less than blocksize) as
  // blocks. Also, we transfer file name in the header for all the blocks for a
  // large file. This can be optimized as follows -
  // a) if filesize < blocksize, we do not send blocksize and offset in the
  // header. This should be useful for tiny files(0-few hundred bytes). We will
  // have to use separate header format and commands for files and blocks.
  // b) if filesize > blocksize, we can use send filename only in the first
  // block and use a shorter header for subsequent blocks. Also, we can remove
  // block size once negotiated, since blocksize is sort of fixed.
  fileInfo.verifyAndFixFlags();
  auto &fileSize = fileInfo.fileSize;
  auto &relPath = fileInfo.fileName;
  int64_t blockSizeBytes = options_.block_size_mbytes * 1024 * 1024;
  bool enableBlockTransfer = blockSizeBytes > 0;
  if (!enableBlockTransfer) {
    VLOG(2) << "Block transfer disabled for this transfer";
  }
  // if block transfer is disabled, treating fileSize as block size. This
  // ensures that we create a single block
  auto blockSize = enableBlockTransfer ? blockSizeBytes : fileSize;
  int blockCount = 0;
  std::unique_lock<std::mutex> lock(mutex_, std::defer_lock);
  if (!alreadyLocked) {
    lock.lock();
  }
  std::vector<Interval> remainingChunks;
  int64_t seqId;
  FileAllocationStatus allocationStatus;
  int64_t prevSeqId = 0;
  auto it = previouslyTransferredChunks_.find(relPath);
  if (it == previouslyTransferredChunks_.end()) {
    // No previously transferred chunks
    remainingChunks.emplace_back(0, fileSize);
    seqId = nextSeqId_++;
    allocationStatus = NOT_EXISTS;
  } else if (it->second.getFileSize() > fileSize) {
    // file size is greater on the receiver side
    remainingChunks.emplace_back(0, fileSize);
    seqId = nextSeqId_++;
    LOG(INFO) << "File size is greater in the receiver side " << relPath << " "
              << fileSize << " " << it->second.getFileSize();
    allocationStatus = EXISTS_TOO_LARGE;
    prevSeqId = it->second.getSeqId();
  } else {
    auto &fileChunksInfo = it->second;
    remainingChunks = fileChunksInfo.getRemainingChunks(fileSize);
    if (remainingChunks.empty()) {
      LOG(INFO) << relPath << " completely sent in previous transfer";
      return;
    }
    seqId = fileChunksInfo.getSeqId();
    allocationStatus = it->second.getFileSize() < fileSize
                           ? EXISTS_TOO_SMALL
                           : EXISTS_CORRECT_SIZE;
  }

  SourceMetaData *metadata = new SourceMetaData();
  metadata->fullPath = fullPath;
  metadata->relPath = relPath;
  metadata->seqId = seqId;
  metadata->oFlags = fileInfo.oFlags;
  metadata->size = fileSize;
  metadata->allocationStatus = allocationStatus;
  metadata->prevSeqId = prevSeqId;
  sharedFileData_.emplace_back(metadata);
  for (const auto &chunk : remainingChunks) {
    int64_t offset = chunk.start_;
    int64_t remainingBytes = chunk.size();
    do {
      const int64_t size = std::min<int64_t>(remainingBytes, blockSize);
      std::unique_ptr<ByteSource> source = folly::make_unique<FileByteSource>(
          metadata, size, offset, fileSourceBufferSize_);
      sourceQueue_.push(std::move(source));
      remainingBytes -= size;
      offset += size;
      blockCount++;
    } while (remainingBytes > 0);
    totalFileSize_ += chunk.size();
  }
  numEntries_++;
  numBlocks_ += blockCount;
  if (!alreadyLocked) {
    lock.unlock();
  }
  smartNotify(blockCount);
}

std::vector<TransferStats> &DirectorySourceQueue::getFailedSourceStats() {
  while (!sourceQueue_.empty()) {
    failedSourceStats_.emplace_back(
        std::move(sourceQueue_.top()->getTransferStats()));
    sourceQueue_.pop();
  }
  return failedSourceStats_;
}

std::vector<string> &DirectorySourceQueue::getFailedDirectories() {
  return failedDirectories_;
}

bool DirectorySourceQueue::enqueueFiles() {
  for (auto &info : fileInfo_) {
    if (abortChecker_->shouldAbort()) {
      LOG(ERROR) << "Directory transfer thread aborted";
      return false;
    }
    string fullPath = rootDir_ + info.fileName;
    if (info.fileSize < 0) {
      struct stat fileStat;
      if (stat(fullPath.c_str(), &fileStat) != 0) {
        PLOG(ERROR) << "stat failed on path " << fullPath;
        return false;
      }
      info.fileSize = fileStat.st_size;
    }
    createIntoQueue(fullPath, info, false);
  }
  return true;
}

bool DirectorySourceQueue::finished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_ && sourceQueue_.empty();
}

int64_t DirectorySourceQueue::getCount() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return numEntries_;
}

std::pair<int64_t, ErrorCode> DirectorySourceQueue::getNumBlocksAndStatus()
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  ErrorCode status = OK;
  if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
    status = ERROR;
  }
  return std::make_pair(numBlocks_, status);
}

int64_t DirectorySourceQueue::getTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return totalFileSize_;
}

bool DirectorySourceQueue::fileDiscoveryFinished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_;
}

std::unique_ptr<ByteSource> DirectorySourceQueue::getNextSource(
    ErrorCode &status) {
  std::unique_ptr<ByteSource> source;
  while (true) {
    std::unique_lock<std::mutex> lock(mutex_);
    while (sourceQueue_.empty() && !initFinished_) {
      conditionNotEmpty_.wait(lock);
    }
    if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
      status = ERROR;
    } else {
      status = OK;
    }
    if (sourceQueue_.empty()) {
      return nullptr;
    }
    // using const_cast since priority_queue returns a const reference
    source = std::move(
        const_cast<std::unique_ptr<ByteSource> &>(sourceQueue_.top()));
    sourceQueue_.pop();
    if (sourceQueue_.empty() && initFinished_) {
      conditionNotEmpty_.notify_all();
    }
    lock.unlock();
    VLOG(1) << "got next source " << rootDir_ + source->getIdentifier()
            << " size " << source->getSize();
    // try to open the source
    if (source->open() == OK) {
      lock.lock();
      numBlocksDequeued_++;
      return source;
    }
    source->close();
    // we need to lock again as we will be adding element to failedSourceStats
    // vector
    lock.lock();
    failedSourceStats_.emplace_back(std::move(source->getTransferStats()));
  }
}
}
}
