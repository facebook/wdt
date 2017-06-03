/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/DirectorySourceQueue.h>

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <wdt/Protocol.h>
#include <algorithm>
#include <set>
#include <utility>

#include <fcntl.h>
#include <regex>

// NOTE: this should remain standalone code and not use WdtOptions directly
// also note this is used not just by the Sender but also by the receiver
// (so code like opening files during discovery is disabled by default and
// no reading the config directly from the options and only set by the Sender)

namespace facebook {
namespace wdt {

using std::string;

WdtFileInfo::WdtFileInfo(const string &name, int64_t size, bool doDirectReads)
    : fileName(name), fileSize(size), directReads(doDirectReads) {
}

WdtFileInfo::WdtFileInfo(int fd, int64_t size, const string &name)
    : WdtFileInfo(name, size, false) {
  this->fd = fd;
}

void WdtFileInfo::verifyAndFixFlags() {
  if (fd >= 0) {
#ifdef O_DIRECT
    int flags = fcntl(fd, F_GETFL, 0);
    // directReads does not depend on the option in this case
    directReads = (flags & O_DIRECT);
// Do not have to worry about F_NOCACHE, since it has no alignment
// requirement
#endif
  }
  if (directReads) {
#ifndef WDT_SUPPORTS_ODIRECT
    WLOG(WARNING) << "Wdt can't handle O_DIRECT in this system. " << fileName;
    directReads = false;
#endif
  }
}

DirectorySourceQueue::DirectorySourceQueue(const WdtOptions &options,
                                           const string &rootDir,
                                           IAbortChecker const *abortChecker) {
  threadCtx_ =
      std::make_unique<ThreadCtx>(options, /* do not allocate buffer */ false);
  threadCtx_->setAbortChecker(abortChecker);
  setRootDir(rootDir);
}

void DirectorySourceQueue::setIncludePattern(const string &includePattern) {
  includePattern_ = includePattern;
}

void DirectorySourceQueue::setExcludePattern(const string &excludePattern) {
  excludePattern_ = excludePattern;
}

void DirectorySourceQueue::setPruneDirPattern(const string &pruneDirPattern) {
  pruneDirPattern_ = pruneDirPattern;
}

void DirectorySourceQueue::setBlockSizeMbytes(int64_t blockSizeMbytes) {
  blockSizeMbytes_ = blockSizeMbytes;
}

void DirectorySourceQueue::setFileInfo(
    const std::vector<WdtFileInfo> &fileInfo) {
  fileInfo_ = fileInfo;
  exploreDirectory_ = false;
}

const std::vector<WdtFileInfo> &DirectorySourceQueue::getFileInfo() const {
  return fileInfo_;
}

void DirectorySourceQueue::setFollowSymlinks(const bool followSymlinks) {
  followSymlinks_ = followSymlinks;
  if (followSymlinks_) {
    setRootDir(rootDir_);
  }
}

std::vector<SourceMetaData *>
    &DirectorySourceQueue::getDiscoveredFilesMetaData() {
  return sharedFileData_;
}

// const ref string param but first thing we do is make a copy because
// of logging original input vs resolved one
bool DirectorySourceQueue::setRootDir(const string &newRootDir) {
  if (newRootDir.empty()) {
    WLOG(ERROR) << "Invalid empty root dir!";
    return false;
  }
  string dir(newRootDir);
  if (followSymlinks_) {
    dir.assign(resolvePath(newRootDir));
    if (dir.empty()) {
      // error already logged
      return false;
    }
    WLOG(INFO) << "Following symlinks " << newRootDir << " -> " << dir;
  }
  if (dir.back() != '/') {
    dir.push_back('/');
  }
  if (dir != rootDir_) {
    rootDir_.assign(dir);
    WLOG(INFO) << "Root dir now " << rootDir_;
  }
  return true;
}

void DirectorySourceQueue::clearSourceQueue() {
  // clear current content of the queue. For some reason, priority_queue does
  // not have a clear method
  while (!sourceQueue_.empty()) {
    sourceQueue_.pop();
  }
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
    auto fileName = chunkInfo.getFileName();
    previouslyTransferredChunks_.insert(
        std::make_pair(std::move(fileName), std::move(chunkInfo)));
  }
  clearSourceQueue();
  // recreate the queue
  for (const auto metadata : sharedFileData_) {
    // TODO: do not notify inside createIntoQueueInternal. This method still
    // holds the lock, so no point in notifying
    createIntoQueueInternal(metadata);
  }
  enqueueFilesToBeDeleted();
}

DirectorySourceQueue::~DirectorySourceQueue() {
  // need to remove all the sources because they access metadata at the
  // destructor.
  clearSourceQueue();
  for (SourceMetaData *fileData : sharedFileData_) {
    if (fileData->needToClose && fileData->fd >= 0) {
      int ret = ::close(fileData->fd);
      if (ret) {
        WPLOG(ERROR) << "Failed to close file " << fileData->fullPath;
      }
    }
    delete fileData;
  }
}

std::thread DirectorySourceQueue::buildQueueAsynchronously() {
  // relying on RVO (and thread not copyable to avoid multiple ones)
  return std::thread(&DirectorySourceQueue::buildQueueSynchronously, this);
}

bool DirectorySourceQueue::buildQueueSynchronously() {
  auto startTime = Clock::now();
  WVLOG(1) << "buildQueueSynchronously() called";
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
  if (exploreDirectory_) {
    res = explore();
  } else {
    WLOG(INFO) << "Using list of file info. Number of files "
               << fileInfo_.size();
    res = enqueueFiles();
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    initFinished_ = true;
    enqueueFilesToBeDeleted();
    // TODO: comment why
    if (sourceQueue_.empty()) {
      conditionNotEmpty_.notify_all();
    }
  }
  directoryTime_ = durationSeconds(Clock::now() - startTime);
  WVLOG(1) << "finished initialization of DirectorySourceQueue in "
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
    WPLOG(ERROR) << "Couldn't resolve " << path;
    return result;  // empty string == error
  }
  result.assign(resolvedPath);
  free(resolvedPath);
  WVLOG(3) << "resolvePath(\"" << path << "\") -> " << result;
  return result;
}

bool DirectorySourceQueue::explore() {
  WLOG(INFO) << "Exploring root dir " << rootDir_
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
    if (threadCtx_->getAbortChecker()->shouldAbort()) {
      WLOG(ERROR) << "Directory transfer thread aborted";
      hasError = true;
      break;
    }
    // would be nice to do those 2 in 1 call...
    auto relativePath = todoList.front();
    todoList.pop_front();
    const string fullPath = rootDir_ + relativePath;
    WVLOG(1) << "Processing directory " << fullPath;
    DIR *dirPtr = opendir(fullPath.c_str());
    if (!dirPtr) {
      WPLOG(ERROR) << "Error opening dir " << fullPath;
      failedDirectories_.emplace_back(fullPath);
      hasError = true;
      continue;
    }
    // http://elliotth.blogspot.com/2012/10/how-not-to-use-readdirr3.html
    // tl;dr readdir is actually better than readdir_r ! (because of the
    // nastiness of calculating correctly buffer size and race conditions there)
    struct dirent *dirEntryRes = nullptr;
    while (true) {
      if (threadCtx_->getAbortChecker()->shouldAbort()) {
        break;
      }
      errno = 0;  // yes that's right
      dirEntryRes = readdir(dirPtr);
      if (!dirEntryRes) {
        if (errno) {
          WPLOG(ERROR) << "Error reading dir " << fullPath;
          // closedir always called
          hasError = true;
        } else {
          WVLOG(2) << "Done with " << fullPath;
          // finished reading dir
        }
        break;
      }
      const auto dType = dirEntryRes->d_type;
      WVLOG(2) << "Found entry " << dirEntryRes->d_name << " type "
               << (int)dType;
      if (dirEntryRes->d_name[0] == '.') {
        if (dirEntryRes->d_name[1] == '\0' ||
            (dirEntryRes->d_name[1] == '.' && dirEntryRes->d_name[2] == '\0')) {
          WVLOG(3) << "Skipping entry : " << dirEntryRes->d_name;
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
        WVLOG(3) << "Ignoring entry type " << (int)(dType);
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
          WPLOG(ERROR) << "lstat() failed on path " << newFullPath;
          hasError = true;
          continue;
        }
        isLink = S_ISLNK(fileStat.st_mode);
        WVLOG(2) << "lstat for " << newFullPath << " is link ? " << isLink;
        if (followSymlinks_ && isLink) {
          // Use stat to see if the pointed file is of the right type
          // (overrides previous stat call result)
          if (stat(newFullPath.c_str(), &fileStat) != 0) {
            WPLOG(ERROR) << "stat() failed on path " << newFullPath;
            hasError = true;
            continue;
          }
          newFullPath = resolvePath(newFullPath);
          if (newFullPath.empty()) {
            // already logged error
            hasError = true;
            continue;
          }
          WVLOG(2) << "Resolved symlink " << dirEntryRes->d_name << " to "
                   << newFullPath;
        }

        // could dcheck that if DT_REG we better be !isDir
        isDir = S_ISDIR(fileStat.st_mode);
        // if we were DT_UNKNOWN this could still be a symlink, block device
        // etc... (xfs)
        if (S_ISREG(fileStat.st_mode)) {
          WVLOG(2) << "Found file " << newFullPath << " of size "
                   << fileStat.st_size;
          if (!excludePattern_.empty() &&
              std::regex_match(newRelativePath, excludeRegex)) {
            continue;
          }
          if (!includePattern_.empty() &&
              !std::regex_match(newRelativePath, includeRegex)) {
            continue;
          }
          WdtFileInfo fileInfo(newRelativePath, fileStat.st_size, directReads_);
          createIntoQueue(newFullPath, fileInfo);
          continue;
        }
      }
      if (isDir) {
        if (followSymlinks_) {
          if (visited.find(newFullPath) != visited.end()) {
            WLOG(ERROR) << "Attempted to visit directory twice: "
                        << newFullPath;
            hasError = true;
            continue;
          }
          // TODO: consider custom hashing ignoring common prefix
          visited.insert(newFullPath);
        }
        newRelativePath.push_back('/');
        if (pruneDirPattern_.empty() ||
            !std::regex_match(newRelativePath, pruneDirRegex)) {
          WVLOG(2) << "Adding " << newRelativePath;
          todoList.push_back(std::move(newRelativePath));
        }
      }
    }
    closedir(dirPtr);
  }
  WLOG(INFO) << "Number of files explored: " << numEntries_ << " opened "
             << numFilesOpened_ << " with direct " << numFilesOpenedWithDirect_
             << " errors " << std::boolalpha << hasError;
  return !hasError;
}

void DirectorySourceQueue::smartNotify(int32_t addedSource) {
  if (addedSource >= numClientThreads_) {
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
    sourceQueue_.push(std::move(source));
    returnedCount++;
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
                                           WdtFileInfo &fileInfo) {
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
  SourceMetaData *metadata = new SourceMetaData();
  metadata->fullPath = fullPath;
  metadata->relPath = fileInfo.fileName;
  metadata->fd = fileInfo.fd;
  metadata->directReads = fileInfo.directReads;
  metadata->size = fileInfo.fileSize;
  if ((openFilesDuringDiscovery_ != 0) && (metadata->fd < 0)) {
    metadata->fd =
        FileUtil::openForRead(*threadCtx_, fullPath, metadata->directReads);
    ++numFilesOpened_;
    if (metadata->directReads) {
      ++numFilesOpenedWithDirect_;
    }
    metadata->needToClose = (metadata->fd >= 0);
    // works for -1 up to 4B files
    if (--openFilesDuringDiscovery_ == 0) {
      WLOG(WARNING) << "Already opened " << numFilesOpened_
                    << " files, will open the reminder as they are sent";
    }
  }
  std::unique_lock<std::mutex> lock(mutex_);
  sharedFileData_.emplace_back(metadata);
  createIntoQueueInternal(metadata);
}

void DirectorySourceQueue::createIntoQueueInternal(SourceMetaData *metadata) {
  // TODO: currently we are treating small files(size less than blocksize) as
  // blocks. Also, we transfer file name in the header for all the blocks for a
  // large file. This can be optimized as follows -
  // a) if filesize < blocksize, we do not send blocksize and offset in the
  // header. This should be useful for tiny files(0-few hundred bytes). We will
  // have to use separate header format and commands for files and blocks.
  // b) if filesize > blocksize, we can use send filename only in the first
  // block and use a shorter header for subsequent blocks. Also, we can remove
  // block size once negotiated, since blocksize is sort of fixed.
  auto &fileSize = metadata->size;
  auto &relPath = metadata->relPath;
  int64_t blockSizeBytes = blockSizeMbytes_ * 1024 * 1024;
  bool enableBlockTransfer = blockSizeBytes > 0;
  if (!enableBlockTransfer) {
    WVLOG(2) << "Block transfer disabled for this transfer";
  }
  // if block transfer is disabled, treating fileSize as block size. This
  // ensures that we create a single block
  auto blockSize = enableBlockTransfer ? blockSizeBytes : fileSize;
  int blockCount = 0;
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
    WLOG(INFO) << "File size is greater in the receiver side " << relPath << " "
               << fileSize << " " << it->second.getFileSize();
    allocationStatus = EXISTS_TOO_LARGE;
    prevSeqId = it->second.getSeqId();
  } else {
    auto &fileChunksInfo = it->second;
    // Some portion of the file was sent in previous transfers. Receiver sends
    // the list of chunks to the sender. Adding all the bytes of those chunks
    // should give us the number of bytes saved due to incremental download
    previouslySentBytes_ += fileChunksInfo.getTotalChunkSize();
    remainingChunks = fileChunksInfo.getRemainingChunks(fileSize);
    if (remainingChunks.empty()) {
      WLOG(INFO) << relPath << " completely sent in previous transfer";
      return;
    }
    seqId = fileChunksInfo.getSeqId();
    allocationStatus = it->second.getFileSize() < fileSize
                           ? EXISTS_TOO_SMALL
                           : EXISTS_CORRECT_SIZE;
  }
  metadata->seqId = seqId;
  metadata->prevSeqId = prevSeqId;
  metadata->allocationStatus = allocationStatus;

  for (const auto &chunk : remainingChunks) {
    int64_t offset = chunk.start_;
    int64_t remainingBytes = chunk.size();
    do {
      const int64_t size = std::min<int64_t>(remainingBytes, blockSize);
      std::unique_ptr<ByteSource> source =
          std::make_unique<FileByteSource>(metadata, size, offset);
      sourceQueue_.push(std::move(source));
      remainingBytes -= size;
      offset += size;
      blockCount++;
    } while (remainingBytes > 0);
    totalFileSize_ += chunk.size();
  }
  numEntries_++;
  numBlocks_ += blockCount;
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
    if (threadCtx_->getAbortChecker()->shouldAbort()) {
      WLOG(ERROR) << "Directory transfer thread aborted";
      return false;
    }
    string fullPath = rootDir_ + info.fileName;
    if (info.fileSize < 0) {
      struct stat fileStat;
      if (stat(fullPath.c_str(), &fileStat) != 0) {
        WPLOG(ERROR) << "stat failed on path " << fullPath;

        TransferStats failedSourceStat(info.fileName);
        failedSourceStat.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
        {
          std::unique_lock<std::mutex> lock(mutex_);
          failedSourceStats_.emplace_back(std::move(failedSourceStat));
        }

        return false;
      }
      info.fileSize = fileStat.st_size;
    }
    createIntoQueue(fullPath, info);
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

const PerfStatReport &DirectorySourceQueue::getPerfReport() const {
  return threadCtx_->getPerfReport();
}

std::pair<int64_t, ErrorCode> DirectorySourceQueue::getNumBlocksAndStatus()
    const {
  std::lock_guard<std::mutex> lock(mutex_);
  ErrorCode status = OK;
  if (!failedSourceStats_.empty() || !failedDirectories_.empty()) {
    // this function is called by active sender threads. The only way files or
    // directories can fail when sender threads are active is due to read errors
    status = BYTE_SOURCE_READ_ERROR;
  }
  return std::make_pair(numBlocks_, status);
}

int64_t DirectorySourceQueue::getTotalSize() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return totalFileSize_;
}

int64_t DirectorySourceQueue::getPreviouslySentBytes() const {
  return previouslySentBytes_;
}

bool DirectorySourceQueue::fileDiscoveryFinished() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return initFinished_;
}

void DirectorySourceQueue::enqueueFilesToBeDeleted() {
  if (!deleteFiles_) {
    return;
  }
  if (!initFinished_ || previouslyTransferredChunks_.empty()) {
    // if the directory transfer has not finished yet or existing files list has
    // not yet been received, return
    return;
  }
  std::set<std::string> discoveredFiles;
  for (const SourceMetaData *metadata : sharedFileData_) {
    discoveredFiles.insert(metadata->relPath);
  }
  int64_t numFilesToBeDeleted = 0;
  for (auto &it : previouslyTransferredChunks_) {
    const std::string &fileName = it.first;
    if (discoveredFiles.find(fileName) != discoveredFiles.end()) {
      continue;
    }
    int64_t seqId = it.second.getSeqId();
    // extra file on the receiver side
    WLOG(INFO) << "Extra file " << fileName << " seq-id " << seqId
               << " on the receiver side, will be deleted";
    SourceMetaData *metadata = new SourceMetaData();
    metadata->relPath = fileName;
    metadata->size = 0;
    // we can reuse the previous seq-id
    metadata->seqId = seqId;
    metadata->allocationStatus = TO_BE_DELETED;
    sharedFileData_.emplace_back(metadata);
    // create a byte source with size and offset equal to 0
    std::unique_ptr<ByteSource> source =
        std::make_unique<FileByteSource>(metadata, 0, 0);
    sourceQueue_.push(std::move(source));
    numFilesToBeDeleted++;
  }
  numEntries_ += numFilesToBeDeleted;
  numBlocks_ += numFilesToBeDeleted;
  smartNotify(numFilesToBeDeleted);
}

std::unique_ptr<ByteSource> DirectorySourceQueue::getNextSource(
    ThreadCtx *callerThreadCtx, ErrorCode &status) {
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
    WVLOG(1) << "got next source " << rootDir_ + source->getIdentifier()
             << " size " << source->getSize();
    // try to open the source
    if (source->open(callerThreadCtx) == OK) {
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
