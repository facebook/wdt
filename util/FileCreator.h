/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/Protocol.h>
#include <wdt/WdtConfig.h>
#include <wdt/util/CommonImpl.h>
#include <wdt/util/TransferLogManager.h>

#include <folly/SpinLock.h>
#include <glog/logging.h>
#include <condition_variable>
#include <map>
#include <mutex>
#include <string>
#include <unordered_set>

namespace facebook {
namespace wdt {

/**
 * Utility class for creating/opening files for writing while
 * creating subdirs automatically and only once in case multiple
 * files are created relative to the rootDir directory.
 *
 * Path to rootDir doesn't need to have a trailing slash
 * (it's added for you if missing)
 *
 * This class is thread-safe. (yeah!)
 */
class FileCreator {
 public:
  FileCreator(const std::string &rootDir, int numThreads,
              TransferLogManager &transferLogManager, bool skipWrites)
      : transferLogManager_(transferLogManager), skipWrites_(skipWrites) {
    CHECK(!rootDir.empty());

    // For creating root directory, we are using createDirRecursively.
    // Since, this function adds rootDir to the path provided to it,
    // we are setting the value of rootDir after the function call.
    // So, createDirRecursively uses empty rootDir for this call.
    std::string rootDirPath = rootDir;
    addTrailingSlash(rootDirPath);
    createDirRecursively(rootDirPath, false);
    resetDirCache();
    rootDir_ = rootDirPath;
    threadConditionVariables_ = new std::condition_variable[numThreads];
  }

  virtual ~FileCreator() {
    delete[] threadConditionVariables_;
  }

  /**
   * This is used to open the file in block mode. If the current thread is the
   * first one to try to open the file, then it allocates space using
   * openAndSetSize function. Other threads wait for the first thread to finish
   * and opens the file without setting size.
   *
   * @param threadCtx     context of the calling thread
   * @param blockDetails  block-details
   *
   * @return              file descriptor in case of success, -1 otherwise
   */
  int openForBlocks(ThreadCtx &threadCtx, BlockDetails const *blockDetails);

  /// reset internal directory cache
  void resetDirCache() {
    std::lock_guard<std::mutex> lock(mutex_);
    createdDirs_.clear();
  }

  /// clears allocation status map, called after end of each session
  void clearAllocationMap() {
    folly::SpinLockGuard guard(lock_);
    fileStatusMap_.clear();
  }

 private:
  /**
   * Opens the file and sets its size. If the existing file size is greater than
   * required size, the file is truncated using ftruncate. Space is
   * allocated using posix_fallocate.
   *
   * @param threadCtx     context of the calling thread
   * @param blockDetails  block-details
   *
   * @return          file descriptor in case of success, -1 otherwise
   */
  int openAndSetSize(ThreadCtx &threadCtx, BlockDetails const *blockDetails);

  /**
   * Create a file and open for writing, recursively create subdirs.
   * Subdirs are only created once due to createdDirs_ cache, but
   * if an open fails where we assumed the directory already exists
   * based on cache, we try creating the dir and open again before
   * failing. Will not overwrite existing files unless overwrite option
   * is set.
   *
   * @param threadCtx     context of the calling thread
   * @param relPath       path relative to root dir
   *
   * @return          file descriptor or -1 on error
   */
  int createFile(ThreadCtx &threadCtx, const std::string &relPath);
  /**
   * Open existing file
   */
  int openExistingFile(ThreadCtx &threadCtx, const std::string &relPath);

  /**
   * sets the size of the file. If the size is greater then the
   * file is truncated using ftruncate. Space is allocated using fallocate.
   *
   * @param threadCtx context of the calling thread
   * @param fd        file descriptor
   * @param fileSize  size of the file
   *
   * @return          true for success, false otherwise
   */
  bool setFileSize(ThreadCtx &threadCtx, int fd, int64_t fileSize);

  /**
   * opens the file and sets it size. Called only for the first block to request
   * opening a multi-block file. Sets the allocation status in fileStatusMap_
   * and notifies other waiting thread.
   *
   * @param threadCtx     context of the calling thread
   * @param blockDetails  block-details
   *
   * @return          file descriptor or -1 on error
   */
  int openForFirstBlock(ThreadCtx &threadCtx, BlockDetails const *blockDetails);

  /// waits for allocation of a file to finish
  bool waitForAllocationFinish(int allocatingThreadIndex, int64_t seqId);

  /// appends a trailing / if not already there to path
  static void addTrailingSlash(std::string &path);

  /**
   * Create directory recursively, populating cache. Cache is only
   * used if force is false (but it's still populated in any case).
   *
   * @param dir         dir to create recursively, should end with
   *                    '/' and not start with '/'
   * @param force       whether to force trying to create/skip
   *                    checking the cache
   *
   * @return            true iff successful
   */
  bool createDirRecursively(const std::string dir, bool force = false);

  /// Check whether directory has been created/is in cache
  bool dirCreated(const std::string &dir) {
    std::lock_guard<std::mutex> lock(mutex_);
    return createdDirs_.find(dir) != createdDirs_.end();
  }

  /// returns full path of a file
  std::string getFullPath(const std::string &relPath);

  /// root directory
  std::string rootDir_;

  /// directories created so far, relative to root
  std::unordered_set<std::string> createdDirs_;

  /// protects createdDirs_
  std::mutex mutex_;

  const int ALLOCATED{-1};
  const int FAILED{-2};

  /// map from file sequence id to allocation status. There are four possible
  /// allocation status. NOT STARTED(no entry in the map), ALLOCATED(-1),
  /// FAILED(-2) and IN_PROGRESS(map value is the index of the allocating
  /// thread)
  std::map<int64_t, int> fileStatusMap_;
  /// transfer log manger used by receiver
  TransferLogManager &transferLogManager_;
  /// mutex to coordinate waiting among threads
  std::mutex allocationMutex_;
  /// array of condition_variables for different threads
  std::condition_variable *threadConditionVariables_;
  /// lock protecting fileStatusMap_
  folly::SpinLock lock_;

  // Set to prevent creating files
  bool skipWrites_;
};
}
}
