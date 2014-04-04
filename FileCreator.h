#pragma once

#include <glog/logging.h>
#include <mutex>
#include <string>
#include <unordered_set>

namespace facebook { namespace wdt {

/**
 * Utitliy class for creating/opening files for writing while
 * creating subdirs automatically and only once in case multiple
 * files are created in the same directory.
 *
 * This class is thread-safe.
 */
class FileCreator {
public:
  /// rootDir is assumed to exist
  explicit FileCreator(const char* rootDir) : rootDir_(rootDir) {
    CHECK(!rootDir_.empty());
    if (rootDir_.back() != '/') {
      rootDir_.push_back('/');
    }
  }

  /**
   * Create a file and open for writing, recursively create subdirs.
   * Subdirs are only created once due to createdDirs_ cache, but
   * if an open fails where we assumed the directory already exists
   * based on cache, we try creating the dir and open again before
   * failing.
   *
   * @relPath       path relative to root dir
   *
   * @return        file descriptor or -1 on error
   */
  int createFile(const char* relPath);

  /// reset internal directory cache
  void reset() {
    std::lock_guard<std::mutex> lock(mutex_);
    createdDirs_.clear();
  }

private:
  /**
   * Create directory recursively, populating cache. Cache is only
   * used if force is false (but it's still populated in any case).
   *
   * @param dir         dir to create recursively, should end with
   *                    '/' and not start with '/'
   * @parm force        whether to force trying to create/skip
   *                    checking the cache
   *
   * @return            true iff successful
   */
  bool createDirRecursively(const std::string& dir, bool force = false);

  /// Check whether directory has been created/is in cache
  bool dirCreated(const std::string& dir) {
    std::lock_guard<std::mutex> lock(mutex_);
    return createdDirs_.find(dir) != createdDirs_.end();
  }

  /// root directory
  std::string rootDir_;

  /// directories created so far, relative to root
  std::unordered_set<std::string> createdDirs_;

  /// protects createdDirs_
  std::mutex mutex_;
};

}}
