#pragma once

#include <algorithm>
#include <condition_variable>
#include <dirent.h>
#include <glog/logging.h>
#include <mutex>
#include <queue>
#include <string>

#include "SourceQueue.h"

namespace facebook {
namespace wdt {

/**
 * SourceQueue that returns all the regular files under a given directory
 * (recursively) as individual FileByteSource objects, sorted by decreasing
 * file size.
 */
class DirectorySourceQueue : public SourceQueue {
 public:
  /**
   * Create a DirectorySourceQueue. Call init() separately to actually recurse
   * over the root directory and initialize data about files.
   *
   * @param rootDir               root directory to recurse on
   * @param fileSourceBufferSize  buffer size to use when creating individual
   *                              FileByteSource objects (returned by
   *                              getNextSource)
   */
  explicit DirectorySourceQueue(const std::string& rootDir,
                                size_t fileSourceBufferSize)
    : rootDir_(rootDir),
      fileSourceBufferSize_(fileSourceBufferSize),
      initCalled_(false),
      initFinished_(false) {
    CHECK(!rootDir_.empty());
    CHECK(fileSourceBufferSize_ > 0);
    if (rootDir_.back() != '/') {
      rootDir_.push_back('/');
    }
  };

  /**
   * Recurse over given root directory, gather data about regular files and
   * initialize internal data structures. getNextSource() will return sources
   * as this call discovers them.
   *
   * This should only be called once. Subsequent calls will do nothing and
   * return false. In case it is called from multiple threads, one of them
   * will do initialization while the other calls will fail.
   *
   * @return          true iff initialization was successful and hasn't
   *                  been done before
   */
  bool init();

  /// @return true iff all regular files under root dir have been consumed
  virtual bool finished() const;

  /// @return next FileByteSource to consume or nullptr when finished
  virtual std::unique_ptr<ByteSource> getNextSource();

 private:
  /**
   * Recurse on a relative path (to rootDir_) to gather data about files.
   *
   * @param relativePath    relative path to rootDir_
   *
   * @return                whether an error was encountered
   */
  bool recurseOnPath(const std::string& relativePath = "");

  /// root directory to recurse on
  std::string rootDir_;

  /**
   * buffer size to use when creating individual FileByteSource objects
   * (returned by getNextSource).
   */
  const size_t fileSourceBufferSize_;

  /// protects initCalled_/initFinished_/sizeToPath_
  mutable std::mutex mutex_;

  /// condition variable indicating sizeToPath_ is not empty
  mutable std::condition_variable conditionNotEmpty_;

  /// Indicates whether init() has been called to prevent multiple calls
  bool initCalled_;

  /// Indicates whether call to init() has finished
  bool initFinished_;

  /// Orders size/relative path pairs for files under root by decreasing size
  std::priority_queue<std::pair<uint64_t, std::string>> sizeToPath_;
};
}
}
