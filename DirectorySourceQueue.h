#pragma once

#include <algorithm>
#include <condition_variable>
#include <dirent.h>
#include <glog/logging.h>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <utility>

#include "SourceQueue.h"

namespace facebook {
namespace wdt {

/// filename-filesize pair. Negative filesize denotes the entire file.
typedef std::pair<std::string, int64_t> FileInfo;

/**
 * SourceQueue that returns all the regular files under a given directory
 * (recursively) as individual FileByteSource objects, sorted by decreasing
 * file size.
 *
 * TODO: The actual building of the queue is specific to this implementation
 * which may or may not make it easy to plug a different implementation
 * (as shown by the current implementation of Sender.cpp)
 */
class DirectorySourceQueue : public SourceQueue {
 public:
  /**
   * Create a DirectorySourceQueue.
   * Call buildQueueSynchronously() or buildQueueAsynchronously() separately
   * to actually recurse over the root directory gather files and sizes.
   *
   * @param rootDir               root directory to recurse on
   * @param fileSourceBufferSize  buffer size to use when creating individual
   *                              FileByteSource objects (returned by
   *                              getNextSource)
   * @param fileInfo              (optional) if non-empty, only operate on the
   *                              specified paths relative to rootDir
   */
  DirectorySourceQueue(const std::string &rootDir, size_t fileSourceBufferSize,
                       const std::vector<FileInfo> &fileInfo = {});

  /**
   * Recurse over given root directory, gather data about regular files and
   * initialize internal data structures. getNextSource() will return sources
   * as this call discovers them.
   *
   * This should only be called once. Subsequent calls will do nothing and
   * return false. In case it is called from multiple threads, one of them
   * will do initialization while the other calls will fail.
   *
   * This is synchronous in the succeeding thread - it will block until
   * the directory is completely discovered. Use buildQueueAsynchronously()
   * for async fetch from parallel thread.
   *
   * @return          true iff initialization was successful and hasn't
   *                  been done before
   */
  bool buildQueueSynchronously();

  /**
   * Starts a new thread to build the queue @see buildQueueSynchronously()
   * @return the created thread (to be joined if needed)
   */
  std::thread buildQueueAsynchronously();

  /// @return true iff all regular files under root dir have been consumed
  bool finished() const override;

  /// @return next FileByteSource to consume or nullptr when finished
  std::unique_ptr<ByteSource> getNextSource() override;

  size_t count() const override {
    return numEntries_;
  }

 private:
  /**
   * Traverse rootDir_ to gather files and sizes to enqueue
   *
   * @return                true on success, false on error
   */
  bool explore();

  /**
   * Stat the input files and populate sizeToPath_ (alternative to
   * explore used when fileInfo was specified)
   *
   * @return                true on success, false on error
   */
  bool enqueueFiles();

  /// root directory to recurse on if fileInfo_ is empty
  std::string rootDir_{""};

  /**
   * buffer size to use when creating individual FileByteSource objects
   * (returned by getNextSource).
   */
  const size_t fileSourceBufferSize_;

  /// List of files to enqueue instead of recursing over rootDir_.
  std::vector<FileInfo> fileInfo_;

  /// protects initCalled_/initFinished_/sizeToPath_
  mutable std::mutex mutex_;

  /// condition variable indicating sizeToPath_ is not empty
  mutable std::condition_variable conditionNotEmpty_;

  /// Indicates whether init() has been called to prevent multiple calls
  bool initCalled_{false};

  /// Indicates whether call to init() has finished
  bool initFinished_{false};

  /// Orders size/relative path pairs for files under root by decreasing size
  std::priority_queue<std::pair<uint64_t, std::string>> sizeToPath_;

  /// Total number of entries/files that have passed through the queue
  size_t numEntries_{0};
};
}
}
