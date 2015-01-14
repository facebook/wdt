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
#include "WdtOptions.h"

DECLARE_int32(buffer_size);

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
   */
  explicit DirectorySourceQueue(const std::string &rootDir);

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

  /// @return true if all the files have been discovered, false otherwise
  bool fileDiscoveryFinished() const;

  /// @return next FileByteSource to consume or nullptr when finished
  virtual std::unique_ptr<ByteSource> getNextSource() override;

  /**
   * @return          total number and total size in bytes of sources
   *                  enqueued/processed through the queue
   */
  virtual std::pair<size_t, size_t> getCountAndSize() const override;

  /**
   * Sets regex represnting files to include for transfer
   *
   * @param includePattern          file inclusion regex
   */
  void setIncludePattern(const std::string &includePattern);

  /**
   * Sets regex represnting files to exclude for transfer
   *
   * @param excludePattern          file exclusion regex
   */
  void setExcludePattern(const std::string &excludePattern);

  /**
   * Sets regex represnting directories to exclude for transfer
   *
   * @param pruneDirPattern         directory exclusion regex
   */
  void setPruneDirPattern(const std::string &pruneDirPattern);

  /**
   * Sets buffer size to use during creating individual FileByteSource object
   *
   * @param fileSourceBufferSize  buffers size
   */
  void setFileSourceBufferSize(const size_t fileSourceBufferSize);

  /**
   * Sets specific files to be transferred
   *
   * @param fileInfo              files to transferred
   */
  void setFileInfo(const std::vector<FileInfo> &fileInfo);

  /**
   * Sets whether to follow symlink or not
   *
   * @param followSymlinks        whether to follow symlink or not
   */
  void setFollowSymlinks(const bool followSymlinks);

  /**
   * returns a source to the queue, checks for fail/retries, doesn't increment
   * numentries
   *
   * @param source               source to be returned to the queue
   */
  virtual void returnToQueue(std::unique_ptr<ByteSource> &source);

  /**
   * Returns list of files which were not transfereed. It empties the queue and
   * adds queue entries to the failed file list. This function should be called
   * after all the sending threads have finished execurtion
   *
   * @return                      stats for failed sources
   */
  std::vector<TransferStats> &getFailedSourceStats();

  virtual ~DirectorySourceQueue() {
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

  /**
   * initial creation from either explore or enqueue files - always increment
   * numentries inside the lock, doesn't check for fail retries
   *
   * @param relativePath         relative path of the file to be added
   * @param fileSize             size of the file
   */
  virtual void createIntoQueue(const std::string &relativePath,
                               const size_t fileSize);

  /// root directory to recurse on if fileInfo_ is empty
  std::string rootDir_;

  /// regex represnting directories to prune
  std::string pruneDirPattern_;

  /// regex representing files to include
  std::string includePattern_;

  /// regex representing files to exclude
  std::string excludePattern_;

  /**
   * buffer size to use when creating individual FileByteSource objects
   * (returned by getNextSource).
   */
  size_t fileSourceBufferSize_;

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

  struct SourceComparator {
    bool operator()(const std::unique_ptr<ByteSource> &source1,
                    const std::unique_ptr<ByteSource> &source2) {
      auto retryCount1 = source1->getTransferStats().getFailedAttempts();
      auto retryCount2 = source2->getTransferStats().getFailedAttempts();
      if (retryCount1 != retryCount2) {
        return retryCount1 > retryCount2;
      }
      if (source1->getSize() != source2->getSize()) {
        return source1->getSize() < source2->getSize();
      }
      return source1->getIdentifier() < source2->getIdentifier();
    }
  };

  /**
   * priority queue of sources. Sources are first ordered by increasing
   * failedAttempts, then by decreasing size.
   */
  std::priority_queue<std::unique_ptr<ByteSource>,
                      std::vector<std::unique_ptr<ByteSource>>,
                      SourceComparator> sourceQueue_;

  /// Transfer stats for sources which are not transferred
  std::vector<TransferStats> failedSourceStats_;

  /// Total number of entries/files that have passed through the queue
  size_t numEntries_{0};

  /// Total size of entries/files that have passed through the queue
  size_t totalFileSize_{0};

  /// Whether to follow symlinks or not
  bool followSymlinks_{false};

  const WdtOptions &options_;
};
}
}
