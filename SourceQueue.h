/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <memory>

#include <wdt/ByteSource.h>

namespace facebook {
namespace wdt {

/**
 * Interface for consuming data from multiple ByteSource's.
 * Call getNextSource() repeatedly to get new sources to consume data
 * from, until finished() returns true.
 *
 * This class is thread-safe, i.e. multiple threads can consume sources
 * in parallel and terminate once finished() returns true. Each source
 * is guaranteed to be consumed exactly once.
 */
class SourceQueue {
 public:
  virtual ~SourceQueue() {
  }

  /// @return true iff no more sources to read from
  virtual bool finished() const = 0;

  /**
   * Get the next source to consume. Ownership transfers to the caller.
   * The method will block until it's able to get the next available source
   * or be sure consumption of all sources has finished.
   *
   * @param threadCtx context of the caller thread
   * @param status    this variable is set to true, if the transfer has already
   *
   * @return          New ByteSource to consume or nullptr if there are
   *                  no more sources to read from (equivalent to finished()).
   */
  virtual std::unique_ptr<ByteSource> getNextSource(ThreadCtx *threadCtx,
                                                    ErrorCode &status) = 0;

  /// @return         total number of files processed/enqueued
  virtual int64_t getCount() const = 0;

  /// @return         total size of files processed/enqueued
  virtual int64_t getTotalSize() const = 0;
};
}
}
