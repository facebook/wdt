#pragma once

#include <memory>

#include "ByteSource.h"

namespace facebook { namespace wdt {

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
  virtual ~SourceQueue() {}

  /// @return true iff no more sources to read from
  virtual bool finished() const = 0;

  /**
   * Get the next source to consume. Ownership transfers to the caller.
   * The method will block until it's able to get the next available source
   * or be sure consumption of all sources has finished.
   *
   * @return          New ByteSource to consume or nullptr if there are
   *                  no more sources to read from (equivalent to finished()).
   */
  virtual std::unique_ptr<ByteSource> getNextSource() = 0;
};

}}
