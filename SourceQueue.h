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
  /// @return true iff no more sources to read from
  bool finished() const;

  /**
   * Get the next source to consume. Ownership transfers to the caller.
   *
   * @return          New ByteSource to consume or nullptr if there is
   *                  no source available (for now equivalent to finished()
   *                  but later sources could be discovered over time).
   */
  std::unique_ptr<ByteSource> getNextSource();
};

}}
