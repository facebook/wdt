/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "Reporting.h"
#include "Protocol.h"

#include <string>

namespace facebook {
namespace wdt {

/// struct representing file level data shared between blocks
struct SourceMetaData {
  /// full filepath
  std::string fullPath;
  /// relative pathname
  std::string relPath;
  /**
   * sequence number associated with the file. Sequence number
   * represents the order in which files were first added to the queue.
   * This is a file level identifier. It is same for blocks belonging
   * to the same file. This is efficient while using in sets. Instead
   * of using full path of the file, we can use this to identify the
   * file.
   */
  int64_t seqId;
  /// size of the entire source
  int64_t size;
  /// file allocation status in the receiver side
  FileAllocationStatus allocationStatus;
  /// if there is a size mismatch, this is the previous sequence id
  int64_t prevSeqId;
  /// Read the file with these flags
  int oFlags;
};

class ByteSource {
 public:
  virtual ~ByteSource() {
  }

  /// @return identifier for the source
  virtual const std::string &getIdentifier() const = 0;

  /// @return number of bytes in this source
  virtual int64_t getSize() const = 0;

  /// @return size of buffer
  virtual int64_t getBufferSize() const = 0;

  /// @return offset from which to start reading
  virtual int64_t getOffset() const = 0;

  /// @return metadata for the source
  virtual const SourceMetaData &getMetaData() const = 0;

  /// @return true iff all data read successfully
  virtual bool finished() const = 0;

  /// @return true iff there was an error reading
  virtual bool hasError() const = 0;

  /**
   * Read chunk of data from the source and return a pointer to data and its
   * size. Memory is owned by the source. Subsequent calls to read() might
   * delete the previously read data so make sure to consume all data between
   * calls to read().
   *
   * @param size      will be set to number of bytes read (the source will
   *                  decide how much data to read at once)
   *
   * @return          pointer to the data read; in case of failure or EOF,
   *                  nullptr will be returned and size will be set to 0;
   *                  use finished() and hasError() members to distinguish
   *                  the two cases
   */
  virtual char *read(int64_t &size) = 0;

  /// Advances ByteSource offset by numBytes
  virtual void advanceOffset(int64_t numBytes) = 0;

  /// open the source for reading
  virtual ErrorCode open() = 0;

  /// close the source for reading
  virtual void close() = 0;

  /**
   * @return transfer stats for the source. If the stats is moved by the
   *         caller, then this function can not be called again
   */
  virtual TransferStats &getTransferStats() = 0;

  /// @param stats    Stats to be added
  virtual void addTransferStats(const TransferStats &stats) = 0;
};
}
}
