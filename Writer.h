/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ErrorCodes.h>

namespace facebook {
namespace wdt {
/**
 * Interface to write received data
 */
class Writer {
 public:
  virtual ~Writer() {
  }

  /**
   * open the source for reading
   *
   * @return status of the open
   */
  virtual ErrorCode open() = 0;

  /**
   * writes size number bytes from buffer
   *
   * @param buf   buffer to write from
   * @param size  number of bytes to write
   *
   * @return      status of the write
   */
  virtual ErrorCode write(char *buf, int64_t size) = 0;

  /// @return   total number of bytes written
  virtual int64_t getTotalWritten() = 0;

  /// sync data to disk
  virtual ErrorCode sync() = 0;

  /// close the writer
  virtual ErrorCode close() = 0;
};
}
}
