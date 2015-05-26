#pragma once

#include "ErrorCodes.h"

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
  virtual uint64_t getTotalWritten() = 0;

  /// close the writer
  virtual void close() = 0;
};
}
}
