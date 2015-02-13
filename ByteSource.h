#pragma once

#include "Reporting.h"

#include <string>

namespace facebook {
namespace wdt {

class ByteSource {
 public:
  virtual ~ByteSource() {
  }

  /// @return identifier for the source
  virtual const std::string &getIdentifier() const = 0;

  /// @return number of bytes in this source
  virtual uint64_t getSize() const = 0;

  /// @return number of bytes in the original source
  virtual uint64_t getTotalSize() const = 0;

  /// @return offset from which to start reading
  virtual uint64_t getOffset() const = 0;

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
  virtual char *read(size_t &size) = 0;

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
