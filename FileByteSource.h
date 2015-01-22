#pragma once

#include <unistd.h>

#include "ByteSource.h"
#include "folly/ThreadLocal.h"

namespace facebook {
namespace wdt {

/**
 * ByteSource that reads data from a file. The buffer used is thread-local
 * for efficiency reasons so only one FileByteSource can be created/used
 * per thread. It's also unsafe to access the same FileByteSource from
 * multiple threads.
 */
class FileByteSource : public ByteSource {
 public:
  /**
   * Create a new FileByteSource for a given path.
   *
   * @param rootPath          root dir of file
   * @param relPath           relative filepath to root
   * @param size              size of file; if actual size is larger we'll
   *                          truncate, if it's smaller we'll fail
   * @param bufferSize        size of buffer for temporarily storing read
   *bytes
   */
  FileByteSource(const std::string &rootPath, const std::string &relPath,
                 uint64_t size, size_t bufferSize);

  /// close file descriptor if still open
  virtual ~FileByteSource() {
    this->close();
  }

  /// @return filepath
  virtual const std::string &getIdentifier() const {
    return relPath_;
  }

  /// @return size of file in bytes
  virtual uint64_t getSize() const {
    return size_;
  }

  /// @return true iff finished reading file successfully
  virtual bool finished() const {
    return bytesRead_ == size_ && !hasError();
  }

  /// @return true iff there was an error reading file
  virtual bool hasError() const {
    return fd_ < 0;
  }

  /// @see ByteSource.h
  virtual char *read(size_t &size);

  /// open the source for reading
  virtual void open();

  /// close the source for reading
  virtual void close() override {
    if (fd_ >= 0) {
      ::close(fd_);
      fd_ = -1;
    }
  }

  /**
   * @return transfer stats for the source. If the stats is moved by the
   *         caller, then this function can not be called again
   */
  virtual TransferStats &getTransferStats() {
    return transferStats_;
  }

  /// @param stats    Stats to be added
  virtual void addTransferStats(const TransferStats &stats) {
    transferStats_ += stats;
  }

 private:
  struct Buffer {
    explicit Buffer(size_t size) : size_(size) {
      data_ = new char[size + 1];
    }

    ~Buffer() {
      delete[] data_;
    }

    char *data_;
    size_t size_;
  };

  /**
   * Buffer for temporarily holding bytes read from file. This is thread-local
   * for efficiency reasons, so only one FileByteSource can be used at once
   * per thread.
   */
  static folly::ThreadLocalPtr<Buffer> buffer_;

  /// root path
  const std::string rootPath_;

  /// relative filepath
  const std::string relPath_;

  /// filesize
  const uint64_t size_;

  /// open file descriptor for file (set to < 0 on error)
  int fd_{-1};

  /// number of bytes read so far from file
  uint64_t bytesRead_;

  /// buffer size
  size_t bufferSize_;

  /// transfer stats
  TransferStats transferStats_;
};
}
}
