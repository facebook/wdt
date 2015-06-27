#pragma once

#include <unistd.h>

#include "ByteSource.h"
#include "Reporting.h"
#include <folly/ThreadLocal.h>

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
   * @param metadata          shared file data
   * @param size              size of file; if actual size is larger we'll
   *                          truncate, if it's smaller we'll fail
   * @param offset            block offset
   * @param bufferSize        size of buffer for temporarily storing read
   *                          bytes
   */
  FileByteSource(SourceMetaData *metadata, int64_t size, int64_t offset,
                 int64_t bufferSize);

  /// close file descriptor if still open
  virtual ~FileByteSource() {
    this->close();
  }

  /// @return filepath
  virtual const std::string &getIdentifier() const override {
    return metadata_->relPath;
  }

  /// @return size of file in bytes
  virtual int64_t getSize() const override {
    return size_;
  }

  /// @return offset from which to start reading
  virtual int64_t getOffset() const override {
    return offset_;
  }

  /// @see ByteSource.h
  virtual const SourceMetaData &getMetaData() const override {
    return *metadata_;
  }

  /// @return true iff finished reading file successfully
  virtual bool finished() const override {
    return bytesRead_ == size_ && !hasError();
  }

  /// @return true iff there was an error reading file
  virtual bool hasError() const override {
    return fd_ < 0;
  }

  /// @see ByteSource.h
  virtual char *read(int64_t &size) override;

  /// open the source for reading
  virtual ErrorCode open() override;

  /// close the source for reading
  virtual void close() override {
    if (fd_ >= 0) {
      START_PERF_TIMER
      ::close(fd_);
      RECORD_PERF_RESULT(PerfStatReport::FILE_CLOSE)
      fd_ = -1;
    }
  }

  /**
   * @return transfer stats for the source. If the stats is moved by the
   *         caller, then this function can not be called again
   */
  virtual TransferStats &getTransferStats() override {
    return transferStats_;
  }

  /// @param stats    Stats to be added
  virtual void addTransferStats(const TransferStats &stats) override {
    transferStats_ += stats;
  }

 private:
  struct Buffer {
    explicit Buffer(int64_t size) : size_(size) {
      data_ = new char[size + 1];
    }

    ~Buffer() {
      delete[] data_;
    }

    char *data_;
    int64_t size_;
  };

  /**
   * Buffer for temporarily holding bytes read from file. This is thread-local
   * for efficiency reasons, so only one FileByteSource can be used at once
   * per thread.
   */
  static folly::ThreadLocalPtr<Buffer> buffer_;

  /// shared file information
  SourceMetaData *metadata_;

  /// filesize
  const int64_t size_;

  /// open file descriptor for file (set to < 0 on error)
  int fd_{-1};

  /// block offset
  const int64_t offset_;

  /// number of bytes read so far from file
  int64_t bytesRead_;

  /// buffer size
  int64_t bufferSize_;

  /// transfer stats
  TransferStats transferStats_;
};
}
}
