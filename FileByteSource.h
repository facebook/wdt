#pragma once

#include <unistd.h>

#include "ByteSource.h"
#include "folly/ThreadLocal.h"

namespace facebook {
namespace wdt {

/// class representing file level data shared between blocks
class FileMetaData {
 public:
  /**
   * @param fullPath    full path of the file
   * @param relPath     relative path w.r.t root directory. keeping full path,
   *                    rather than root path, because we only need full path,
   *                    and creating full path from root and rel path involves
   *                    string concatenation
   * @param fileSize    size of the file
   */
  FileMetaData(const std::string &fullPath, const std::string &relPath,
               size_t fileSize)
      : fullPath_(fullPath), relPath_(relPath), fileSize_(fileSize) {
  }

  /// @return           full path of the file
  const std::string &getFullPath() {
    return fullPath_;
  }

  /// @return           relative path of the file
  const std::string &getRelPath() {
    return relPath_;
  }

  /// @return           size of the file
  const size_t getFileSize() {
    return fileSize_;
  }

 private:
  /// full filepath
  const std::string fullPath_;

  /// relative pathname
  const std::string relPath_;

  /// size of the entire file
  const size_t fileSize_;
};

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
   * @param fileData          shared file data
   * @param size              size of file; if actual size is larger we'll
   *                          truncate, if it's smaller we'll fail
   * @param offset            block offset
   * @param bufferSize        size of buffer for temporarily storing read
   *                          bytes
   */
  FileByteSource(FileMetaData *fileData, uint64_t size, uint64_t offset,
                 size_t bufferSize);

  /// close file descriptor if still open
  virtual ~FileByteSource() {
    this->close();
  }

  /// @return filepath
  virtual const std::string &getIdentifier() const {
    return fileData_->getRelPath();
  }

  /// @return size of file in bytes
  virtual uint64_t getSize() const {
    return size_;
  }

  /// @return number of bytes in the original source
  virtual uint64_t getTotalSize() const {
    return fileData_->getFileSize();
  }

  /// @return offset from which to start reading
  virtual uint64_t getOffset() const {
    return offset_;
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
  virtual ErrorCode open();

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

  /// shared file information
  FileMetaData *fileData_;

  /// filesize
  const uint64_t size_;

  /// open file descriptor for file (set to < 0 on error)
  int fd_{-1};

  /// block offset
  const uint64_t offset_;

  /// number of bytes read so far from file
  uint64_t bytesRead_;

  /// buffer size
  size_t bufferSize_;

  /// transfer stats
  TransferStats transferStats_;
};
}
}
