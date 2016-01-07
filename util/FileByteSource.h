/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <unistd.h>

#include <wdt/ByteSource.h>
#include <wdt/Reporting.h>
#include <folly/ThreadLocal.h>

namespace facebook {
namespace wdt {
const int64_t kDiskBlockSize = 4 * 1024;

/// File related code
class FileUtil {
 public:
  /**
   * Opens the file for reading.
   *
   * @param filename        name of the file
   * @param isDirectReads   whether to open for direct reads
   *
   * @return    If successful, fd is returned, else -1 is returned
   */
  static int openForRead(const std::string &filename, bool isDirectReads);
  // TODO: create a separate file for this class and move other file related
  // code here
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

  /// @see ByteSource.h
  virtual void advanceOffset(int64_t numBytes) override;

  /// open the source for reading
  virtual ErrorCode open() override;

  /// close the source for reading
  virtual void close() override {
    if (metadata_->fd >= 0) {
      // if the fd is not opened by this source, no need to close it
      VLOG(1) << "No need to close " << getIdentifier()
              << ", this was not opened by FileByteSource";
      fd_ = -1;
      return;
    }
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

  int64_t getBufferSize() const override {
    if (!buffer_) {
      return 0;
    }
    return buffer_->size_;
  }

 private:
  struct Buffer {
    explicit Buffer(int64_t size, bool isMemAligned) : size_(size) {
      isMemAligned_ = false;
      if (!isMemAligned) {
        data_ = new char[size + 1];
        return;
      }
#ifdef WDT_SUPPORTS_ODIRECT
      const int64_t remainder = size_ % kDiskBlockSize;
      if (remainder != 0) {
        // Making size the next multiple of disk block size
        size_ = (size_ - remainder) + kDiskBlockSize;
        LOG(INFO) << "Changing the buffer size to multiple "
                  << "of " << kDiskBlockSize << ". New size " << size_
                  << " old size " << size;
      }
      VLOG(1) << "Posix memaligned buffer, size = " << size_;
      int ret = posix_memalign((void **)&data_, kDiskBlockSize, size_);
      if (ret) {
        LOG(ERROR) << "Memalign memory failed " << strerrorStr(ret);
      }
      isMemAligned_ = true;
#endif
    }

    ~Buffer() {
      if (isMemAligned_) {
        free(data_);
        return;
      }
      delete[] data_;
    }
    bool isMemAligned_;
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
  int64_t size_;

  /// open file descriptor for file (set to < 0 on error)
  int fd_{-1};

  /// block offset
  int64_t offset_;

  /// number of bytes read so far from file
  int64_t bytesRead_;

  /// buffer size
  int64_t bufferSize_;

  /// Whether reads have to be done using aligned buffer and size
  bool alignedReadNeeded_{false};

  /// transfer stats
  TransferStats transferStats_;
};
}
}
