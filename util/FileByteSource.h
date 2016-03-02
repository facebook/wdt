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
#include <wdt/util/CommonImpl.h>

namespace facebook {
namespace wdt {

/// File related code
class FileUtil {
 public:
  /**
   * Opens the file for reading.
   *
   * @param threadCtx       thread context
   * @param filename        name of the file
   * @param isDirectReads   whether to open for direct reads
   *
   * @return    If successful, fd is returned, else -1 is returned
   */
  static int openForRead(ThreadCtx &threadCtx, const std::string &filename,
                         bool isDirectReads);
  // TODO: create a separate file for this class and move other file related
  // code here
};

/**
 * ByteSource that reads data from a file.
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
   */
  FileByteSource(SourceMetaData *metadata, int64_t size, int64_t offset);

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
    return (metadata_->allocationStatus != TO_BE_DELETED) && (fd_ < 0);
  }

  /// @see ByteSource.h
  virtual char *read(int64_t &size) override;

  /// @see ByteSource.h
  virtual void advanceOffset(int64_t numBytes) override;

  /// @see ByteSource.h
  virtual ErrorCode open(ThreadCtx *threadCtx) override;

  /// close the source for reading
  virtual void close() override;

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
  /// clears page cache
  void clearPageCache();

  ThreadCtx *threadCtx_{nullptr};

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

  /// Whether reads have to be done using aligned buffer and size
  bool alignedReadNeeded_{false};

  /// transfer stats
  TransferStats transferStats_;
};
}
}
