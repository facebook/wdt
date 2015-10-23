/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/FileByteSource.h>

#include <algorithm>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
namespace facebook {
namespace wdt {

folly::ThreadLocalPtr<FileByteSource::Buffer> FileByteSource::buffer_;

int FileUtil::openForRead(const std::string &filename, bool isDirectReads) {
  int openFlags = O_RDONLY;
  if (isDirectReads) {
#ifdef O_DIRECT
    // no need to change any flags if we are using F_NOCACHE
    openFlags |= O_DIRECT;
#endif
  }
  START_PERF_TIMER
  int fd = ::open(filename.c_str(), openFlags);
  RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
  if (fd >= 0) {
    if (isDirectReads) {
#ifndef O_DIRECT
#ifdef F_NOCACHE
      VLOG(1) << "O_DIRECT not found, using F_NOCACHE instead "
              << "for " << filename;
      int ret = fcntl(fd, F_NOCACHE, 1);
      if (ret) {
        PLOG(ERROR) << "Not able to set F_NOCACHE";
      }
#else
      WDT_CHECK(false)
          << "Direct read enabled, but both O_DIRECT and F_NOCACHE not defined "
          << filename;
#endif
#endif
    }
  } else {
    PLOG(ERROR) << "Error opening file " << filename;
  }
  return fd;
}

FileByteSource::FileByteSource(SourceMetaData *metadata, int64_t size,
                               int64_t offset, int64_t bufferSize)
    : metadata_(metadata),
      size_(size),
      offset_(offset),
      bytesRead_(0),
      bufferSize_(bufferSize),
      alignedReadNeeded_(false) {
  transferStats_.setId(getIdentifier());
}

ErrorCode FileByteSource::open() {
  bytesRead_ = 0;
  this->close();
  ErrorCode errCode = OK;
  bool isDirectReads = metadata_->directReads;
  VLOG(1) << "Reading in direct mode " << isDirectReads;
  if (isDirectReads) {
#ifdef O_DIRECT
    alignedReadNeeded_ = true;
#endif
  }
  bool hasValidBuffer = (buffer_ && bufferSize_ <= buffer_->size_);
  if (!hasValidBuffer || (alignedReadNeeded_ && !buffer_->isMemAligned_)) {
    // TODO: if posix_memalign is present, create aligned buffer by default
    buffer_.reset(new Buffer(bufferSize_, alignedReadNeeded_));
  }

  if (metadata_->fd >= 0) {
    VLOG(1) << "metadata already has fd, no need to open " << getIdentifier();
    fd_ = metadata_->fd;
  } else {
    fd_ = FileUtil::openForRead(metadata_->fullPath, isDirectReads);
    if (fd_ < 0) {
      errCode = BYTE_SOURCE_READ_ERROR;
    }
  }

  transferStats_.setErrorCode(errCode);
  return errCode;
}

void FileByteSource::advanceOffset(int64_t numBytes) {
  offset_ += numBytes;
  size_ -= numBytes;
}

char *FileByteSource::read(int64_t &size) {
  size = 0;
  if (hasError() || finished()) {
    return nullptr;
  }
  int64_t expectedRead =
      (int64_t)std::min<int64_t>(buffer_->size_, size_ - bytesRead_);
  int64_t toRead = expectedRead;
  if (alignedReadNeeded_) {
    toRead =
        ((expectedRead + kDiskBlockSize - 1) / kDiskBlockSize) * kDiskBlockSize;
  }
  // actualRead is guaranteed to be <= buffer_->size_
  WDT_CHECK(toRead <= buffer_->size_) << "Attempting to read " << toRead
                                      << " while buffer size is "
                                      << buffer_->size_;
  START_PERF_TIMER
  int64_t numRead = ::pread(fd_, buffer_->data_, toRead, offset_ + bytesRead_);
  if (numRead < 0) {
    PLOG(ERROR) << "failure while reading file " << metadata_->fullPath;
    this->close();
    transferStats_.setErrorCode(BYTE_SOURCE_READ_ERROR);
    return nullptr;
  }
  if (numRead == 0) {
    this->close();
    return nullptr;
  }
  RECORD_PERF_RESULT(PerfStatReport::FILE_READ)
  // Can only happen in case of O_DIRECT and when
  // we are trying to read the last chunk of file
  // or we are reading in multiples of disk block size
  // from a sub block of the file smaller than disk block
  // size
  if (numRead > expectedRead) {
    WDT_CHECK(alignedReadNeeded_);
    numRead = expectedRead;
  }
  bytesRead_ += numRead;
  size = numRead;
  return buffer_->data_;
}
}
}
