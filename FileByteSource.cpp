/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "FileByteSource.h"
#include "WdtOptions.h"

#include <algorithm>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>
namespace facebook {
namespace wdt {

folly::ThreadLocalPtr<FileByteSource::Buffer> FileByteSource::buffer_;

FileByteSource::FileByteSource(SourceMetaData *metadata, int64_t size,
                               int64_t offset, int64_t bufferSize)
    : metadata_(metadata),
      size_(size),
      offset_(offset),
      bytesRead_(0),
      bufferSize_(bufferSize) {
  transferStats_.setId(getIdentifier());
}

ErrorCode FileByteSource::open() {
  bytesRead_ = 0;
  this->close();
  bool isOdirect = (metadata_->oFlags & O_DIRECT);
  int oFlags = O_RDONLY;
  if (isOdirect) {
    oFlags |= O_DIRECT;
  }
#ifdef WDT_SIMULATED_ODIRECT
  if (isOdirect) {
    oFlags &= ~(O_DIRECT);
  }
#endif
  VLOG(1) << "Reading with O_DIRECT " << ((oFlags & O_DIRECT) > 0);
  ErrorCode errCode = OK;
  bool hasValidBuffer = (buffer_ && bufferSize_ <= buffer_->size_);
  if (!hasValidBuffer || (isOdirect && !buffer_->isMemAligned_)) {
    buffer_.reset(new Buffer(bufferSize_, isOdirect));
  }
  const std::string &fullPath = metadata_->fullPath;
  START_PERF_TIMER
  fd_ = ::open(fullPath.c_str(), oFlags);
  if (fd_ < 0) {
    errCode = BYTE_SOURCE_READ_ERROR;
    PLOG(ERROR) << "Error opening file " << fullPath;
  } else {
#ifdef WDT_SIMULATED_ODIRECT
#ifdef F_NOCACHE
    if (isOdirect) {
      LOG(WARNING) << "O_DIRECT not found, using F_NOCACHE instead "
                   << "for " << getIdentifier();
      int ret = fcntl(fd_, F_NOCACHE, 1);
      if (ret) {
        PLOG(ERROR) << "Not able to do F_NOCACHE";
      }
    }
#else
    if (isOdirect) {
      LOG(ERROR) << "O_DIRECT requested but this OS doesn't support "
                 << "O_DIRECT or F_NOCACHE";
    }
#endif
#endif
    RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
    if (offset_ > 0) {
      START_PERF_TIMER
      if (lseek(fd_, offset_, SEEK_SET) < 0) {
        errCode = BYTE_SOURCE_READ_ERROR;
        PLOG(ERROR) << "Error seeking file " << fullPath;
      } else {
        RECORD_PERF_RESULT(PerfStatReport::FILE_SEEK)
      }
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
  bool isOdirect = (metadata_->oFlags & O_DIRECT);
  if (isOdirect) {
    toRead =
        ((expectedRead + kDiskBlockSize - 1) / kDiskBlockSize) * kDiskBlockSize;
  }
  // actualRead is guaranteed to be <= buffer_->size_
  WDT_CHECK(toRead <= buffer_->size_) << "Attempting to read " << toRead
                                      << " while buffer size is "
                                      << buffer_->size_;
  START_PERF_TIMER
  int64_t numRead = ::read(fd_, buffer_->data_, toRead);
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
    WDT_CHECK(isOdirect);
    numRead = expectedRead;
  }
  bytesRead_ += numRead;
  size = numRead;
  return buffer_->data_;
}
}
}
