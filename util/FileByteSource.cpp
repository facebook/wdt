/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/FileByteSource.h>

#include <fcntl.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <algorithm>
namespace facebook {
namespace wdt {

int FileUtil::openForRead(ThreadCtx &threadCtx, const std::string &filename,
                          const bool isDirectReads) {
  int openFlags = O_RDONLY;
  if (isDirectReads) {
#ifdef O_DIRECT
    // no need to change any flags if we are using F_NOCACHE
    openFlags |= O_DIRECT;
#endif
  }
  int fd;
  {
    PerfStatCollector statCollector(threadCtx, PerfStatReport::FILE_OPEN);
    fd = ::open(filename.c_str(), openFlags);
  }
  if (fd >= 0) {
    if (isDirectReads) {
#ifndef O_DIRECT
#ifdef F_NOCACHE
      WVLOG(1) << "O_DIRECT not found, using F_NOCACHE instead "
               << "for " << filename;
      int ret = fcntl(fd, F_NOCACHE, 1);
      if (ret) {
        WPLOG(ERROR) << "Not able to set F_NOCACHE";
      }
#else
      WDT_CHECK(false)
          << "Direct read enabled, but both O_DIRECT and F_NOCACHE not defined "
          << filename;
#endif
#endif
    }
  } else {
    WPLOG(ERROR) << "Error opening file " << filename;
  }
  return fd;
}

FileByteSource::FileByteSource(SourceMetaData *metadata, int64_t size,
                               int64_t offset)
    : metadata_(metadata),
      size_(size),
      offset_(offset),
      bytesRead_(0),
      alignedReadNeeded_(false) {
  transferStats_.setId(getIdentifier());
}

ErrorCode FileByteSource::open(ThreadCtx *threadCtx) {
  if (metadata_->allocationStatus == TO_BE_DELETED) {
    return OK;
  }
  bytesRead_ = 0;
  this->close();
  threadCtx_ = threadCtx;
  ErrorCode errCode = OK;
  const bool isDirectReads = metadata_->directReads;
  WVLOG(1) << "Reading in direct mode " << isDirectReads;
  if (isDirectReads) {
#ifdef O_DIRECT
    alignedReadNeeded_ = true;
#endif
  }

  if (metadata_->fd >= 0) {
    WVLOG(1) << "metadata already has fd, no need to open " << getIdentifier();
    fd_ = metadata_->fd;
  } else {
    fd_ =
        FileUtil::openForRead(*threadCtx_, metadata_->fullPath, isDirectReads);
    if (fd_ < 0) {
      errCode = BYTE_SOURCE_READ_ERROR;
    }
  }

  transferStats_.setLocalErrorCode(errCode);
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
  const Buffer *buffer = threadCtx_->getBuffer();
  int64_t offsetRemainder = 0;
  if (alignedReadNeeded_) {
    offsetRemainder = (offset_ + bytesRead_) % kDiskBlockSize;
  }
  int64_t logicalRead = (int64_t)std::min<int64_t>(
      buffer->getSize() - offsetRemainder, size_ - bytesRead_);
  int64_t physicalRead = logicalRead;
  if (alignedReadNeeded_) {
    physicalRead = ((logicalRead + offsetRemainder + kDiskBlockSize - 1) /
                    kDiskBlockSize) *
                   kDiskBlockSize;
  }
  const int64_t seekPos = (offset_ + bytesRead_) - offsetRemainder;
  int numRead;
  {
    PerfStatCollector statCollector(*threadCtx_, PerfStatReport::FILE_READ);
    numRead = ::pread(fd_, buffer->getData(), physicalRead, seekPos);
  }
  if (numRead < 0) {
    WPLOG(ERROR) << "Failure while reading file " << metadata_->fullPath
                 << " need align " << alignedReadNeeded_ << " physicalRead "
                 << physicalRead << " offset " << offset_ << " seepPos "
                 << seekPos << " offsetRemainder " << offsetRemainder
                 << " bytesRead " << bytesRead_;
    this->close();
    transferStats_.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
    return nullptr;
  }
  if (numRead == 0) {
    WLOG(ERROR) << "Unexpected EOF on " << metadata_->fullPath << " need align "
                << alignedReadNeeded_ << " physicalRead " << physicalRead
                << " offset " << offset_ << " seepPos " << seekPos
                << " offsetRemainder " << offsetRemainder << " bytesRead "
                << bytesRead_;
    this->close();
    return nullptr;
  }
  // Can only happen in case of O_DIRECT and when
  // we are trying to read the last chunk of file
  // or we are reading in multiples of disk block size
  // from a sub block of the file smaller than disk block
  // size
  size = numRead - offsetRemainder;
  if (size > logicalRead) {
    WDT_CHECK(alignedReadNeeded_);
    size = logicalRead;
  }
  bytesRead_ += size;
  WVLOG(1) << "Size " << size << " need align " << alignedReadNeeded_
           << " physicalRead " << physicalRead << " offset " << offset_
           << " seepPos " << seekPos << " offsetRemainder " << offsetRemainder
           << " bytesRead " << bytesRead_;
  return buffer->getData() + offsetRemainder;
}

void FileByteSource::clearPageCache() {
#ifdef HAS_POSIX_FADVISE
  if (metadata_->directReads) {
    // no need to clear page cache for direct reads
    return;
  }
  if (threadCtx_ == nullptr) {
    return;
  }
  auto &options = threadCtx_->getOptions();
  if (bytesRead_ > 0 && !options.skip_fadvise) {
    PerfStatCollector statCollector(*threadCtx_, PerfStatReport::FADVISE);
    if (posix_fadvise(fd_, offset_, bytesRead_, POSIX_FADV_DONTNEED) != 0) {
      WPLOG(ERROR) << "posix_fadvise failed for " << getIdentifier() << " "
                   << offset_ << " " << bytesRead_;
    }
  }
#endif
}

void FileByteSource::close() {
  clearPageCache();
  if (metadata_->fd >= 0) {
    // if the fd is not opened by this source, no need to close it
    WVLOG(1) << "No need to close " << getIdentifier()
             << ", this was not opened by FileByteSource";
  } else if (fd_ >= 0) {
    PerfStatCollector statCollector(*threadCtx_, PerfStatReport::FILE_CLOSE);
    ::close(fd_);
  }
  fd_ = -1;
  threadCtx_ = nullptr;
}
}
}
