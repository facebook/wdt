/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/FileWriter.h>
#include <wdt/util/CommonImpl.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/types.h>

namespace facebook {
namespace wdt {

FileWriter::~FileWriter() {
  // Make sure that the file is closed but this should be a no-op as the
  // caller should always call sync() and close() manually to check the error
  // code.
  if (!isClosed()) {
    WLOG(ERROR ) << "File " << blockDetails_->fileName
                 << " was not closed and needed to be closed in the dtor";
    close();
  }
}

ErrorCode FileWriter::open() {
  if (threadCtx_.getOptions().skip_writes) {
    return OK;
  }
  // TODO: consider a working optimization for small files
  fd_ = fileCreator_->openForBlocks(threadCtx_, blockDetails_);
  if (blockDetails_->allocationStatus == TO_BE_DELETED) {
    WDT_CHECK_EQ(-1, fd_);
    return OK;
  }
  if (fd_ >= 0 && blockDetails_->offset > 0) {
    int64_t ret;
    {
      PerfStatCollector statCollector(threadCtx_, PerfStatReport::FILE_SEEK);
      ret = lseek(fd_, blockDetails_->offset, SEEK_SET);
    }
    if (ret < 0) {
      WPLOG(ERROR) << "Unable to seek to " << blockDetails_->offset << " for "
                   << blockDetails_->fileName;
      close();
      return FILE_WRITE_ERROR;
    }
  }
  if (fd_ == -1) {
    WLOG(ERROR) << "File open/seek failed for " << blockDetails_->fileName;
    return FILE_WRITE_ERROR;
  }
  return OK;
}

ErrorCode FileWriter::sync() {
  if (fd_ < 0) {
    // File was either never opened or already closed
    return OK;
  }
  const auto &options = threadCtx_.getOptions();
  if (options.fsync || options.isLogBasedResumption()) {
    PerfStatCollector statCollector(threadCtx_, PerfStatReport::FSYNC_STATS);
    if (::fsync(fd_) < 0) {
      WPLOG(ERROR) << "Unable to fsync() fd " << fd_;
      return FILE_WRITE_ERROR;
   }
  }
#ifdef HAS_POSIX_FADVISE
  if (!options.skip_fadvise) {
    PerfStatCollector statCollector(threadCtx_, PerfStatReport::FADVISE);
    if (posix_fadvise(fd_, blockDetails_->offset, blockDetails_->dataSize,
                      POSIX_FADV_DONTNEED) != 0) {
      WPLOG(ERROR) << "posix_fadvise failed for " << blockDetails_->fileName
                   << " " << blockDetails_->offset << " "
                   << blockDetails_->dataSize;
      return FILE_WRITE_ERROR;
    }
  }
#endif
  return OK;
}

ErrorCode FileWriter::close() {
  if (fd_ >= 0) {
    PerfStatCollector statCollector(threadCtx_, PerfStatReport::FILE_CLOSE);
    if (::close(fd_) != 0) {
      WPLOG(ERROR) << "Unable to close fd " << fd_;
      fd_ = -1;
      return FILE_WRITE_ERROR;
    }
    fd_ = -1;
  }
  return OK;
}

bool FileWriter::isClosed() {
  return fd_ < 0;
}

ErrorCode FileWriter::write(char *buf, int64_t size) {
  WDT_CHECK_NE(TO_BE_DELETED, blockDetails_->allocationStatus);
  auto &options = threadCtx_.getOptions();
  if (!options.skip_writes) {
    int64_t count = 0;
    while (count < size) {
      int64_t written;
      {
        PerfStatCollector statCollector(threadCtx_, PerfStatReport::FILE_WRITE);
        written = ::write(fd_, buf + count, size - count);
      }
      if (written == -1) {
        if (errno == EINTR) {
          WVLOG(1) << "Disk write interrupted, retrying "
                   << blockDetails_->fileName;
          continue;
        }
        WPLOG(ERROR) << "File write failed for " << blockDetails_->fileName
                     << "fd : " << fd_ << " " << written << " " << count << " "
                     << size;
        return FILE_WRITE_ERROR;
      }
      count += written;
    }
    WVLOG(1) << "Successfully written " << count << " bytes to fd " << fd_
             << " for file " << blockDetails_->fileName;
    const bool finished = ((totalWritten_ + size) == blockDetails_->dataSize);
    if (!syncFileRange(count, finished /*forced*/)) {
      return FILE_WRITE_ERROR;
    }
  }
  totalWritten_ += size;
  return OK;
}

bool FileWriter::syncFileRange(int64_t written, bool forced) {
#ifdef HAS_SYNC_FILE_RANGE
  const WdtOptions &options = threadCtx_.getOptions();
  if (options.disk_sync_interval_mb < 0) {
    return true;
  }
  const int64_t syncIntervalBytes = options.disk_sync_interval_mb * 1024 * 1024;
  writtenSinceLastSync_ += written;
  if (writtenSinceLastSync_ == 0) {
    // no need to sync
    WVLOG(1) << "skipping syncFileRange for " << blockDetails_->fileName
             << ". Data written " << written
             << " sync forced = " << std::boolalpha << forced;
    return true;
  }
  if (forced || writtenSinceLastSync_ > syncIntervalBytes) {
    // sync_file_range with flag SYNC_FILE_RANGE_WRITE is an asynchronous
    // operation. So, this is not that costly. Source :
    // http://yoshinorimatsunobu.blogspot.com/2014/03/how-syncfilerange-really-works.html
    int status;
    {
      PerfStatCollector statCollector(threadCtx_,
                                      PerfStatReport::SYNC_FILE_RANGE);
      status = sync_file_range(fd_, nextSyncOffset_, writtenSinceLastSync_,
                               SYNC_FILE_RANGE_WRITE);
      // NOTE: it may be beneficial to have the option to do fadvise after each
      // sync_file_range() call here.
    }
    if (status != 0) {
      WPLOG(ERROR) << "sync_file_range() failed for " << blockDetails_->fileName
                   << "fd " << fd_;
      return false;
    }
    WVLOG(1) << "file range [" << nextSyncOffset_ << " "
             << writtenSinceLastSync_ << "] synced for file "
             << blockDetails_->fileName;
    nextSyncOffset_ += writtenSinceLastSync_;
    writtenSinceLastSync_ = 0;
  }
#endif
  return true;
}
}
}
