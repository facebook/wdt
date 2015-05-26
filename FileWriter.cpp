#include "FileWriter.h"
#include "WdtOptions.h"

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>

namespace facebook {
namespace wdt {

ErrorCode FileWriter::open() {
  auto &options = WdtOptions::get();
  if (options.skip_writes) {
    return OK;
  }
  if (fileSize_ == dataSize_) {
    // single block file
    WDT_CHECK(offset_ == 0);
    fd_ = fileCreator_->openAndSetSize(fileName_, fileSize_);
  } else {
    // multi block file
    fd_ =
        fileCreator_->openForBlocks(threadIndex_, fileName_, seqId_, fileSize_);
    if (fd_ >= 0 && offset_ > 0 && lseek(fd_, offset_, SEEK_SET) < 0) {
      PLOG(ERROR) << "Unable to seek " << fileName_;
      close();
    }
  }
  if (fd_ == -1) {
    LOG(ERROR) << "File open/seek failed for " << fileName_;
    return FILE_WRITE_ERROR;
  }
  return OK;
}

void FileWriter::close() {
  if (fd_ >= 0) {
    if (::close(fd_) != 0) {
      PLOG(ERROR) << "Unable to close fd " << fd_;
    }
    fd_ = -1;
  }
}

ErrorCode FileWriter::write(char *buf, int64_t size) {
  auto &options = WdtOptions::get();
  if (!options.skip_writes) {
    ssize_t count = 0;
    while (count < size) {
      ssize_t written = ::write(fd_, buf + count, size - count);
      if (written == -1) {
        if (errno == EINTR) {
          VLOG(1) << "Disk write interrupted, retrying " << fileName_;
          continue;
        }
        PLOG(ERROR) << "File write failed for " << fd_ << " " << written << " "
                    << count << " " << size;
        return FILE_WRITE_ERROR;
      }
      count += written;
    }
    VLOG(1) << "Successfully written " << count << " bytes to fd " << fd_;
    bool finished = ((totalWritten_ + size) == dataSize_);
    syncFileRange(count, finished);
  }
  totalWritten_ += size;
  return OK;
}

void FileWriter::syncFileRange(uint64_t written, bool forced) {
#ifdef HAS_SYNC_FILE_RANGE
  auto &options = WdtOptions::get();
  int64_t syncIntervalBytes = options.disk_sync_interval_mb * 1024 * 1024;
  if (syncIntervalBytes < 0) {
    return;
  }
  writtenSinceLastSync_ += written;
  if (writtenSinceLastSync_ == 0) {
    // no need to sync
    VLOG(1) << "skipping syncFileRange " << written << "  " << forced;
    return;
  }
  if (forced || writtenSinceLastSync_ > syncIntervalBytes) {
    // sync_file_range with flag SYNC_FILE_RANGE_WRITE is an asynchronous
    // operation. So, this is not that costly. Source :
    // http://yoshinorimatsunobu.blogspot.com/2014/03/how-syncfilerange-really-works.html
    auto status = sync_file_range(fd_, nextSyncOffset_, writtenSinceLastSync_,
                                  SYNC_FILE_RANGE_WRITE);
    if (status != 0) {
      PLOG(ERROR) << "sync_file_range() failed for fd " << fd_;
      return;
    }
    VLOG(1) << "file range synced " << nextSyncOffset_ << " "
            << writtenSinceLastSync_;
    nextSyncOffset_ += writtenSinceLastSync_;
    writtenSinceLastSync_ = 0;
  }
#endif
}
}
}
