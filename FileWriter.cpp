#include "FileWriter.h"
#include "WdtOptions.h"
#include "Reporting.h"

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
  if (blockDetails_->fileSize == blockDetails_->dataSize) {
    // single block file
    WDT_CHECK(blockDetails_->offset == 0);
    fd_ = fileCreator_->openAndSetSize(blockDetails_);
  } else {
    // multi block file
    fd_ = fileCreator_->openForBlocks(threadIndex_, blockDetails_);
    if (fd_ >= 0 && blockDetails_->offset > 0) {
      START_PERF_TIMER
      if (lseek(fd_, blockDetails_->offset, SEEK_SET) < 0) {
        PLOG(ERROR) << "Unable to seek " << blockDetails_->fileName;
        close();
      } else {
        RECORD_PERF_RESULT(PerfStatReport::FILE_SEEK)
      }
    }
  }
  if (fd_ == -1) {
    LOG(ERROR) << "File open/seek failed for " << blockDetails_->fileName;
    return FILE_WRITE_ERROR;
  }
  return OK;
}

void FileWriter::close() {
  if (fd_ >= 0) {
    START_PERF_TIMER
    if (::close(fd_) != 0) {
      PLOG(ERROR) << "Unable to close fd " << fd_;
    }
    RECORD_PERF_RESULT(PerfStatReport::FILE_CLOSE)
    fd_ = -1;
  }
}

ErrorCode FileWriter::write(char *buf, int64_t size) {
  auto &options = WdtOptions::get();
  if (!options.skip_writes) {
    ssize_t count = 0;
    while (count < size) {
      START_PERF_TIMER
      ssize_t written = ::write(fd_, buf + count, size - count);
      if (written == -1) {
        if (errno == EINTR) {
          VLOG(1) << "Disk write interrupted, retrying "
                  << blockDetails_->fileName;
          continue;
        }
        PLOG(ERROR) << "File write failed for " << fd_ << " " << written << " "
                    << count << " " << size;
        return FILE_WRITE_ERROR;
      }
      RECORD_PERF_RESULT(PerfStatReport::FILE_WRITE)
      count += written;
    }
    VLOG(1) << "Successfully written " << count << " bytes to fd " << fd_;
    bool finished = ((totalWritten_ + size) == blockDetails_->dataSize);
    if (options.enable_download_resumption && finished) {
      if (fsync(fd_) != 0) {
        PLOG(ERROR) << "fsync failed for " << blockDetails_->fileName
                    << " offset " << blockDetails_->offset << " file-size "
                    << blockDetails_->fileSize
                    << " data-size  << blockDetails_->dataSize";
        return FILE_WRITE_ERROR;
      }
    } else {
      syncFileRange(count, finished);
    }
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
    START_PERF_TIMER
    auto status = sync_file_range(fd_, nextSyncOffset_, writtenSinceLastSync_,
                                  SYNC_FILE_RANGE_WRITE);
    if (status != 0) {
      PLOG(ERROR) << "sync_file_range() failed for fd " << fd_;
      return;
    }
    RECORD_PERF_RESULT(PerfStatReport::SYNC_FILE_RANGE)
    VLOG(1) << "file range synced " << nextSyncOffset_ << " "
            << writtenSinceLastSync_;
    nextSyncOffset_ += writtenSinceLastSync_;
    writtenSinceLastSync_ = 0;
  }
#endif
}
}
}
