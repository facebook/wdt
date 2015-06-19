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

FileByteSource::FileByteSource(SourceMetaData *metadata, uint64_t size,
                               uint64_t offset, size_t bufferSize)
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

  ErrorCode errCode = OK;
  if (!buffer_ || bufferSize_ > buffer_->size_) {
    buffer_.reset(new Buffer(bufferSize_));
  }
  const std::string &fullPath = metadata_->fullPath;
  START_PERF_TIMER
  fd_ = ::open(fullPath.c_str(), O_RDONLY);
  if (fd_ < 0) {
    errCode = BYTE_SOURCE_READ_ERROR;
    PLOG(ERROR) << "error opening file " << fullPath;
  } else {
    RECORD_PERF_RESULT(PerfStatReport::FILE_OPEN)
    if (offset_ > 0) {
      START_PERF_TIMER
      if (lseek(fd_, offset_, SEEK_SET) < 0) {
        errCode = BYTE_SOURCE_READ_ERROR;
        PLOG(ERROR) << "error seeking file " << fullPath;
      } else {
        RECORD_PERF_RESULT(PerfStatReport::FILE_SEEK)
      }
    }
  }
  transferStats_.setErrorCode(errCode);
  return errCode;
}

char *FileByteSource::read(size_t &size) {
  size = 0;
  if (hasError() || finished()) {
    return nullptr;
  }
  START_PERF_TIMER
  size_t toRead =
      (size_t)std::min<uint64_t>(buffer_->size_, size_ - bytesRead_);
  ssize_t numRead = ::read(fd_, buffer_->data_, toRead);
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
  bytesRead_ += numRead;
  size = numRead;
  return buffer_->data_;
}
}
}
