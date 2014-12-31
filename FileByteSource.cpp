#include "FileByteSource.h"

#include <algorithm>
#include <fcntl.h>
#include <glog/logging.h>
#include <sys/types.h>
#include <sys/stat.h>

namespace facebook {
namespace wdt {

folly::ThreadLocalPtr<FileByteSource::Buffer> FileByteSource::buffer_;

FileByteSource::FileByteSource(const std::string &rootPath,
                               const std::string &relPath, uint64_t size,
                               size_t bufferSize)
    : rootPath_(rootPath),
      relPath_(relPath),
      size_(size),
      bytesRead_(0),
      bufferSize_(bufferSize) {
  transferStats_.setId(relPath_);
}

void FileByteSource::open() {
  bytesRead_ = 0;
  if (fd_ >= 0) {
    close(fd_);
  }

  if (!buffer_ || bufferSize_ > buffer_->size_) {
    buffer_.reset(new Buffer(bufferSize_));
  }
  const std::string fullPath = rootPath_ + relPath_;
  fd_ = ::open(fullPath.c_str(), O_RDONLY);
  if (fd_ < 0) {
    PLOG(ERROR) << "error opening file " << fullPath;
  }
}

char *FileByteSource::read(size_t &size) {
  size = 0;
  if (hasError() || finished()) {
    return nullptr;
  }
  size_t toRead =
      (size_t)std::min<uint64_t>(buffer_->size_, size_ - bytesRead_);
  ssize_t numRead = ::read(fd_, buffer_->data_, toRead);
  if (numRead < 0) {
    PLOG(ERROR) << "failure while reading file " << rootPath_ + relPath_;
    close(fd_);
    fd_ = -1;
    return nullptr;
  }
  if (numRead == 0) {
    close(fd_);
    fd_ = -1;
    return nullptr;
  }
  bytesRead_ += numRead;
  size = numRead;
  return buffer_->data_;
}
}
}
