/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/CommonImpl.h>

namespace facebook {
namespace wdt {

Buffer::Buffer(const int64_t size) {
  WDT_CHECK_EQ(0, size % kDiskBlockSize);
  isAligned_ = false;
  size_ = 0;
#ifdef HAS_POSIX_MEMALIGN
  // always allocate aligned buffer if possible
  int ret = posix_memalign((void**)&data_, kDiskBlockSize, size);
  if (ret || data_ == nullptr) {
    WLOG(ERROR) << "posix_memalign failed " << strerrorStr(ret) << " size "
                << size;
    return;
  }
  WVLOG(1) << "Allocated aligned memory " << size;
  isAligned_ = true;
  size_ = size;
  return;
#else
  data_ = (char*)malloc(size);
  if (data_ == nullptr) {
    WLOG(ERROR) << "Failed to allocate memory using malloc " << size;
    return;
  }
  WVLOG(1) << "Allocated unaligned memory " << size;
  size_ = size;
#endif
}

char* Buffer::getData() const {
  return data_;
}

bool Buffer::isAligned() const {
  return isAligned_;
}

int64_t Buffer::getSize() const {
  return size_;
}

Buffer::~Buffer() {
  if (data_ != nullptr) {
    free(data_);
  }
}

ThreadCtx::ThreadCtx(const WdtOptions& options, bool allocateBuffer)
    : options_(options), perfReport_(options) {
  if (!allocateBuffer) {
    return;
  }
  buffer_ = std::make_unique<Buffer>(options_.buffer_size);
}

ThreadCtx::ThreadCtx(const WdtOptions& options, bool allocateBuffer,
                     int threadIndex)
    : ThreadCtx(options, allocateBuffer) {
  threadIndex_ = threadIndex;
}

const WdtOptions& ThreadCtx::getOptions() const {
  return options_;
}

int ThreadCtx::getThreadIndex() const {
  WDT_CHECK_GE(threadIndex_, 0);
  return threadIndex_;
}

const Buffer* ThreadCtx::getBuffer() const {
  return buffer_.get();
}

PerfStatReport& ThreadCtx::getPerfReport() {
  return perfReport_;
}

void ThreadCtx::setAbortChecker(IAbortChecker const* abortChecker) {
  abortChecker_ = abortChecker;
}

const IAbortChecker* ThreadCtx::getAbortChecker() const {
  return abortChecker_;
}
}
}
