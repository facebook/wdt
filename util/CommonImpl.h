/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/Reporting.h>

namespace facebook {
namespace wdt {

const int64_t kDiskBlockSize = 4 * 1024;

/// class representing a buffer
class Buffer {
 public:
  /// @param size     size to allocate
  explicit Buffer(const int64_t size);

  /// @return   buffer ptr
  char *getData() const;

  /// @return   whether the allocated buffer is aligned
  bool isAligned() const;

  /// @return   buffer size
  int64_t getSize() const;

  ~Buffer();

  // making the object non-copyable and non-moveable
  Buffer(const Buffer &stats) = delete;
  Buffer &operator=(const Buffer &stats) = delete;
  Buffer(Buffer &&stats) = delete;
  Buffer &operator=(Buffer &&stats) = delete;

 private:
  char *data_{nullptr};
  int64_t size_{0};
  bool isAligned_{false};
};

/// class representing thread context
class ThreadCtx {
 public:
  /// @param  options        options to use
  /// @param  allocateBuffer whether to allocate buffer
  ThreadCtx(const WdtOptions &options, bool allocateBuffer);

  /// @param  options        options to use
  /// @param  allocateBuffer whether to allocate buffer
  /// @param  threadIndex    index of the thread
  ThreadCtx(const WdtOptions &options, bool allocateBuffer, int threadIndex);

  /// @return   options to use
  const WdtOptions &getOptions() const;

  /// @param    thread index
  int getThreadIndex() const;

  /// @return   buffer to use
  const Buffer *getBuffer() const;

  /// @return   perf stat reporter
  PerfStatReport &getPerfReport();

  /// @param    abort checker to use
  void setAbortChecker(IAbortChecker const *abortChecker);

  /// @return   abort checker to use
  const IAbortChecker *getAbortChecker() const;

  // making the object non-copyable and non-moveable
  ThreadCtx(const ThreadCtx &stats) = delete;
  ThreadCtx &operator=(const ThreadCtx &stats) = delete;
  ThreadCtx(ThreadCtx &&stats) = delete;
  ThreadCtx &operator=(ThreadCtx &&stats) = delete;

 private:
  const WdtOptions &options_;
  int threadIndex_{-1};
  std::unique_ptr<Buffer> buffer_{nullptr};
  PerfStatReport perfReport_;
  IAbortChecker const *abortChecker_{nullptr};
};

/// util class to collect perf stat
class PerfStatCollector {
 public:
  PerfStatCollector(ThreadCtx &threadCtx,
                    const PerfStatReport::StatType statType)
      : threadCtx_(threadCtx), statType_(statType) {
    if (threadCtx_.getOptions().enable_perf_stat_collection) {
      startTime_ = Clock::now();
    }
  }

  ~PerfStatCollector() {
    if (threadCtx_.getOptions().enable_perf_stat_collection) {
      int64_t duration = durationMicros(Clock::now() - startTime_);
      threadCtx_.getPerfReport().addPerfStat(statType_, duration);
    }
  }

 private:
  ThreadCtx &threadCtx_;
  const PerfStatReport::StatType statType_;
  Clock::time_point startTime_;
};
}
}
