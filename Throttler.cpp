/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Throttler.h>
#include <wdt/ErrorCodes.h>
#include <wdt/WdtOptions.h>
#include <cmath>

namespace facebook {
namespace wdt {

// Constants for different calculations
const int64_t kMillisecsPerSec = 1000;
const double kPeakMultiplier = 1.2;
const int kBucketMultiplier = 2;
const double kTimeMultiplier = 0.25;

std::shared_ptr<Throttler> Throttler::makeThrottler(const WdtOptions& options) {
  double avgRateBytesPerSec = options.avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = options.max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = options.throttler_bucket_limit * kMbToB;
  return Throttler::makeThrottler(avgRateBytesPerSec, peakRateBytesPerSec,
                                  bucketLimitBytes,
                                  options.throttler_log_time_millis);
}

std::shared_ptr<Throttler> Throttler::makeThrottler(
    double avgRateBytesPerSec, double peakRateBytesPerSec,
    double bucketLimitBytes, int64_t throttlerLogTimeMillis) {
  configureOptions(avgRateBytesPerSec, peakRateBytesPerSec, bucketLimitBytes);
  if (avgRateBytesPerSec > 0 || peakRateBytesPerSec > 0) {
    return std::make_shared<Throttler>(avgRateBytesPerSec, peakRateBytesPerSec,
                                       bucketLimitBytes,
                                       throttlerLogTimeMillis);
  }
  return nullptr;
}

void Throttler::configureOptions(double& avgRateBytesPerSec,
                                 double& peakRateBytesPerSec,
                                 double& bucketLimitBytes) {
  if (peakRateBytesPerSec < avgRateBytesPerSec && peakRateBytesPerSec >= 0) {
    LOG(WARNING) << "Per thread peak rate should be greater "
                 << "than per thread average rate. "
                 << "Making peak rate 1.2 times the average rate";
    peakRateBytesPerSec = kPeakMultiplier * (double)avgRateBytesPerSec;
  }
  if (bucketLimitBytes <= 0 && peakRateBytesPerSec > 0) {
    bucketLimitBytes =
        kTimeMultiplier * kBucketMultiplier * peakRateBytesPerSec;
    LOG(INFO) << "Burst limit not specified but peak "
              << "rate is configured. Auto configuring to "
              << bucketLimitBytes / kMbToB << " Mbytes";
  }
}

Throttler::Throttler(double avgRateBytesPerSec, double peakRateBytesPerSec,
                     double bucketLimitBytes, int64_t throttlerLogTimeMillis)
    : avgRateBytesPerSec_(avgRateBytesPerSec) {
  bucketRateBytesPerSec_ = peakRateBytesPerSec;
  bytesTokenBucketLimit_ = 2 * bucketRateBytesPerSec_ * 0.25;
  /* We keep the number of tokens generated as zero initially
   * It could be argued that we keep this filled when we created the
   * bucket. However the startTime is passed in this case and the hope is
   * that we will have enough number of tokens by the time we send the data
   */
  bytesTokenBucket_ = 0;
  if (bucketLimitBytes > 0) {
    bytesTokenBucketLimit_ = bucketLimitBytes;
  }
  if (avgRateBytesPerSec > 0) {
    LOG(INFO) << "Average rate " << avgRateBytesPerSec_ / kMbToB
              << " Mbytes/sec";
  } else {
    LOG(INFO) << "No average rate specified";
  }
  if (bucketRateBytesPerSec_ > 0) {
    LOG(INFO) << "Peak rate " << bucketRateBytesPerSec_ / kMbToB
              << " Mbytes/sec.  Bucket limit "
              << bytesTokenBucketLimit_ / kMbToB << " Mbytes.";
  } else {
    LOG(INFO) << "No peak rate specified";
  }
  throttlerLogTimeMillis_ = throttlerLogTimeMillis;
}

void Throttler::setThrottlerRates(double& avgRateBytesPerSec,
                                  double& bucketRateBytesPerSec,
                                  double& bytesTokenBucketLimit) {
  // configureThrottlerOptions will change the rates in case they don't make
  // sense
  configureOptions(avgRateBytesPerSec, bucketRateBytesPerSec,
                   bytesTokenBucketLimit);
  folly::SpinLockGuard lock(throttlerMutex_);
  if (refCount_ > 0 && avgRateBytesPerSec < avgRateBytesPerSec_) {
    LOG(INFO) << "new avg rate : " << avgRateBytesPerSec
              << " cur avg rate : " << avgRateBytesPerSec_
              << " Average throttler rate can't be "
              << "lowered mid transfer. Ignoring the new value";
    avgRateBytesPerSec = avgRateBytesPerSec_;
  }

  LOG(INFO) << "Updating the rates avgRateBytesPerSec : " << avgRateBytesPerSec
            << " bucketRateBytesPerSec : " << bucketRateBytesPerSec
            << " bytesTokenBucketLimit : " << bytesTokenBucketLimit;
  avgRateBytesPerSec_ = avgRateBytesPerSec;
  bucketRateBytesPerSec_ = bucketRateBytesPerSec;
  bytesTokenBucketLimit_ = bytesTokenBucketLimit;
}

void Throttler::limit(double deltaProgress) {
  // now should be before taking the lock
  std::chrono::time_point<Clock> now = Clock::now();
  double sleepTimeSeconds = calculateSleep(deltaProgress, now);
  if (throttlerLogTimeMillis_ > 0) {
    printPeriodicLogs(now, deltaProgress);
  }
  if (sleepTimeSeconds > 0) {
    /* sleep override */
    START_PERF_TIMER
    std::this_thread::sleep_for(
        std::chrono::duration<double>(sleepTimeSeconds));
    RECORD_PERF_RESULT(PerfStatReport::THROTTLER_SLEEP)
  }
}

double Throttler::calculateSleep(double deltaProgress,
                                 const Clock::time_point& now) {
  folly::SpinLockGuard lock(throttlerMutex_);
  if (refCount_ <= 0) {
    LOG(ERROR) << "Using the throttler without registering the transfer";
    return -1;
  }
  bytesProgress_ += deltaProgress;
  double avgThrottlerSleep = averageThrottler(now);
  const bool willSleep = (avgThrottlerSleep > 0);
  if (willSleep) {
    return avgThrottlerSleep;
  }
  // we still hold the lock if peak throttler can come into effect
  if ((bucketRateBytesPerSec_ > 0) && (bytesTokenBucketLimit_ > 0)) {
    std::chrono::duration<double> elapsedDuration = now - lastFillTime_;
    lastFillTime_ = now;
    double elapsedSeconds = elapsedDuration.count();
    bytesTokenBucket_ += elapsedSeconds * bucketRateBytesPerSec_;
    if (bytesTokenBucket_ > bytesTokenBucketLimit_) {
      bytesTokenBucket_ = bytesTokenBucketLimit_;
    }
    bytesTokenBucket_ -= deltaProgress;
    if (bytesTokenBucket_ < 0) {
      /*
       * If we have negative number of tokens lets sleep
       * This way we will have positive number of tokens next time
       */
      double peakThrottlerSleep =
          -1.0 * bytesTokenBucket_ / bucketRateBytesPerSec_;
      VLOG(2) << "Peak throttler wants to sleep " << peakThrottlerSleep
              << " seconds";
      return peakThrottlerSleep;
    }
  }
  return -1;
}

void Throttler::printPeriodicLogs(const Clock::time_point& now,
                                  double deltaProgress) {
  /*
   * This is the part where throttler prints out the progress
   * made periodically.
   */
  std::chrono::duration<double> elapsedLogDuration;
  folly::SpinLockGuard lock(throttlerMutex_);
  instantProgress_ += deltaProgress;
  elapsedLogDuration = now - lastLogTime_;
  double elapsedLogSeconds = elapsedLogDuration.count();
  if (elapsedLogSeconds * kMillisecsPerSec >= throttlerLogTimeMillis_) {
    double instantBytesPerSec = 0;
    instantBytesPerSec = instantProgress_ / elapsedLogSeconds;
    instantProgress_ = 0;
    lastLogTime_ = now;
    std::chrono::duration<double> elapsedAvgDuration = now - startTime_;
    double elapsedAvgSeconds = elapsedAvgDuration.count();
    double avgBytesPerSec = bytesProgress_ / elapsedAvgSeconds;
    LOG(INFO) << "Throttler:Transfer_Rates::"
              << " " << elapsedAvgSeconds << " " << avgBytesPerSec / kMbToB
              << " " << instantBytesPerSec / kMbToB << " " << deltaProgress;
  }
}

double Throttler::averageThrottler(const Clock::time_point& now) {
  std::chrono::duration<double> elapsedDuration = now - startTime_;
  double elapsedSeconds = elapsedDuration.count();
  if (avgRateBytesPerSec_ <= 0) {
    VLOG(2) << "There is no avg rate limit";
    return -1;
  }
  const double allowedProgressBytes = avgRateBytesPerSec_ * elapsedSeconds;
  if (bytesProgress_ > allowedProgressBytes) {
    double idealTime = bytesProgress_ / avgRateBytesPerSec_;
    const double sleepTimeSeconds = idealTime - elapsedSeconds;
    VLOG(1) << "Throttler : Elapsed " << elapsedSeconds
            << " seconds. Made progress " << bytesProgress_ / kMbToB
            << " Mbytes in " << elapsedSeconds
            << " seconds, maximum allowed progress for this duration is "
            << allowedProgressBytes / kMbToB << " Mbytes. Mean Rate allowed is "
            << avgRateBytesPerSec_ / kMbToB << " Mbytes/sec. Sleeping for "
            << sleepTimeSeconds << " seconds";
    return sleepTimeSeconds;
  }
  return -1;
}

void Throttler::registerTransfer() {
  folly::SpinLockGuard lock(throttlerMutex_);
  if (refCount_ == 0) {
    startTime_ = Clock::now();
    lastFillTime_ = startTime_;
    lastLogTime_ = startTime_;
    instantProgress_ = 0;
    bytesProgress_ = 0;
    bytesTokenBucket_ = 0;
  }
  refCount_++;
}

void Throttler::deRegisterTransfer() {
  folly::SpinLockGuard lock(throttlerMutex_);
  WDT_CHECK(refCount_ > 0);
  refCount_--;
}

double Throttler::getBytesProgress() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return bytesProgress_;
}

double Throttler::getAvgRateBytesPerSec() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return avgRateBytesPerSec_;
}

double Throttler::getPeakRateBytesPerSec() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return bucketRateBytesPerSec_;
}

double Throttler::getBucketLimitBytes() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return bytesTokenBucketLimit_;
}

int64_t Throttler::getThrottlerLogTimeMillis() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return throttlerLogTimeMillis_;
}

void Throttler::setThrottlerLogTimeMillis(int64_t throttlerLogTimeMillis) {
  folly::SpinLockGuard lock(throttlerMutex_);
  throttlerLogTimeMillis_ = throttlerLogTimeMillis;
}

std::ostream& operator<<(std::ostream& stream, const Throttler& throttler) {
  stream << "avgRate: " << throttler.avgRateBytesPerSec_ / kMbToB
         << " Mbytes/sec, peakRate: "
         << throttler.bucketRateBytesPerSec_ / kMbToB
         << " Mbytes/sec, bucketLimit: "
         << throttler.bytesTokenBucketLimit_ / kMbToB
         << " Mbytes, throttlerLogTimeMillis: "
         << throttler.throttlerLogTimeMillis_;
  return stream;
}
}
}
