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

namespace facebook {
namespace wdt {

// Constants for different calculations
const int64_t kMillisecsPerSec = 1000;
const double kPeakMultiplier = 1.2;
const int kBucketMultiplier = 2;
const double kTimeMultiplier = 0.25;

std::shared_ptr<Throttler> Throttler::makeThrottler(
    const ThrottlerOptions& options) {
  return std::make_shared<Throttler>(options);
}

void Throttler::configureOptions(double& avgRatePerSec, double& peakRatePerSec,
                                 double& bucketLimit) {
  if (peakRatePerSec < avgRatePerSec && peakRatePerSec >= 0) {
    WLOG(WARNING) << "Per thread peak rate should be greater "
                  << "than per thread average rate. "
                  << "Making peak rate 1.2 times the average rate";
    peakRatePerSec = kPeakMultiplier * (double)avgRatePerSec;
  }
  if (bucketLimit <= 0 && peakRatePerSec > 0) {
    bucketLimit = kTimeMultiplier * kBucketMultiplier * peakRatePerSec;
    WLOG(INFO) << "Burst limit not specified but peak "
               << "rate is configured. Auto configuring to " << bucketLimit;
  }
}

Throttler::Throttler(const ThrottlerOptions& options)
    : avgRatePerSec_(options.avg_rate_per_sec) {
  bucketRatePerSec_ = options.max_rate_per_sec;
  tokenBucketLimit_ = kTimeMultiplier * kBucketMultiplier * bucketRatePerSec_;
  /* We keep the number of tokens generated as zero initially
   * It could be argued that we keep this filled when we created the
   * bucket. However the startTime is passed in this case and the hope is
   * that we will have enough number of tokens by the time we send the data
   */
  tokenBucket_ = 0;
  if (options.throttler_bucket_limit > 0) {
    tokenBucketLimit_ = options.throttler_bucket_limit;
  }
  if (avgRatePerSec_ > 0) {
    WLOG(INFO) << "Average rate " << avgRatePerSec_;
  } else {
    WLOG(INFO) << "No average rate specified";
  }
  if (bucketRatePerSec_ > 0) {
    WLOG(INFO) << "Peak rate " << bucketRatePerSec_ << ".  Bucket limit "
               << tokenBucketLimit_;
  } else {
    WLOG(INFO) << "No peak rate specified";
  }
  WDT_CHECK_GT(options.single_request_limit, 0);
  singleRequestLimit_ = options.single_request_limit;
  throttlerLogTimeMillis_ = options.throttler_log_time_millis;
}

void Throttler::setThrottlerRates(double& avgRatePerSec,
                                  double& bucketRatePerSec,
                                  double& tokenBucketLimit) {
  // configureOptions will change the rates in case they don't make
  // sense
  configureOptions(avgRatePerSec, bucketRatePerSec, tokenBucketLimit);
  folly::SpinLockGuard lock(throttlerMutex_);

  resetState();

  WLOG(INFO) << "Updating the rates avgRatePerSec : " << avgRatePerSec
             << " bucketRatePerSec : " << bucketRatePerSec
             << " tokenBucketLimit : " << tokenBucketLimit;
  avgRatePerSec_ = avgRatePerSec;
  bucketRatePerSec_ = bucketRatePerSec;
  tokenBucketLimit_ = tokenBucketLimit;
}

void Throttler::setThrottlerRates(const ThrottlerOptions& options) {
  double avgRatePerSec = options.avg_rate_per_sec;
  double peakRatePerSec = options.max_rate_per_sec;
  double bucketLimit = options.throttler_bucket_limit;
  setThrottlerRates(avgRatePerSec, peakRatePerSec, bucketLimit);
}

void Throttler::limit(ThreadCtx& threadCtx, int64_t deltaProgress) {
  limitInternal(&threadCtx, deltaProgress);
}

void Throttler::limit(int64_t deltaProgress) {
  limitInternal(nullptr, deltaProgress);
}

void Throttler::limitInternal(ThreadCtx* threadCtx, int64_t deltaProgress) {
  const int kLogInterval = 100;
  int64_t numThrottled = 0;
  int64_t count = 0;
  while (numThrottled < deltaProgress) {
    const int64_t toThrottle =
        std::min(singleRequestLimit_, deltaProgress - numThrottled);
    limitSingleRequest(threadCtx, toThrottle);
    numThrottled += toThrottle;
    count++;
    if (count % kLogInterval == 0) {
      WLOG(INFO) << "Throttling large amount data, to-throttle: "
                 << deltaProgress << ", num-throttled: " << numThrottled;
    }
  }
}

void Throttler::limitSingleRequest(ThreadCtx* threadCtx,
                                   int64_t deltaProgress) {
  WDT_CHECK_LE(deltaProgress, singleRequestLimit_);
  std::chrono::time_point<Clock> now = Clock::now();
  double sleepTimeSeconds = calculateSleep(deltaProgress, now);
  if (throttlerLogTimeMillis_ > 0) {
    printPeriodicLogs(now, deltaProgress);
  }
  if (sleepTimeSeconds <= 0) {
    return;
  }
  if (threadCtx == nullptr) {
    sleep(sleepTimeSeconds);
    return;
  }
  PerfStatCollector statCollector(*threadCtx, PerfStatReport::THROTTLER_SLEEP);
  sleep(sleepTimeSeconds);
}

void Throttler::sleep(double sleepTimeSecs) const {
  /* sleep override */
  std::this_thread::sleep_for(std::chrono::duration<double>(sleepTimeSecs));
}

double Throttler::calculateSleep(double deltaProgress,
                                 const Clock::time_point& now) {
  folly::SpinLockGuard lock(throttlerMutex_);
  if (refCount_ <= 0) {
    WLOG(ERROR) << "Using the throttler without registering the transfer";
    return -1;
  }
  progress_ += deltaProgress;
  double avgThrottlerSleep = averageThrottler(now);
  const bool willSleep = (avgThrottlerSleep > 0);
  if (willSleep) {
    return avgThrottlerSleep;
  }
  // we still hold the lock if peak throttler can come into effect
  if ((bucketRatePerSec_ > 0) && (tokenBucketLimit_ > 0)) {
    std::chrono::duration<double> elapsedDuration = now - lastFillTime_;
    lastFillTime_ = now;
    double elapsedSeconds = elapsedDuration.count();
    tokenBucket_ += elapsedSeconds * bucketRatePerSec_;
    if (tokenBucket_ > tokenBucketLimit_) {
      tokenBucket_ = tokenBucketLimit_;
    }
    tokenBucket_ -= deltaProgress;
    if (tokenBucket_ < 0) {
      /*
       * If we have negative number of tokens lets sleep
       * This way we will have positive number of tokens next time
       */
      double peakThrottlerSleep = -1.0 * tokenBucket_ / bucketRatePerSec_;
      WVLOG(2) << "Peak throttler wants to sleep " << peakThrottlerSleep
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
    double instantRatePerSec = 0;
    instantRatePerSec = instantProgress_ / elapsedLogSeconds;
    instantProgress_ = 0;
    lastLogTime_ = now;
    std::chrono::duration<double> elapsedAvgDuration = now - startTime_;
    double elapsedAvgSeconds = elapsedAvgDuration.count();
    double avgRatePerSec = progress_ / elapsedAvgSeconds;
    WLOG(INFO) << "Throttler:Transfer_Rates::"
               << " " << elapsedAvgSeconds << " " << avgRatePerSec << " "
               << instantRatePerSec << " " << deltaProgress;
  }
}

double Throttler::averageThrottler(const Clock::time_point& now) {
  std::chrono::duration<double> elapsedDuration = now - startTime_;
  double elapsedSeconds = elapsedDuration.count();
  if (avgRatePerSec_ <= 0) {
    WVLOG(2) << "There is no avg rate limit";
    return -1;
  }
  const double allowedProgress = avgRatePerSec_ * elapsedSeconds;
  if (progress_ > allowedProgress) {
    double idealTime = progress_ / avgRatePerSec_;
    const double sleepTimeSeconds = idealTime - elapsedSeconds;
    WVLOG(1) << "Throttler : Elapsed " << elapsedSeconds
             << " seconds. Made progress " << progress_ << " in "
             << elapsedSeconds
             << " seconds, maximum allowed progress for this duration is "
             << allowedProgress << ". Mean Rate allowed is " << avgRatePerSec_
             << " . Sleeping for " << sleepTimeSeconds << " seconds";
    return sleepTimeSeconds;
  }
  return -1;
}

void Throttler::startTransfer() {
  folly::SpinLockGuard lock(throttlerMutex_);
  if (refCount_ == 0) {
    resetState();
  }
  refCount_++;
}

void Throttler::resetState() {
  startTime_ = Clock::now();
  lastFillTime_ = startTime_;
  lastLogTime_ = startTime_;
  instantProgress_ = 0;
  progress_ = 0;
  tokenBucket_ = 0;
}

void Throttler::endTransfer() {
  folly::SpinLockGuard lock(throttlerMutex_);
  WDT_CHECK(refCount_ > 0);
  refCount_--;
}

double Throttler::getProgress() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return progress_;
}

double Throttler::getAvgRatePerSec() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return avgRatePerSec_;
}

double Throttler::getPeakRatePerSec() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return bucketRatePerSec_;
}

double Throttler::getBucketLimit() {
  folly::SpinLockGuard lock(throttlerMutex_);
  return tokenBucketLimit_;
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
  stream << "avgRate: " << throttler.avgRatePerSec_
         << ", peakRate: " << throttler.bucketRatePerSec_
         << ", bucketLimit: " << throttler.tokenBucketLimit_
         << ", throttlerLogTimeMillis: " << throttler.throttlerLogTimeMillis_;
  return stream;
}
}
}
