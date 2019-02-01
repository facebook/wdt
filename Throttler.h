/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <folly/SpinLock.h>
#include <glog/logging.h>
#include <wdt/util/CommonImpl.h>
#include <thread>
namespace facebook {
namespace wdt {

struct ThrottlerOptions {
  /**
   * Rate at which we would like to throttle,
   * specifying this as < 0 makes it unlimited
   */
  double avg_rate_per_sec{-1};
  /**
   * Rate at which tokens will be generated in TB algorithm.
   * When we lag behind, this will allow us to go faster,
   * specify as < 0 to make it unlimited. Specify as 0
   * for auto configuring.
   * auto conf as (kPeakMultiplier * avgRatePerSec_)
   * Find details in Throttler.cpp
   */
  double max_rate_per_sec{0};
  /**
   * Maximum bucket size of TB algorithm.
   * This together with maxRatePerSec_ will
   * make for maximum burst rate. If specified as 0 it is
   * auto configured in Throttler.cpp
   */
  double throttler_bucket_limit{0};
  /**
   * Throttler logs statistics like average and max rate.
   * This option specifies on how frequently should those
   * be logged
   */
  int64_t throttler_log_time_millis{0};
  /**
   * Limit for single request. If the requested size is greater than this, the
   * request is broken in chunks of this size and processed sequentially.
   * This ensures that a thread asking for large amount of resource does not
   * starve other threads asking for small amount of resource.
   */
  int64_t single_request_limit{1};
};

/**
 * Attempts to limit the rate in two ways.
 * 1. Limit average rate by calling averageThrottler()
 * 2. Limit the peak rate by calling limitByTokenBucket
 * Generally average throttler would be maintaining the rate to avgRate_
 * although at times the actual rate might fall behind and in those
 * circumstances the rate at which you will catch up is limited
 * with respect to the peak rate and the bucket limit using the
 * token bucket algorithm.
 * Token Bucket algorithm can be found on
 * http://en.wikipedia.org/wiki/Token_bucket
 */
class Throttler {
 public:
  /// Utility method that makes throttler using options.
  static std::shared_ptr<Throttler> makeThrottler(
      const ThrottlerOptions& options);

  explicit Throttler(const ThrottlerOptions& options);

  /**
   * Calls calculateSleep which is a thread safe method. Finds out the
   * time thread has to sleep and makes it sleep.
   * Also calls the throttler logger to log the stats
   */
  virtual void limit(ThreadCtx& threadCtx, int64_t deltaProgress);

  /**
   * Same as the other limit, but without reporting for sleep duration
   */
  virtual void limit(int64_t deltaProgress);

  /**
   * This is thread safe implementation of token bucket
   * algorithm. Bucket is filled at the rate of bucketRatePerSec_
   * till the limit of tokenBucketLimit_
   * There is no sleep, we just calculate how much to sleep.
   * This method also calls the averageThrottler inside
   * @param deltaProgress         Progress since the last limit call
   */
  virtual double calculateSleep(double totalProgress,
                                const Clock::time_point& now);

  /// Provides anyone using this throttler instance to update the throttler
  /// rates. The rates might be configured to different values than what
  /// were passed.
  virtual void setThrottlerRates(double& avgRatePerSec,
                                 double& bucketRatePerSec,
                                 double& tokenBucketLimit);

  /// Utility method that set throttler rate using options
  void setThrottlerRates(const ThrottlerOptions& options);

  virtual ~Throttler() {
  }

  /// Anyone who is using the throttler should call this method to maintain
  /// the refCount_ and startTime_ correctly
  void startTransfer();

  /// Method to de-register the transfer and decrement the refCount_
  void endTransfer();

  /// Get the average rate per sec
  double getAvgRatePerSec();

  /// Get the bucket rate per sec
  double getPeakRatePerSec();

  /// Get the bucket limit
  double getBucketLimit();

  /// Get the throttler logging time period in millis
  int64_t getThrottlerLogTimeMillis();

  /// Set the throttler logging time in millis
  void setThrottlerLogTimeMillis(int64_t throttlerLogTimeMillis);

  /// Get tokens processed till now
  double getProgress();

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Throttler& throttler);

 private:
  /**
   * Sometimes the options passed to throttler might not make sense so this
   * method tries to auto configure them
   */
  static void configureOptions(double& avgRatePerSec, double& peakRatePerSec,
                               double& bucketLimit);

  /**
   * This method is invoked repeatedly with the amount of progress made
   * (e.g. number of tokens processed) till now. If the total progress
   * till now is over the allowed average progress then it returns the
   * time to sleep for the calling thread
   * @param now                       Pass in the current time stamp
   */
  double averageThrottler(const Clock::time_point& now);

  void limitInternal(ThreadCtx* threadCtx, int64_t deltaProgress);

  void limitSingleRequest(ThreadCtx* threadCtx, int64_t deltaProgress);

  void sleep(double sleepTimeSecs) const;

  void resetState();

  /**
   * This method periodically prints logs.
   * The period is defined by FLAGS_peak_log_time_ms
   * @param deltaProgress      Progress since last call to limit()
   * @param now                The time point caller has
   * @param sleepTimeSeconds   Duration of sleep caused by limit()
   */
  void printPeriodicLogs(const Clock::time_point& now, double deltaProgress);
  /// Records the time the throttler was started
  Clock::time_point startTime_;

  /**
   * Throttler logs the average and instantaneous progress
   * periodically (check FLAGS_peak_log_time_ms). lastLogTime_ is
   * the last time this log was written
   */
  Clock::time_point lastLogTime_;
  /// Instant progress in the time stats were logged last time
  double instantProgress_{0};
  // Records the total progress in tokens till now
  double progress_{0};
  /// Last time the token bucket was filled
  std::chrono::time_point<std::chrono::high_resolution_clock> lastFillTime_;

 protected:
  /// Number of tokens in the bucket
  int64_t tokenBucket_;
  /// Controls the access across threads
  folly::SpinLock throttlerMutex_;
  /// Number of users of this throttler
  int64_t refCount_{0};
  /// The average rate expected
  double avgRatePerSec_;
  /// Limit on the max number of tokens
  double tokenBucketLimit_;
  /// Rate at which bucket is filled
  double bucketRatePerSec_;
  /// Max number of tokens that can be requested in a single call
  int64_t singleRequestLimit_;
  /// Interval between every print of throttler logs
  int64_t throttlerLogTimeMillis_;
};
}
}  // facebook::wdt
