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
  /// Utility method that makes throttler using the wdt options. It can return
  /// nullptr if throttling is off
  static std::shared_ptr<Throttler> makeThrottler(const WdtOptions& options);

  /**
   * Sometimes the options passed to throttler might not make sense so this
   * method tries to auto configure them
   */
  static void configureOptions(double& avgRateBytesPerSec,
                               double& peakRateBytesPerSec,
                               double& bucketLimitBytes);

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
   * algorithm. Bucket is filled at the rate of bucketRateBytesPerSec_
   * till the limit of bytesTokenBucketLimit_
   * There is no sleep, we just calculate how much to sleep.
   * This method also calls the averageThrottler inside
   * @param deltaProgress         Progress since the last limit call
   */
  virtual double calculateSleep(double bytesTotalProgress,
                                const Clock::time_point& now);

  /// Provides anyone using this throttler instance to update the throttler
  /// rates. The rates might be configured to different values than what
  /// were passed.
  virtual void setThrottlerRates(double& avgRateBytesPerSec,
                                 double& bucketRateBytesPerSec,
                                 double& bytesTokenBucketLimit);

  /// Utility method that set throttler rate using the wdt options
  void setThrottlerRates(const WdtOptions& options);

  virtual ~Throttler() {
  }

  /// Anyone who is using the throttler should call this method to maintain
  /// the refCount_ and startTime_ correctly
  void startTransfer();

  /// Method to de-register the transfer and decrement the refCount_
  void endTransfer();

  /// Get the average rate in bytes per sec
  double getAvgRateBytesPerSec();

  /// Get the bucket rate in bytes per sec
  double getPeakRateBytesPerSec();

  /// Get the bucket limit in bytes
  double getBucketLimitBytes();

  /// Get the throttler logging time period in millis
  int64_t getThrottlerLogTimeMillis();

  /// Set the throttler logging time in millis
  void setThrottlerLogTimeMillis(int64_t throttlerLogTimeMillis);

  /// Get the bytes processed till now
  double getBytesProgress();

  friend std::ostream& operator<<(std::ostream& stream,
                                  const Throttler& throttler);

 private:
  /**
   * @param averageRateBytesPerSec    Average rate in progress/second
   *                                  at which data should be transmitted
   * @param peakRateBytesPerSec       Max burst rate allowed by the
   *                                  token bucket
   * @param bucketLimitBytes          Max size of bucket, specify 0 for auto
   *                                  configure. In auto mode, it will be twice
   *                                  the data you send in 1/4th of a second
   *                                  at the peak rate
   * @param singleRequestLimit        Internal limit to the maximum number of
   *                                  bytes that can be throttled in one call.
   *                                  If more bytes are requested to be
   *                                  throttled, that requested gets broken down
   *                                  and it is treated as multiple throttle
   *                                  calls.
   */
  Throttler(double avgRateBytesPerSec, double peakRateBytesPerSec,
            double bucketLimitBytes, int64_t singleRequestLimit,
            int64_t throttlerLogTimeMillis = 0);

  /**
   * This method is invoked repeatedly with the amount of progress made
   * (e.g. number of bytes written) till now. If the total progress
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
  // Records the total progress in bytes till now
  double bytesProgress_{0};
  /// Last time the token bucket was filled
  std::chrono::time_point<std::chrono::high_resolution_clock> lastFillTime_;

 protected:
  /// Number of tokens in the bucket
  int64_t bytesTokenBucket_;
  /// Controls the access across threads
  folly::SpinLock throttlerMutex_;
  /// Number of users of this throttler
  int64_t refCount_{0};
  /// The average rate expected in bytes
  double avgRateBytesPerSec_;
  /// Limit on the max number of tokens
  double bytesTokenBucketLimit_;
  /// Rate at which bucket is filled
  double bucketRateBytesPerSec_;
  /// Max number of bytes that can be requested in a single call
  int64_t singleRequestLimit_;
  /// Interval between every print of throttler logs
  int64_t throttlerLogTimeMillis_;
};
}
}  // facebook::wdt
