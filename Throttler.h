#pragma once
#include <folly/Synchronized.h>
#include <thread>
#include <glog/logging.h>
namespace facebook {
namespace wdt {
typedef std::chrono::high_resolution_clock Clock;
/**
 * Attempts to limit the rate in two ways.
 * 1. Limit average rate by calling averageThrottler()
 * 2. Limit the peak rate by calling limitByTokenBucket
 * Generally average throttler would be mainting the rate to avgRate_
 * although at times the actual rate might fall behind and in those
 * circumstances the rate at which you will catch up is limited
 * with respect to the peak rate and the bucket limit using the
 * token bucket algorithm.
 * Note: This throttler has to be used per thread.
 * Token Bucket algorithm can be found on
 * http://en.wikipedia.org/wiki/Token_bucket
 */
class Throttler {
 public:
  /**
   * @param averageRateBytesPerSec    Average rate in progress/second
   *                                  at which data should be transmitted
   *
   * @param peakRateBytesPerSec       Max burst rate allowed by the
   *                                  token bucket
   * @param bucketLimitBytes          Max size of bucket, specify 0 for auto
   *                                  configure. In auto mode, it will be twice
   *                                  the data you send in 1/4th of a second
   *                                  at the peak rate
   */
  Throttler(Clock::time_point start, double avgRateBytesPerSec,
            double peakRateBytesPerSec, double bucketLimitBytes,
            double throttlerLogTimeMillis = 0);

  /**
   * This method is implementation of token bucket algorithm.
   * Bucket is filled at the rate of bucketRateBytesPerSec_
   * till the limit of bytesTokenBucketLimit_
   * This method also calls the averageThrottler inside
   * @param bytesTotalProgress         Total Progress till now in bytes
   */
  virtual void limit(double bytesTotalProgress);

  virtual ~Throttler() {
  }

 private:
  /**
   * This method is invoked repeatedly with the amount of progress made
   * (e.g. number of bytes written) till now. If the total progress
   * till now is over the allowed average progress then this method causes
   * thread to sleep and returns that time in seconds
   * @param now                       Pass in the current time stamp
   */
  double averageThrottler(const Clock::time_point &now);

  /**
   * This method periodically prints logs.
   * The period is defined by FLAGS_peak_log_time_ms
   * @params deltaProgress      Progress since last call to limit()
   * @params now                The time point caller has
   * @params sleepTimeSeconds   Duration of sleep caused by limit()
   */
  void printPeriodicLogs(double deltaProgress, const Clock::time_point &now,
                         double sleepTimeSeconds);
  /// Records the time the throttler was started
  Clock::time_point startTime_;

  /**
   * Throttler logs the average and instantaneous progress
   * periodically (check FLAGS_peak_log_time_ms). lastLogTime_ is
   * the last time this log was written
   */
  std::chrono::time_point<std::chrono::high_resolution_clock> lastLogTime_;
  double instantProgress_;
  /**
   * Records the total progress in bytes till now
   */
  double bytesProgress_;
  /// The average rate expected in bytes
  double avgRateBytesPerSec_;
  /// Last time the token bucket was filled
  std::chrono::time_point<std::chrono::high_resolution_clock> lastFillTime_;
  /// Number of tokens in the bucket
  int64_t bytesTokenBucket_;
  /// Limit on the max number of tokens
  int64_t bytesTokenBucketLimit_;
  /// Rate at which bucket is filled
  double bucketRateBytesPerSec_;
  /// Interval between every print of throttler logs
  double throttlerLogTimeMillis_;
  bool isTokenBucketEnabled;
};
}
}  // facebook::wdt
