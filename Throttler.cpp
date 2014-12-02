#include "Throttler.h"
const int64_t kMillisecsPerSec = 1000;
const double kMbToB = 1024 * 1024;
DEFINE_int64(peak_log_time_ms, 0,
             "Peak throttler prints out logs for instantaneous "
             "rate of transfer. Specify the time interval (ms) for "
             "the measure of instance");
namespace facebook {
namespace wdt {
Throttler::Throttler(Clock::time_point start, double avgRateBytesPerSec,
                     double peakRateBytesPerSec, double bucketLimitBytes)
    : startTime_(start),
      bytesProgress_(0),
      avgRateBytesPerSec_(avgRateBytesPerSec) {
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
    LOG(INFO) << "Average rate " << avgRateBytesPerSec_ / kMbToB;
  } else {
    LOG(INFO) << "No average rate specified";
  }
  if (bucketRateBytesPerSec_ > 0) {
    LOG(INFO) << "Peak rate " << bucketRateBytesPerSec_ / kMbToB
              << " mbytes / seconds.  Bucket limit "
              << bytesTokenBucketLimit_ / kMbToB << " mbytes.";
  } else {
    LOG(INFO) << "No peak rate specified";
  }
  instantProgress_ = 0;
  lastFillTime_ = start;
  isTokenBucketEnabled = (bucketRateBytesPerSec_ > 0);
}
void Throttler::limit(double bytesTotalProgress) {
  double deltaProgress = bytesTotalProgress - bytesProgress_;
  bytesProgress_ = bytesTotalProgress;
  std::chrono::time_point<Clock> now = Clock::now();
  double avgThrottlerSleep = averageThrottler(now);
  const bool hasSlept = (avgThrottlerSleep > 0);
  double sleepTimeSeconds = 0;
  if (isTokenBucketEnabled && !hasSlept) {
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
      sleepTimeSeconds = -1.0 * bytesTokenBucket_ / bucketRateBytesPerSec_;
      VLOG(1) << "Peak throttler wants to sleep " << sleepTimeSeconds
              << " seconds";
      std::this_thread::sleep_for(
          std::chrono::duration<double>(sleepTimeSeconds));
    }
  }
  sleepTimeSeconds += avgThrottlerSleep;
  if (FLAGS_peak_log_time_ms > 0) {
    printPeriodicLogs(deltaProgress, now, sleepTimeSeconds);
  }
}

void Throttler::printPeriodicLogs(double deltaProgress,
                                  const Clock::time_point &now,
                                  double sleepTimeSeconds) {
  instantProgress_ += deltaProgress;
  /*
  * This is the part where throttler prints out the progress
  * made periodically.
  */
  std::chrono::duration<double> elapsedLogDuration = now - lastLogTime_;
  double elapsedLogSeconds = elapsedLogDuration.count() + sleepTimeSeconds;
  if (elapsedLogSeconds * kMillisecsPerSec >= FLAGS_peak_log_time_ms) {
    std::chrono::duration<double> elapsedAvgDuration = now - startTime_;
    double elapsedAvgSeconds = elapsedAvgDuration.count() + sleepTimeSeconds;
    double instantBytesPerSec = instantProgress_ / (double)elapsedLogSeconds;
    double avgBytesPerSec = (bytesProgress_) / elapsedAvgSeconds;
    LOG(INFO) << "Throttler:Transfer_Rates::"
              << " " << elapsedAvgSeconds << " " << avgBytesPerSec / kMbToB
              << " " << instantBytesPerSec / kMbToB;
    lastLogTime_ = now;
    instantProgress_ = 0;
  }
}
double Throttler::averageThrottler(const Clock::time_point &now) {
  std::chrono::duration<double> elapsedDuration = now - startTime_;
  double elapsedSeconds = elapsedDuration.count();
  if (avgRateBytesPerSec_ <= 0) {
    VLOG(1) << "There is no rate limit";
    return 0;
  }
  const double allowedProgress = avgRateBytesPerSec_ * elapsedSeconds;
  if (bytesProgress_ > allowedProgress) {
    double idealTime = bytesProgress_ / avgRateBytesPerSec_;
    const double sleepTimeSeconds = idealTime - elapsedSeconds;
    VLOG(1) << "Throttler : Elapsed " << elapsedSeconds
            << " seconds. Made progress " << bytesProgress_ / kMbToB
            << " mbytes in " << elapsedSeconds
            << " seconds, maximum allowed progress for this duration is "
            << allowedProgress / kMbToB << " mbytes. Mean Rate allowed is "
            << avgRateBytesPerSec_ / kMbToB
            << " mbytes per seconds. Sleeping for " << sleepTimeSeconds
            << " seconds";
    std::this_thread::sleep_for(
        std::chrono::duration<double>(sleepTimeSeconds));
    return sleepTimeSeconds;
  }
  return 0;
}
}
}  // namespace facebook::wormhole
