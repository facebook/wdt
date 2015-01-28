#pragma once
#include <folly/String.h>
#include <memory>
#include <thread>
#include <mutex>
namespace facebook {
namespace wdt {
/**
 * A singleton class managing different options for WDT.
 * There will only be one instance of this class created
 * per creation of sender or receiver or both.
 */
class WdtOptions {
 public:
  /**
   * A static method that can be called to create
   * the singleton copy of WdtOptions through the lifetime
   * of wdt run.
   */
  static const WdtOptions& get();
  /**
   * Method to get mutable copy of the singleton
   */
  static WdtOptions& getMutable();
  /**
   * Use ipv6 while establishing connection
   */
  bool ipv6_;
  /**
   * Use ipv4, this takes precedence over ipv6
   */
  bool ipv4_;
  /**
   * Run wdt in a mode where the receiver doesn't
   * write anything to the disk
   */
  bool skipWrites_;
  /**
   * Continue the operation even if there are
   * socket errors
   */
  bool ignoreOpenErrors_;
  /**
   * Run the sender in two phases. First phase is
   * discovery phase and the second one is sending
   * the files over wire.
   */
  bool twoPhases_;
  /**
   * This option specifies whether wdt should
   * discover symlinks and transfer those files
   */
  bool followSymlinks_;

  /**
   * Starting port for wdt. Number of ports allocated
   * are contigous sequence starting of numSockets
   * starting from this port
   */
  int32_t port_;
  int32_t numSockets_;
  /**
   * Maximum buffer size for the write on the sender
   * as well as while reading on receiver.
   */
  int32_t bufferSize_;
  /**
   * Maximum number of retries for the sender in case of
   * failures before exiting
   */
  int32_t maxRetries_;
  /**
   * Time in ms to sleep before retrying after each connection
   * failure
   */
  int32_t sleepMillis_;
  /**
   * Specify the backlog to start the server socket with. Look
   * into ServerSocket.h
   */
  int32_t backlog_;

  /**
   * Rate at which we would like data to be transferred,
   * specifying this as < 0 makes it unlimited
   */
  double avgMbytesPerSec_;
  /**
   * Rate at which tokens will be generated in TB algorithm.
   * When we lag behind, this will allow us to go faster,
   * specify as < 0 to make it unlimited. Specify as 0
   * for auto configuring.
   * auto conf as (kPeakMultiplier * avgMbytesPerSec_)
   * Find details in Sender.cpp
   */
  double maxMbytesPerSec_;
  /**
   * Maximum bucket size of TB algorithm in mbytes
   * This together with maxBytesPerSec_ will
   * make for maximum burst rate. If specified as 0 it is
   * auto configured in Sender.cpp
   */
  double throttlerBucketLimit_;
  /**
   * Throttler logs statistics like average and max rate.
   * This option specifies on how frequently should those
   * be logged
   */
  int64_t throttlerLogTimeMillis_;
  /**
   * Regex for the files to be included in discovery
   */
  std::string includeRegex_;
  /**
   * Regex for the files to be excluded from the discovery
   */
  std::string excludeRegex_;
  /**
   * Regex for the directories that shouldn't be explored
   */
  std::string pruneDirRegex_;

  /**
   * Maximu number of retries for transferring a file
   */
  int maxTransferRetries_;

  /**
   * True if full reporting is enabled. False otherwise
   */
  bool fullReporting_;

  /**
   * IntervalMillis(in milliseconds) between progress reports
   */
  int progressReportIntervalMillis_;

  /// multiplication factor for retry interval
  double retryIntervalMultFactor_;

  /**
   * Timeout checks will be done at this interval (milliseconds)
   */
  int timeoutCheckIntervalMillis_;

  /**
   * Number of failed timeout checks after which receiver should terminate
   */
  int failedTimeoutChecks_;

  /**
   * Since this is a singelton copy constructor
   * and assignment operator are deleted
   */
  WdtOptions(const WdtOptions&) = delete;
  WdtOptions& operator=(const WdtOptions&) = delete;

 private:
  static WdtOptions* instance_;
  WdtOptions() {
    ipv6_ = true;
    ipv4_ = false;
    skipWrites_ = false;
    ignoreOpenErrors_ = false;
    twoPhases_ = false;
    followSymlinks_ = false;

    port_ = 22356;
    numSockets_ = 8;
    bufferSize_ = 256 * 1024;
    maxRetries_ = 20;
    sleepMillis_ = 50;
    backlog_ = 1;
    maxTransferRetries_ = 3;
    fullReporting_ = false;
    progressReportIntervalMillis_ = 0;

    retryIntervalMultFactor_ = 1.0;

    throttlerLogTimeMillis_ = 0;
    avgMbytesPerSec_ = -1;
    maxMbytesPerSec_ = 0;
    throttlerBucketLimit_ = 0;

    failedTimeoutChecks_ = 100;
    timeoutCheckIntervalMillis_ = 1000;

    includeRegex_ = "";
    excludeRegex_ = "";
    pruneDirRegex_ = "";
  }
};
}
}
