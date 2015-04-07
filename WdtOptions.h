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
  bool ipv6{true};
  /**
   * Use ipv4, this takes precedence over ipv6
   */
  bool ipv4{false};
  /**
   * Run wdt in a mode where the receiver doesn't
   * write anything to the disk
   */
  bool skip_writes{false};
  /**
   * Continue the operation even if there are
   * socket errors
   */
  bool ignore_open_errors{false};
  /**
   * Run the sender in two phases. First phase is
   * discovery phase and the second one is sending
   * the files over wire.
   */
  bool two_phases{false};
  /**
   * This option specifies whether wdt should
   * discover symlinks and transfer those files
   */
  bool follow_symlinks{false};

  /**
   * Starting start_port for wdt. Number of ports allocated
   * are contigous sequence starting of numSockets
   * starting from this start_port
   */
  int32_t start_port{22356};
  int32_t num_ports{8};
  /**
   * Maximum buffer size for the write on the sender
   * as well as while reading on receiver.
   */
  int32_t buffer_size{256 * 1024};
  /**
   * Maximum number of retries for the sender in case of
   * failures before exiting
   */
  int32_t max_retries{20};
  /**
   * Time in ms to sleep before retrying after each connection
   * failure
   */
  int32_t sleep_millis{50};

  /**
   * Specify the backlog to start the server socket with. Look
   * into ServerSocket.h
   */
  int32_t backlog{1};

  /**
   * Rate at which we would like data to be transferred,
   * specifying this as < 0 makes it unlimited
   */
  double avg_mbytes_per_sec{-1};
  /**
   * Rate at which tokens will be generated in TB algorithm.
   * When we lag behind, this will allow us to go faster,
   * specify as < 0 to make it unlimited. Specify as 0
   * for auto configuring.
   * auto conf as (kPeakMultiplier * avgMbytesPerSec_)
   * Find details in Sender.cpp
   */
  double max_mbytes_per_sec{0};
  /**
   * Maximum bucket size of TB algorithm in mbytes
   * This together with maxBytesPerSec_ will
   * make for maximum burst rate. If specified as 0 it is
   * auto configured in Sender.cpp
   */
  double throttler_bucket_limit{0};
  /**
   * Throttler logs statistics like average and max rate.
   * This option specifies on how frequently should those
   * be logged
   */
  int64_t throttler_log_time_millis{0};
  /**
   * Regex for the files to be included in discovery
   */
  std::string include_regex{""};
  /**
   * Regex for the files to be excluded from the discovery
   */
  std::string exclude_regex{""};
  /**
   * Regex for the directories that shouldn't be explored
   */
  std::string prune_dir_regex{""};

  /**
   * Maximum number of retries for transferring a file
   */
  int max_transfer_retries{3};

  /**
   * True if full reporting is enabled. False otherwise
   */
  bool full_reporting{false};

  /**
   * IntervalMillis(in milliseconds) between progress reports
   */
  int progress_report_interval_millis{20};

  /**
   * Timeout checks will be done at this interval (milliseconds)
   */
  int timeout_check_interval_millis{1000};

  /**
   * Number of failed timeout checks after which receiver should terminate
   */
  int failed_timeout_checks{100};

  /**
   * block size, it is used to break bigger files into smaller chunks
   * block_size of <= 0 disables block transfer
   */
  double block_size_mbytes{16};

  /**
   * timeout in accept call at the server
   */
  int32_t accept_timeout_millis{100};

  /**
   * maximum number of retries for accept call in joinable mode. we try to
   * accept with timeout of accept_timeout_millis. fisr connection from sender
   * must come before max_accept_retries.
   */
  int32_t max_accept_retries{500};

  /**
   * accept window size in millis. For a session, after the first connection is
   * received, other connections must be received before this duration.
   */
  int32_t accept_window_millis{2000};

  /**
   * timeout for socket read
   */
  int read_timeout_millis{5000};

  /**
   * timeout for socket write
   */
  int write_timeout_millis{5000};

  /**
   * timeout for socket connect
   */
  int connect_timeout_millis{2000};

  /**
   * Since this is a singelton copy constructor
   * and assignment operator are deleted
   */
  WdtOptions(const WdtOptions&) = delete;
  WdtOptions& operator=(const WdtOptions&) = delete;
  WdtOptions() {
  }

 private:
  static WdtOptions* instance_;
};
}
}
