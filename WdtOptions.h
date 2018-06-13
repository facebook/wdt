/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <unistd.h>
#include <wdt/WdtConfig.h>
#include <wdt/util/EncryptionUtils.h>
#include <cstdint>
#include <set>
#include <string>

namespace facebook {
namespace wdt {
struct ThrottlerOptions;

/**
 * A singleton class managing different options for WDT.
 * There will only be one instance of this class created
 * per creation of sender or receiver or both.
 * We can now support more than 1 instance of those per process
 * and attach a different one to sets of Sender/Receivers.
 */
class WdtOptions {
 public:
  // WDT option types
  static const char* FLASH_OPTION_TYPE;
  static const char* DISK_OPTION_TYPE;

  /**
   * A static method that can be called to create
   * the singleton copy of WdtOptions through the lifetime
   * of wdt run.
   * This is to be avoided, instead use the WdtOptions instance
   * off each object.
   * @deprecated
   */
  static const WdtOptions& get();
  /**
   * Method to get mutable copy of the singleton.
   * This is to be avoided, instead use the WdtOptions instance
   * off each object.
   * @deprecated
   */
  static WdtOptions& getMutable();

  /**
   * Modifies options based on the type specified
   *
   * @param optionType           option type
   * @param userSpecifiedOptions options specified by user, this options are not
   *                             changed
   */
  void modifyOptions(const std::string& optionType,
                     const std::set<std::string>& userSpecifiedOptions);

  /**
   * Use ipv6 while establishing connection.
   * When both ipv6 and ipv4 are false we will try both
   * if the host name has both.
   */
  bool ipv6{false};
  /**
   * Use ipv4, this takes precedence over ipv6
   */
  bool ipv4{false};

  /**
   * DSCP flag, see https://en.wikipedia.org/wiki/Differentiated_services.
   */
  int dscp{0};

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
   * are contiguous sequence starting of numSockets
   * starting from this start_port
   */
  int32_t start_port{22356};  // W (D) T = 0x5754
  int32_t num_ports{8};
  /**
   * If true, will use start_port otherwise will use 0 / dynamic ports
   */
  bool static_ports{false};
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
   * Maximum number of times sender thread reconnects without making any
   * progress
   */
  int max_transfer_retries{3};

  /**
   * True if full reporting is enabled. False otherwise
   */
  bool full_reporting{false};

  /**
   * IntervalMillis(in milliseconds) between progress reports
   */
  int progress_report_interval_millis{
      isatty(STDOUT_FILENO) ? 20 : 200};  // default value is much higher for
                                          // logging case, this is done to avoid
                                          // flooding the log with progress
                                          // information

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
   * accept with timeout of accept_timeout_millis. first connection from sender
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
  int32_t connect_timeout_millis{1000};

  /**
   * interval in ms between abort checks
   */
  int abort_check_interval_millis{200};

  /**
   * Disk sync interval in mb. A negative value disables syncing
   */
  double disk_sync_interval_mb{0.5};

  /**
   * If true, each file is fsync'ed after its last block is
   * received.
   * Note that this can cause some performance hit on some filesystems, however
   * if you disable it there no correctness guarantee will be provided.
   */
  bool fsync{true};

  /**
   * Intervals in millis after which progress reporter updates current
   * throughput
   */
  int throughput_update_interval_millis{500};

  /**
   * Flag for turning on/off checksum. Redundant gcm in ENC_AES128_GCM.
   */
  bool enable_checksum{false};

  /**
   * If true, perf stats are collected and reported at the end of transfer
   */
  bool enable_perf_stat_collection{false};

  /**
   * Interval in milliseconds after which transfer log is written to disk
   */
  int transfer_log_write_interval_ms{100};

  /**
   * If true, compact transfer log if transfer finishes successfully
   */
  bool enable_transfer_log_compaction{false};

  /**
   * If true, download resumption is enabled
   */
  bool enable_download_resumption{false};

  /**
   * At transfer end, do not delete transfer log
   */
  bool keep_transfer_log{true};

  /**
   * If true, WDT does not verify sender ip during resumption
   */
  bool disable_sender_verification_during_resumption{false};

  /**
   * Max number of senders allowed globally
   */
  int global_sender_limit{0};

  /**
   * Max number of receivers allowed globally
   */
  int global_receiver_limit{0};

  /**
   * Max number of senders allowed per namespace
   */
  int namespace_sender_limit{0};

  /**
   * Max number of receivers allowed per namespace
   */
  int namespace_receiver_limit{1};

  /**
   * Read files in O_DIRECT
   */
  bool odirect_reads{false};

  /**
   * If true, files are not pre-allocated using posix_fallocate.
   * This flag should not be used directly by wdt code. It should be accessed
   * through shouldPreallocateFiles method.
   */
  bool disable_preallocation{false};

  /**
   * If true, destination directory tree is trusted during resumption. So, only
   * the remaining portion of the files are transferred. This is only supported
   * if preallocation and block mode is disabled.
   */
  bool resume_using_dir_tree{false};

  /**
   * If > 0 will open up to that number of files during discovery
   * if 0 will not open any file during discovery
   * if < 0 will try to open all the files during discovery (which may fail
   * with too many open files errors)
   */
  int open_files_during_discovery{0};

  /**
   * If true, wdt can overwrite existing files
   */
  bool overwrite{false};

  /**
   * Extra time buffer to account for network when sender waits for receiver to
   * finish processing buffered data
   */
  int drain_extra_ms{500};

  /**
   * Encryption type to use
   */
  std::string encryption_type{encryptionTypeToStr(ENC_AES128_GCM)};

  /**
   * Encryption tag verification interval in bytes. A value of zero disables
   * incremental tag verification. In that case, tag only gets verified at the
   * end.
   */
  int encryption_tag_interval_bytes{4 * 1024 * 1024};

  /**
   * send buffer size for Sender. If < = 0, buffer size is not set
   */
  int send_buffer_size{0};

  /**
   * receive buffer size for Receiver. If < = 0, buffer size is not set
   */
  int receive_buffer_size{0};

  /**
   * If true, extra files on the receiver side is deleted during resumption
   */
  bool delete_extra_files{false};

  /**
   * If true, fadvise is skipped after block write
   */
  bool skip_fadvise{false};

  /**
   * If true, periodic heart-beats from receiver to sender is enabled.
   * The heart-beat interval is determined by the socket read timeout of the
   * sender.
   * WDT senders streams data and only waits for a receiver response after
   * all the blocks are sent. Because of the high socket buffer sizes, it might
   * take a long time for receiver to process all the bytes sent. So, for slower
   * threads, there is significant difference between the time receiver
   * processes all the bytes and the time sender finishes sending all the bytes.
   * So, the sender might time out while waiting for the response from receiver.
   * This happens a lot more for disks because of the lower io throughput.
   * To solve this, receiver sends heart-beats to signal that it is sill
   * processing data, and sender waits will it is still getting heart-beats.
   */
  bool enable_heart_beat{true};

  /**
   * Number of MBytes after which encryption iv is changed. A value of 0
   * disables iv change.
   */
  int iv_change_interval_mb{32 * 1024};

  /**
   * @return    whether files should be pre-allocated or not
   */
  bool shouldPreallocateFiles() const;

  /**
   * @return    whether transfer log based resumption is enabled
   */
  bool isLogBasedResumption() const;

  /**
   * @return    whether directory tree based resumption is enabled
   */
  bool isDirectoryTreeBasedResumption() const;

  /**
   * @return    throttler options
   */
  ThrottlerOptions getThrottlerOptions() const;

  // NOTE: any option added here should also be added to util/WdtFlags.cpp.inc

  /**
   * Initialize the fields of this object from another src one. ie makes 1 copy
   * explicitly.
   */
  void copyInto(const WdtOptions& src);
  /**
   * This used to be a singleton (which as always is a pretty bad idea)
   * so copy constructor and assignment operator were deleted
   * We still want to avoid accidental copying around of a fairly
   * big object thus the copyInto pattern above
   */
  WdtOptions(const WdtOptions&) = delete;
  WdtOptions() {
  }
  ~WdtOptions() {
  }

 private:
  WdtOptions& operator=(const WdtOptions&) = default;
};
}
}
