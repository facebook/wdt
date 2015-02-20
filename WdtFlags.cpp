#include "WdtFlags.h"
DEFINE_int32(wdt_start_port, 22356, "Starting port number for wdt");
DEFINE_int32(wdt_num_ports, 8, "Number of sockets");
DEFINE_bool(wdt_ipv6, true, "use ipv6 only");
DEFINE_bool(wdt_ipv4, false, "use ipv4 only, takes precedence over -ipv6");
DEFINE_bool(wdt_ignore_open_errors, false, "will continue despite open errors");
DEFINE_bool(wdt_two_phases, false, "do directory discovery first/separately");
DEFINE_bool(wdt_follow_symlinks, false,
            "If true, follow symlinks and copy them as well");
DEFINE_bool(wdt_skip_writes, false, "Skip writes on the receiver side");
DEFINE_int32(wdt_backlog, 1, "Accept backlog");
// 256k is fastest for test on localhost and shm : > 5 Gbytes/sec
DEFINE_int32(wdt_buffer_size, 256 * 1024, "Buffer size (per thread/socket)");
DEFINE_int32(wdt_max_retries, 20, "how many attempts to connect/listen");
DEFINE_int32(wdt_max_transfer_retries, 3, "Max number of retries for a source");
DEFINE_int32(wdt_sleep_ms, 50, "how many ms to wait between attempts");
DEFINE_double(wdt_retry_mult_factor, 1.0,
              "Factor to multiply with the "
              "retry interval for connnecting");

// Throttler Flags
DEFINE_double(wdt_avg_mbytes_per_sec, -1,
              "Target transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited");
DEFINE_double(wdt_max_mbytes_per_sec, 0,
              "Peak transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited and 0 for auto configure. "
              "In auto configure mode peak rate will be 1.2 "
              "times average rate");
DEFINE_double(wdt_throttler_bucket_limit, 0,
              "Limit of burst in mbytes to control how "
              "much data you can send at unlimited speed. Unless "
              "you specify a peak rate of -1, wdt will either use "
              "your burst limit (if not 0) or max burst possible at a time "
              "will be 2 times the data allowed in "
              "1/4th seconds at peak rate");
DEFINE_int64(wdt_throttler_log_time_ms, 0,
             "Peak throttler prints out logs for instantaneous "
             "rate of transfer. Specify the time interval (ms) for "
             "the measure of instance");

// WDT reporting options
DEFINE_int32(wdt_progress_report_interval_ms, 0,
             "Interval(ms) between progress reports. If the value is 0, no "
             "progress reporting is done");

DEFINE_int32(wdt_timeout_check_interval_ms, 0,
             "Interval(ms) between timeout checks");

DEFINE_int32(wdt_failed_timeout_checks, 200,
             "Number of failed timeout checks after which receiver "
             "shall terminate");

DEFINE_bool(wdt_full_reporting, false,
            "If true, transfer stats for successfully transferred files are "
            "included in the report");

// Regex Flags
DEFINE_string(wdt_include_regex, "",
              "Regular expression representing files to include for transfer, "
              "empty/default is to include all files in directory. If "
              "exclude_regex is also specified, then files matching "
              "exclude_regex are excluded.");
DEFINE_string(wdt_exclude_regex, "",
              "Regular expression representing files to exclude for transfer, "
              "empty/default is to not exclude any file.");
DEFINE_string(wdt_prune_dir_regex, "",
              "Regular expression representing directories to exclude for "
              "transfer, default/empty is to recurse in all directories");
namespace facebook {
namespace wdt {
void WdtFlags::initializeFromFlags() {
  auto &options = WdtOptions::getMutable();
  options.port_ = FLAGS_wdt_start_port;
  options.numSockets_ = FLAGS_wdt_num_ports;
  options.ipv6_ = FLAGS_wdt_ipv6;
  options.ipv4_ = FLAGS_wdt_ipv4;
  options.ignoreOpenErrors_ = FLAGS_wdt_ignore_open_errors;
  options.twoPhases_ = FLAGS_wdt_two_phases;
  options.backlog_ = FLAGS_wdt_backlog;
  options.bufferSize_ = FLAGS_wdt_buffer_size;
  options.maxRetries_ = FLAGS_wdt_max_retries;
  options.maxTransferRetries_ = FLAGS_wdt_max_transfer_retries;
  options.sleepMillis_ = FLAGS_wdt_sleep_ms;
  options.followSymlinks_ = FLAGS_wdt_follow_symlinks;
  options.skipWrites_ = FLAGS_wdt_skip_writes;
  options.retryIntervalMultFactor_ = FLAGS_wdt_retry_mult_factor;

  // Throttler options
  options.avgMbytesPerSec_ = FLAGS_wdt_avg_mbytes_per_sec;
  options.maxMbytesPerSec_ = FLAGS_wdt_max_mbytes_per_sec;
  options.throttlerBucketLimit_ = FLAGS_wdt_throttler_bucket_limit;
  options.throttlerLogTimeMillis_ = FLAGS_wdt_throttler_log_time_ms;

  // Reporting options
  options.timeoutCheckIntervalMillis_ = FLAGS_wdt_timeout_check_interval_ms;
  options.failedTimeoutChecks_ = FLAGS_wdt_failed_timeout_checks;
  options.fullReporting_ = FLAGS_wdt_full_reporting;
  options.progressReportIntervalMillis_ = FLAGS_wdt_progress_report_interval_ms;

  // Regex options
  options.includeRegex_ = FLAGS_wdt_include_regex;
  options.excludeRegex_ = FLAGS_wdt_exclude_regex;
  options.pruneDirRegex_ = FLAGS_wdt_prune_dir_regex;
}
}
}
