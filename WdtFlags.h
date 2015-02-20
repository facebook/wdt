#pragma once
#include <gflags/gflags.h>
#include <iostream>
#include "WdtOptions.h"
DECLARE_int32(wdt_start_port);
DECLARE_int32(wdt_num_ports);
DECLARE_bool(wdt_ipv6);
DECLARE_bool(wdt_ipv4);
DECLARE_bool(wdt_ignore_open_errors);
DECLARE_bool(wdt_two_phases);
DECLARE_bool(wdt_follow_symlinks);
DECLARE_bool(wdt_skip_writes);
DECLARE_int32(wdt_backlog);
DECLARE_int32(wdt_buffer_size);
DECLARE_int32(wdt_max_retries);
DECLARE_int32(wdt_max_transfer_retries);
DECLARE_int32(wdt_sleep_ms);
DECLARE_double(wdt_retry_mult_factor);

// Throttler Flags
DECLARE_double(wdt_avg_mbytes_per_sec);
DECLARE_double(wdt_max_mbytes_per_sec);
DECLARE_double(wdt_throttler_bucket_limit);
DECLARE_int64(wdt_throttler_log_time_ms);
// WDT reporting options
DECLARE_int32(wdt_progress_report_interval_ms);

DECLARE_int32(wdt_timeout_check_interval_ms);
DECLARE_int32(wdt_failed_timeout_checks);

DECLARE_bool(wdt_full_reporting);
// Regex Flags
DECLARE_string(wdt_include_regex);
DECLARE_string(wdt_exclude_regex);
DECLARE_string(wdt_prune_dir_regex);
namespace facebook {
namespace wdt {
class WdtFlags {
 public:
  /**
   * Set the values of options in WdtOptions from corresponding flags
   */
  static void initializeFromFlags();
  static void toString();
};
}
}
