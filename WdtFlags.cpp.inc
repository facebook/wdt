/**
 * This file is responsible for making flags.
 * When STANDALONE_APP is not defined then all the flags are
 * prefixed with wdt_.
 * The default values for the flags are picked from the corresponding
 * WdtOptions.
 * There are multiple checks which check if the flags are defined or not,
 * and undefine/redefine them. This is because this file can be
 * included multiple times for e.g. WdtFlags.cpp included this file
 * the first time through WdtFlags.h and second time in the method
 * initializeFromFlags()
 */
#ifdef GENMACRO_X
#undef GENMACRO_X
#endif
#ifdef GENMACRO
#undef GENMACRO
#endif

#ifndef OPTIONS
#define OPTIONS WdtOptions
#endif

#ifndef STANDALONE_APP
#define PREFIX(argument) wdt_##argument
#else
#define PREFIX(argument) argument
#endif
#define VALUE(A) facebook::wdt::OPTIONS::get().A

#ifdef DECLARE_ONLY
#define GENMACRO_X(type, argument, value, description) DECLARE_##type(argument);
#else
#define GENMACRO_X(type, argument, value, description) \
  DEFINE_##type(argument, VALUE(value), description);
#endif

#ifdef STANDALONE_APP
#define GENMACRO(type, argument, description) \
  GENMACRO_X(type, argument, argument, description)
#else
#define GENMACRO(type, argument, description) \
  GENMACRO_X(type, wdt_##argument, argument, description)
#endif

#ifndef WDT_OPT
#define WDT_OPT(argument, type, description) \
  GENMACRO(type, argument, description)
#endif

#define VALUE_X(argument) FLAGS_##argument
#define CAT(argument) VALUE_X(argument)
#define FLAG_VALUE(argument) CAT(PREFIX(argument))

#ifdef ASSIGN_OPT
#ifdef WDT_OPT
#undef WDT_OPT
#endif
#define WDT_OPT(argument, type, description) \
  facebook::wdt::OPTIONS::getMutable().argument = FLAG_VALUE(argument);
#endif

#ifdef PRINT_OPT
#ifdef WDT_OPT
#undef WDT_OPT
#endif
#define WDT_OPT(argument, type, description) \
  LOG(INFO) << #argument << " " << facebook::wdt::OPTIONS::get().argument;
#endif
WDT_OPT(start_port, int32, "Starting port number for wdt");
WDT_OPT(num_ports, int32, "Number of sockets");
WDT_OPT(ipv6, bool, "use ipv4 only, takes precedence over -ipv6");
WDT_OPT(ipv4, bool, "use ipv4 only, takes precedence over -ipv6");
WDT_OPT(ignore_open_errors, bool, "will continue despite open errors");
WDT_OPT(two_phases, bool, "do directory discovery first/separately");
WDT_OPT(follow_symlinks, bool,
        "If true, follow symlinks and copy them as well");
WDT_OPT(skip_writes, bool, "Skip writes on the receiver side");
WDT_OPT(backlog, int32, "Accept backlog");
WDT_OPT(buffer_size, int32, "Buffer size (per thread/socket)");
WDT_OPT(max_retries, int32, "how many attempts to connect/listen");
WDT_OPT(max_transfer_retries, int32, "Max number of retries for a source");
WDT_OPT(sleep_millis, int32, "how many ms to wait between attempts");
WDT_OPT(block_size_mbytes, double,
        "Size of the blocks that files will be divided in, specify negative "
        "to disable the file splitting mode");
WDT_OPT(avg_mbytes_per_sec, double,
        "Target transfer rate in mbytes/sec that should be "
        "maintained, specify negative for unlimited");
WDT_OPT(max_mbytes_per_sec, double,
        "Peak transfer rate in mbytes/sec that should be "
        "maintained, specify negative for unlimited and 0 for "
        "auto configure. In auto configure mode peak rate will "
        "be 1.2 times average rate");
WDT_OPT(throttler_bucket_limit, double,
        "Limit of burst in mbytes to control how "
        "much data you can send at unlimited speed. Unless "
        "you specify a peak rate of -1, wdt will either use "
        "your burst limit (if not 0) or max burst possible at a time "
        "will be 2 times the data allowed in "
        "1/4th seconds at peak rate");
WDT_OPT(throttler_log_time_millis, int64,
        "Peak throttler prints out logs for instantaneous "
        "rate of transfer. Specify the time interval (ms) for "
        "the measure of instance");
WDT_OPT(progress_report_interval_millis, int32,
        "Interval(ms) between progress reports. If the value is 0, no "
        "progress reporting is done");
WDT_OPT(full_reporting, bool,
        "If true, transfer stats for successfully transferred files "
        "are included in the report");
WDT_OPT(include_regex, string,
        "Regular expression representing files to include for "
        "transfer empty/default is to include all files in "
        "directory. If exclude_regex is also specified, then files "
        "matching exclude_regex are excluded.");
WDT_OPT(exclude_regex, string,
        "Regular expression representing files to exclude for "
        "transfer, empty/default is to not exclude any file.");
WDT_OPT(prune_dir_regex, string,
        "Regular expression representing directories to exclude for "
        "transfer, default/empty is to recurse in all directories");
WDT_OPT(accept_timeout_millis, int32,
        "accept timeout for wdt receiver in milliseconds");
WDT_OPT(max_accept_retries, int32,
        "max number of retries for accept call in receiver. First "
        "connection from sender must come before max_accept_retries * "
        "accept_timeout_ms milliseconds.");
WDT_OPT(accept_window_millis, int32,
        "accept window size in milliseconds. For a session, after the "
        "first connection is received, other connections must be "
        "received within this duration");
WDT_OPT(read_timeout_millis, int32, "socket read timeout in milliseconds");
WDT_OPT(write_timeout_millis, int32, "socket write timeout in milliseconds");
WDT_OPT(connect_timeout_millis, int32,
        "socket connect timeout in milliseconds");
WDT_OPT(abort_check_interval_millis, int32,
        "Interval in ms between checking for abort during network i/o, a "
        "negative value or 0 disables abort check");
WDT_OPT(disk_sync_interval_mb, double,
        "Disk sync interval in mb. A negative value disables syncing");
WDT_OPT(throughput_update_interval, int32,
        "Number of intervals after which"
        " progress reporter updates current throughput");
WDT_OPT(enable_checksum, bool,
        "If true, blocks are checksumed during transfer");
WDT_OPT(
    enable_perf_stat_collection, bool,
    "If true, perf stats are collected and reported at the end of transfer");
WDT_OPT(transfer_log_write_interval_ms, int32,
        "Interval in milliseconds after which transfer log is written to disk."
        " written to disk");
WDT_OPT(enable_download_resumption, bool,
        "If true, wdt supports download resumption for immutable files");
WDT_OPT(keep_transfer_log, bool,
        "If true, transfer logs are not deleted at "
        "the end of the transfer");
