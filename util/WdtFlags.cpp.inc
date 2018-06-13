/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/**
 * This file is responsible for making flags.
 * When STANDALONE_APP is not defined then all the flags are
 * prefixed with wdt_.
 * The default values for the flags are picked from the corresponding
 * WdtOptions. Flags added here should exist as options in WdtOptions.h
 * There are multiple checks which check if the flags are defined or not,
 * and undefine/redefine them. This is because this file can be
 * included multiple times for e.g. WdtFlags.cpp included this file
 * the first time through WdtFlags.h and second time in the method
 * initializeFromFlags()
 */
#include <wdt/util/WdtFlagsMacros.h>
WDT_OPT(start_port, int32, "Starting port number for wdt, if set, "
                    "implies static_ports");
WDT_OPT(num_ports, int32, "Number of sockets");
WDT_OPT(static_ports, bool, "Use static ports (start_port) or any free port");
WDT_OPT(ipv6, bool, "prefers ipv6");
WDT_OPT(ipv4, bool, "use ipv4 only, takes precedence over -ipv6");
WDT_OPT(dscp, int32, "specify DSCP flag for the sockets used in transfers");
WDT_OPT(ignore_open_errors, bool, "will continue despite open errors");
WDT_OPT(two_phases, bool, "do directory discovery first/separately");
WDT_OPT(follow_symlinks, bool,
        "If true, follow symlinks and copy them as well");
WDT_OPT(skip_writes, bool, "Skip writes on the receiver side");
WDT_OPT(backlog, int32, "Accept backlog");
WDT_OPT(buffer_size, int32, "Buffer size (per thread/socket)");
WDT_OPT(max_retries, int32, "how many attempts to connect/listen");
WDT_OPT(max_transfer_retries, int32,
        "Maximum number of times sender thread reconnects without making any "
        "progress");
WDT_OPT(sleep_millis, int32, "how many ms to wait between attempts");
WDT_OPT(block_size_mbytes, double,
        "Size of the blocks that files will be divided in, specify negative "
        "to disable the file splitting mode");
WDT_OPT(avg_mbytes_per_sec, double,
        "Target transfer rate in Mbytes/sec that should be "
        "maintained, specify negative for unlimited");
WDT_OPT(max_mbytes_per_sec, double,
        "Peak transfer rate in Mbytes/sec that should be "
        "maintained, specify negative for unlimited and 0 for "
        "auto configure. In auto configure mode peak rate will "
        "be 1.2 times average rate");
WDT_OPT(throttler_bucket_limit, double,
        "Limit of burst in Mbytes to control how "
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
        "accept_timeout_ms milliseconds. 0 or negative means infinite retries");
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
WDT_OPT(throughput_update_interval_millis, int32,
        "Intervals in millis after which progress reporter updates current"
        " throughput");
WDT_OPT(enable_checksum, bool,
        "If true, blocks are checksummed during transfer, redundant with gcm");
WDT_OPT(
    enable_perf_stat_collection, bool,
    "If true, perf stats are collected and reported at the end of transfer");
WDT_OPT(transfer_log_write_interval_ms, int32,
        "Interval in milliseconds after which transfer log is written to disk."
        " written to disk");
WDT_OPT(enable_transfer_log_compaction, bool,
        "If true, transfer log will be compacted when a transfer session"
        " finishes successfully");
WDT_OPT(enable_download_resumption, bool,
        "If true, wdt supports download resumption for append-only files");
WDT_OPT(keep_transfer_log, bool,
        "If true, transfer logs are not deleted at "
        "the end of the transfer");
WDT_OPT(
    disable_sender_verification_during_resumption, bool,
    "If true, sender-ip is not verified with the ip in transfer log. This is "
    "useful if files can be downloaded from different hosts");
WDT_OPT(global_sender_limit, int32,
        "Max number of senders allowed globally. "
        "A value of zero disables limits");
WDT_OPT(global_receiver_limit, int32,
        "Max number of receivers allowed globally. "
        "A value of zero disables limits");
WDT_OPT(namespace_sender_limit, int32,
        "Max number of senders allowed per namespace. "
        "A value of zero disables limits");
WDT_OPT(namespace_receiver_limit, int32,
        "Max number of receivers allowed per namespace. "
        "A value of zero disables limits");

#ifdef WDT_SUPPORTS_ODIRECT
WDT_OPT(odirect_reads, bool,
        "Wdt can read files in O_DIRECT mode, set this flag to true"
        " to make sender read all files in O_DIRECT");
#else
WDT_OPT(odirect_reads, bool,
        "Ignored: Wdt can't handle O_DIRECT one or more of O_DIRECT, "
        "posix_memalign, or F_NOCACHE was not found on this OS");
#endif

#ifdef HAS_POSIX_FALLOCATE
WDT_OPT(disable_preallocation, bool,
        "If true, files are not pre-allocated using posix_fallocate");
#else
WDT_OPT(disable_preallocation, bool,
        "Ignored: posix_fallocate does not exist in this system. So, files "
        "won't be pre-allocated.");
#endif
WDT_OPT(resume_using_dir_tree, bool,
        "If true, destination directory tree is trusted during resumption. So, "
        "only the remaining portion of the files are transferred. This is only "
        "supported if preallocation and block mode are disabled");
WDT_OPT(open_files_during_discovery, int32,
        "If >0 up to that many files are opened when they are discovered."
        "0 for none. -1 for trying to open all the files during discovery");
WDT_OPT(overwrite, bool, "Allow the receiver to overwrite existing files");
WDT_OPT(drain_extra_ms, int32,
        "Extra time buffer to account for network when sender waits for "
        "receiver to finish processing buffered data");
WDT_OPT(encryption_type, string,
        "Encryption type to use. WDT currently "
        "supports aes128ctr (fastest but no integrity check) and "
        "aes128gcm (recommended, default). "
        "A value of none disables encryption (fastest but insecure)");
WDT_OPT(encryption_tag_interval_bytes, int32,
        "Encryption tag verification interval in bytes. A value of zero "
        "disables incremental tag verification. In that case, tag only "
        "gets verified at the end.");
WDT_OPT(send_buffer_size, int32,
        "Send buffer size for sender sockets. If <= 0, buffer size is not set");
WDT_OPT(receive_buffer_size, int32,
        "Receive buffer size for receiver sockets. If <= 0, buffer size is not "
        "set");
WDT_OPT(
    delete_extra_files, bool,
    "If true, extra files on the receiver side is deleted during resumption");
WDT_OPT(skip_fadvise, bool, "If true, fadvise is skipped after block write");
WDT_OPT(fsync, bool,
        "If true, each file is fsync'ed after its last block is received");
WDT_OPT(enable_heart_beat, bool,
        "If true, periodic heart-beat from receiver to sender is enabled.");
WDT_OPT(iv_change_interval_mb, int32,
        "Number of MBytes after which encryption iv is changed. A value of "
        "0 disables iv change.");
