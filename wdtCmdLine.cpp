/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Sender.h"
#include "Receiver.h"
#include "WdtOptions.h"
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <signal.h>

// General settings Flags
DEFINE_int32(port, 22356, "Starting port number");  // W (D) T = 0x5754
DEFINE_int32(num_sockets, 8, "Number of sockets");
DEFINE_bool(skip_writes, false,
            "If true no files will be created/written (benchmark/dummy mode)");
DEFINE_bool(ipv6, true, "use ipv6 only");
DEFINE_bool(ipv4, false, "use ipv4 only, takes precedence over -ipv6");
DEFINE_bool(ignore_open_errors, false, "will continue despite open errors");
DEFINE_bool(two_phases, false, "do directory discovery first/separately");
DEFINE_bool(follow_symlinks, false,
            "If true, follow symlinks and copy them as well");
DEFINE_int32(backlog, 1, "Accept backlog");
// 256k is fastest for test on localhost and shm : > 5 Gbytes/sec
DEFINE_int32(buffer_size, 256 * 1024, "Buffer size (per thread/socket)");
DEFINE_int32(max_retries, 20, "how many attempts to connect/listen");
DEFINE_int32(sleep_ms, 50, "how many ms to wait between attempts");

// Throttler Flags
DEFINE_double(avg_mbytes_per_sec, -1,
              "Target transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited");
DEFINE_double(max_mbytes_per_sec, 0,
              "Peak transfer rate in mbytes/sec that should be maintained, "
              "specify negative for unlimited and 0 for auto configure. "
              "In auto configure mode peak rate will be 1.2 "
              "times average rate");
DEFINE_double(bucket_limit, 0,
              "Limit of burst in mbytes to control how "
              "much data you can send at unlimited speed. Unless "
              "you specify a peak rate of -1, wdt will either use "
              "your burst limit (if not 0) or max burst possible at a time "
              "will be 2 times the data allowed in "
              "1/4th seconds at peak rate");
DEFINE_int64(peak_log_time_ms, 0,
             "Peak throttler prints out logs for instantaneous "
             "rate of transfer. Specify the time interval (ms) for "
             "the measure of instance");

DEFINE_int32(progress_report_interval_ms, 20,
             "Interval(ms) between progress reports. If the value is 0, no "
             "progress reporting is done");

// Regex Flags
DEFINE_string(include_regex, "",
              "Regular expression representing files to include for transfer, "
              "empty/default is to include all files in directory. If "
              "exclude_regex is also specified, then files matching "
              "exclude_regex are excluded.");
DEFINE_string(exclude_regex, "",
              "Regular expression representing files to exclude for transfer, "
              "empty/default is to not exclude any file.");
DEFINE_string(prune_dir_regex, "",
              "Regular expression representing directories to exclude for "
              "transfer, default/empty is to recurse in all directories");

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_bool(files, false,
            "If true, read a list of files and optional "
            "filesizes from stdin relative to the directory and transfer then");
DEFINE_string(
    destination, "",
    "empty is server (destination) mode, non empty is destination host");
DEFINE_bool(full_reporting, false,
            "If true, transfer stats for successfully transferred files are "
            "included in the report");

void initOptions() {
  auto &options = facebook::wdt::WdtOptions::getMutable();
  options.avgMbytesPerSec_ = FLAGS_avg_mbytes_per_sec;
  options.maxMbytesPerSec_ = FLAGS_max_mbytes_per_sec;
  options.throttlerBucketLimit_ = FLAGS_bucket_limit;
  options.throttlerLogTimeMillis_ = FLAGS_peak_log_time_ms;

  options.includeRegex_ = FLAGS_include_regex;
  options.excludeRegex_ = FLAGS_exclude_regex;
  options.pruneDirRegex_ = FLAGS_prune_dir_regex;

  options.port_ = FLAGS_port;
  options.numSockets_ = FLAGS_num_sockets;
  options.skipWrites_ = FLAGS_skip_writes;
  options.ipv6_ = FLAGS_ipv6;
  options.ipv4_ = FLAGS_ipv4;
  options.ignoreOpenErrors_ = FLAGS_ignore_open_errors;
  options.twoPhases_ = FLAGS_two_phases;
  options.followSymlinks_ = FLAGS_follow_symlinks;
  options.fullReporting_ = FLAGS_full_reporting;
  options.progressReportIntervalMillis_ = FLAGS_progress_report_interval_ms;

  options.backlog_ = FLAGS_backlog;
  options.bufferSize_ = FLAGS_buffer_size;
  options.maxRetries_ = FLAGS_max_retries;
  options.sleepMillis_ = FLAGS_sleep_ms;
}

DECLARE_bool(logtostderr);  // default of standard glog is off - let's set it on

using namespace facebook::wdt;

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  initOptions();
  signal(SIGPIPE, SIG_IGN);
  LOG(INFO) << "Starting with directory=" << FLAGS_directory
            << " and destination=" << FLAGS_destination
            << " num sockets=" << FLAGS_num_sockets
            << " from port=" << FLAGS_port;
  ErrorCode retCode = OK;
  if (FLAGS_destination.empty()) {
    Receiver receiver(FLAGS_port, FLAGS_num_sockets, FLAGS_directory);
    // TODO fix this
    auto const &errCodes = receiver.start();
    for (const auto &errCode : errCodes) {
      if (errCode != OK) {
        retCode = errCode;
      }
    }
  } else {
    std::vector<FileInfo> fileInfo;
    if (FLAGS_files) {
      // Each line should have the filename and optionally
      // the filesize separated by a single space
      std::string line;
      while (std::getline(std::cin, line)) {
        std::vector<std::string> fields;
        folly::split('\t', line, fields, true);
        if (fields.empty() || fields.size() > 2) {
          LOG(FATAL) << "Invalid input in stdin: " << line;
        }
        int64_t filesize =
            fields.size() > 1 ? folly::to<int64_t>(fields[1]) : -1;
        fileInfo.emplace_back(fields[0], filesize);
      }
    }
    Sender sender(FLAGS_destination, FLAGS_directory);
    sender.setIncludeRegex(FLAGS_include_regex);
    sender.setExcludeRegex(FLAGS_exclude_regex);
    sender.setPruneDirRegex(FLAGS_prune_dir_regex);
    sender.setSrcFileInfo(fileInfo);
    // TODO fix that
    auto report = sender.start();
    retCode = report->getSummary().getErrorCode();
  }
  return retCode;
}
