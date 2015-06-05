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
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <signal.h>
#define STANDALONE_APP
#include "WdtFlags.h"
#include "WdtFlags.cpp.inc"
DEFINE_bool(run_as_daemon, true,
            "If true, run the receiver as never ending process");

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_bool(files, false,
            "If true, read a list of files and optional "
            "filesizes from stdin relative to the directory and transfer then");
DEFINE_string(
    destination, "",
    "empty is server (destination) mode, non empty is destination host");

DEFINE_string(transfer_id, "", "Transfer id (optional, should match");
DEFINE_int32(
    protocol_version, 0,
    "Protocol version to use, this is used to simulate protocol negotiation");

DECLARE_bool(logtostderr);  // default of standard glog is off - let's set it on

using namespace facebook::wdt;
template <typename T>
std::ostream &operator<<(std::ostream &os, const std::set<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}
int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  // Ugliness in gflags' api; to be able to use program name
  google::SetArgv(argc, const_cast<const char **>(argv));
  google::SetVersionString(WDT_VERSION_STR);
  std::string usage("WDT Warp-speed Data Transfer. version ");
  usage.append(google::VersionString());
  usage.append(". Sample usage:\n\t");
  usage.append(google::ProgramInvocationShortName());
  usage.append(" # for a server/receiver\n\t");
  usage.append(google::ProgramInvocationShortName());
  usage.append(" -destination host # for a sender");
  google::SetUsageMessage(usage);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  signal(SIGPIPE, SIG_IGN);

#define STANDALONE_APP
#define ASSIGN_OPT
#include "WdtFlags.cpp.inc"  //nolint

  LOG(INFO) << "Starting with directory = " << FLAGS_directory
            << " and destination = " << FLAGS_destination
            << " num sockets = " << FLAGS_num_ports
            << " from port = " << FLAGS_start_port;
  const auto &options = WdtOptions::getMutable();
  ErrorCode retCode = OK;
  if (FLAGS_destination.empty()) {
    // TODO: inconsistent! switch to option like sender...
    Receiver receiver(FLAGS_start_port, FLAGS_num_ports, FLAGS_directory);
    receiver.setReceiverId(FLAGS_transfer_id);
    if (FLAGS_protocol_version > 0) {
      receiver.setProtocolVersion(FLAGS_protocol_version);
    }
    int numSuccess = receiver.registerPorts();
    if (numSuccess == 0) {
      LOG(ERROR) << "Couldn't bind on any port";
      return 0;
    }
    // TODO fix this
    if (!FLAGS_run_as_daemon) {
      receiver.transferAsync();
      std::unique_ptr<TransferReport> report = receiver.finish();
      retCode = report->getSummary().getErrorCode();
    } else {
      receiver.runForever();
      retCode = OK;
    }
    return retCode;
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
    std::vector<int32_t> ports;
    const auto &options = WdtOptions::get();
    for (int i = 0; i < options.num_ports; i++) {
      ports.push_back(options.start_port + i);
    }
    Sender sender(FLAGS_destination, FLAGS_directory, ports, fileInfo);
    sender.setSenderId(FLAGS_transfer_id);
    if (FLAGS_protocol_version > 0) {
      sender.setProtocolVersion(FLAGS_protocol_version);
    }
    sender.setIncludeRegex(FLAGS_include_regex);
    sender.setExcludeRegex(FLAGS_exclude_regex);
    sender.setPruneDirRegex(FLAGS_prune_dir_regex);
    // TODO fix that
    std::unique_ptr<TransferReport> report = sender.transfer();
    retCode = report->getSummary().getErrorCode();
  }
  return retCode;
}
