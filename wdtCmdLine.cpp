/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Sender.h"
#include "Receiver.h"
#include "WdtResourceController.h"
#include <chrono>
#include <future>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <signal.h>
#include <thread>

#ifndef FLAGS_INCLUDE_FILE
#define FLAGS_INCLUDE_FILE "WdtFlags.cpp.inc"
#endif

#ifndef ADDITIONAL_SENDER_SETUP
#define ADDITIONAL_SENDER_SETUP
#endif

#define STANDALONE_APP
#include FLAGS_INCLUDE_FILE

// Flags not already in WdtOptions.h/WdtFlags.cpp.inc
DEFINE_bool(run_as_daemon, false,
            "If true, run the receiver as never ending process");

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_bool(files, false,
            "If true, read a list of files and optional "
            "filesizes from stdin relative to the directory and transfer then");
DEFINE_string(
    destination, "",
    "empty is server (destination) mode, non empty is destination host");
DEFINE_bool(parse_transfer_log, false,
            "If true, transfer log is parsed and fixed");

DEFINE_string(transfer_id, "", "Transfer id (optional, should match");
DEFINE_int32(
    protocol_version, 0,
    "Protocol version to use, this is used to simulate protocol negotiation");

DEFINE_string(connection_url, "",
              "Provide the connection string to connect to receiver");

DECLARE_bool(logtostderr);  // default of standard glog is off - let's set it on

DEFINE_int32(abort_after_seconds, 0,
             "Abort transfer after given seconds. 0 means don't abort.");

using namespace facebook::wdt;
template <typename T>
std::ostream &operator<<(std::ostream &os, const std::set<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}

std::mutex abortMutex;
std::condition_variable abortCondVar;

void setUpAbort(WdtBase &senderOrReceiver) {
  int abortSeconds = FLAGS_abort_after_seconds;
  LOG(INFO) << "Setting up abort " << abortSeconds << " seconds.";
  if (abortSeconds <= 0) {
    return;
  }
  static std::atomic<bool> abortTrigger{false};
  static WdtAbortChecker chkr(abortTrigger);
  senderOrReceiver.setAbortChecker(&chkr);
  auto lambda = [=] {
    LOG(INFO) << "Will abort in " << abortSeconds << " seconds.";
    std::unique_lock<std::mutex> lk(abortMutex);
    if (abortCondVar.wait_for(lk, std::chrono::seconds(abortSeconds)) ==
        std::cv_status::no_timeout) {
      LOG(INFO) << "Already finished normally, no abort.";
    } else {
      LOG(INFO) << "Requesting abort.";
      abortTrigger.store(true);
    }
  };
  // we want to run in bg, not block
  static std::future<void> abortThread = std::async(std::launch::async, lambda);
}

void cancelAbort() {
  std::unique_lock<std::mutex> lk(abortMutex);
  abortCondVar.notify_one();
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  // Ugliness in gflags' api; to be able to use program name
  google::SetArgv(argc, const_cast<const char **>(argv));
  google::SetVersionString(Protocol::getFullVersion());
  std::string usage("WDT Warp-speed Data Transfer. v ");
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
#include FLAGS_INCLUDE_FILE  //nolint

  LOG(INFO) << "Running WDT " << Protocol::getFullVersion();
  ErrorCode retCode = OK;
  if (FLAGS_parse_transfer_log) {
    // Log parsing mode
    TransferLogManager transferLogManager;
    transferLogManager.setRootDir(FLAGS_directory);
    if (!transferLogManager.parseAndPrint()) {
      LOG(ERROR) << "Transfer log parsing failed";
      retCode = ERROR;
    }
  } else if (FLAGS_destination.empty() && FLAGS_connection_url.empty()) {
    Receiver receiver(FLAGS_start_port, FLAGS_num_ports, FLAGS_directory);
    receiver.setTransferId(FLAGS_transfer_id);
    if (FLAGS_protocol_version > 0) {
      receiver.setProtocolVersion(FLAGS_protocol_version);
    }
    WdtTransferRequest transferRequest = receiver.init();
    if (transferRequest.errorCode == ERROR) {
      LOG(ERROR) << "Error setting up receiver";
      return transferRequest.errorCode;
    }
    LOG(INFO) << "Starting receiver with connection url ";
    std::cout << transferRequest.generateUrl() << std::endl;
    std::cout.flush();
    setUpAbort(receiver);
    if (!FLAGS_run_as_daemon) {
      receiver.transferAsync();
      std::unique_ptr<TransferReport> report = receiver.finish();
      retCode = report->getSummary().getErrorCode();
    } else {
      retCode = receiver.runForever();
      // not reached
    }
  } else {
    // Sender mode
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
    std::unique_ptr<Sender> sender;
    if (FLAGS_connection_url.empty()) {
      sender.reset(
          new Sender(FLAGS_destination, FLAGS_directory, ports, fileInfo));
      if (FLAGS_protocol_version > 0) {
        sender->setProtocolVersion(FLAGS_protocol_version);
      }
      sender->setTransferId(FLAGS_transfer_id);
    } else {
      // If you are using a connection url it is
      // expected that you set protocol version, ports
      // and transfer id in the url
      WdtTransferRequest transferRequest(FLAGS_connection_url);
      LOG(INFO) << transferRequest.generateUrl(true);
      if (transferRequest.directory.empty()) {
        transferRequest.directory = FLAGS_directory;
      }
      sender.reset(new Sender(transferRequest));
    }
    WdtTransferRequest processedRequest = sender->init();
    LOG(INFO) << "Starting sender with details "
              << processedRequest.generateUrl(true);
    ADDITIONAL_SENDER_SETUP
    setUpAbort(*sender);
    sender->setIncludeRegex(FLAGS_include_regex);
    sender->setExcludeRegex(FLAGS_exclude_regex);
    sender->setPruneDirRegex(FLAGS_prune_dir_regex);
    // TODO fix that
    std::unique_ptr<TransferReport> report = sender->transfer();
    retCode = report->getSummary().getErrorCode();
  }
  cancelAbort();
  return retCode;
}
