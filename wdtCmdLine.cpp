/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Wdt.h>
#include <wdt/Receiver.h>
#include <wdt/WdtResourceController.h>

#include <chrono>
#include <future>
#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>
#include <fstream>
#include <signal.h>
#include <thread>

// Used in fbonly to add socket creator setup
#ifndef ADDITIONAL_SENDER_SETUP
#define ADDITIONAL_SENDER_SETUP
#endif

// This can be the fbonly (FbWdt) version (extended initialization, and options)
#ifndef WDTCLASS
#define WDTCLASS Wdt
#endif

// Flags not already in WdtOptions.h/WdtFlags.cpp.inc
DEFINE_bool(run_as_daemon, false,
            "If true, run the receiver as never ending process");

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_string(manifest, "",
              "If specified, then we will read a list of files and optional "
              "sizes from this file, use - for stdin");
DEFINE_string(
    destination, "",
    "empty is server (destination) mode, non empty is destination host");
DEFINE_bool(parse_transfer_log, false,
            "If true, transfer log is parsed and fixed");

DEFINE_string(transfer_id, "",
              "Transfer id. Receiver will generate one to be used (via URL) on"
              " the sender if not set explicitly");
DEFINE_int32(
    protocol_version, 0,
    "Protocol version to use, this is used to simulate protocol negotiation");

DEFINE_string(connection_url, "",
              "Provide the connection string to connect to receiver"
              " (incl. transfer_id and other parameters)");

DECLARE_bool(logtostderr);  // default of standard glog is off - let's set it on

DEFINE_int32(abort_after_seconds, 0,
             "Abort transfer after given seconds. 0 means don't abort.");

DEFINE_string(recovery_id, "", "Recovery-id to use for download resumption");

DEFINE_bool(treat_fewer_port_as_error, false,
            "If the receiver is unable to bind to all the ports, treat that as "
            "an error.");
DEFINE_bool(print_options, false,
            "If true, wdt prints the option values and exits. Option values "
            "printed take into account option type and other command line "
            "flags specified.");
DEFINE_bool(exit_on_bad_flags, true,
            "If true, wdt exits on bad/unknown flag. Otherwise, an unknown "
            "flags are ignored");
DEFINE_string(test_only_encryption_secret, "",
              "Test only encryption secret, to test url encoding/decoding");

DEFINE_string(app_name, "wdt", "Identifier used for reporting (scuba, at fb)");

using namespace facebook::wdt;

// TODO: move this to some util and/or delete
template <typename T>
std::ostream &operator<<(std::ostream &os, const std::set<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}

std::mutex abortMutex;
std::condition_variable abortCondVar;

std::shared_ptr<WdtAbortChecker> setupAbortChecker() {
  int abortSeconds = FLAGS_abort_after_seconds;
  if (abortSeconds <= 0) {
    return nullptr;
  }
  LOG(INFO) << "Setting up abort " << abortSeconds << " seconds.";
  static std::atomic<bool> abortTrigger{false};
  auto res = std::make_shared<WdtAbortChecker>(abortTrigger);
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
  return res;
}

void setAbortChecker(WdtBase &senderOrReceiver) {
  senderOrReceiver.setAbortChecker(setupAbortChecker());
}

void cancelAbort() {
  {
    std::unique_lock<std::mutex> lk(abortMutex);
    abortCondVar.notify_one();
  }
  std::this_thread::yield();
}

void readManifest(std::istream &fin, WdtTransferRequest &req) {
  std::string line;
  while (std::getline(fin, line)) {
    std::vector<std::string> fields;
    folly::split('\t', line, fields, true);
    if (fields.empty() || fields.size() > 2) {
      LOG(FATAL) << "Invalid input manifest: " << line;
    }
    int64_t filesize = fields.size() > 1 ? folly::to<int64_t>(fields[1]) : -1;
    req.fileInfo.emplace_back(fields[0], filesize);
  }
}

namespace google {
extern GFLAGS_DLL_DECL void (*gflags_exitfunc)(int);
}

bool badGflagFound = false;

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
  usage.append(" -connection_url url_produced_by_receiver # for a sender");
  google::SetUsageMessage(usage);
  google::gflags_exitfunc = [](int code) {
    if (FLAGS_exit_on_bad_flags) {
      exit(code);
    }
    badGflagFound = true;
  };
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  if (badGflagFound) {
    LOG(ERROR) << "Continuing despite bad flags";
  }
  signal(SIGPIPE, SIG_IGN);

  // Might be a sub class (fbonly wdtCmdLine.cpp)
  Wdt &wdt = WDTCLASS::initializeWdt(FLAGS_app_name);
  if (FLAGS_print_options) {
    wdt.printWdtOptions(std::cout);
    return 0;
  }

  ErrorCode retCode = OK;

  // Odd ball case of log parsing
  if (FLAGS_parse_transfer_log) {
    // Log parsing mode
    WdtOptions::getMutable().enable_download_resumption = true;
    TransferLogManager transferLogManager;
    transferLogManager.setRootDir(FLAGS_directory);
    transferLogManager.openLog();
    bool success = transferLogManager.parseAndPrint();
    LOG_IF(ERROR, success) << "Transfer log parsing failed";
    transferLogManager.closeLog();
    return success ? OK : ERROR;
  }

  // General case : Sender or Receiver
  const auto &options = WdtOptions::get();
  std::unique_ptr<WdtTransferRequest> reqPtr;
  if (FLAGS_connection_url.empty()) {
    reqPtr = folly::make_unique<WdtTransferRequest>(
        options.start_port, options.num_ports, FLAGS_directory);
    reqPtr->hostName = FLAGS_destination;
    reqPtr->transferId = FLAGS_transfer_id;
    if (!FLAGS_test_only_encryption_secret.empty()) {
      reqPtr->encryptionData =
          EncryptionParams(parseEncryptionType(options.encryption_type),
                           FLAGS_test_only_encryption_secret);
    }
  } else {
    reqPtr = folly::make_unique<WdtTransferRequest>(FLAGS_connection_url);
    if (reqPtr->errorCode != OK) {
      LOG(ERROR) << "Invalid url \"" << FLAGS_connection_url
                 << "\" : " << errorCodeToStr(reqPtr->errorCode);
      return ERROR;
    }
    reqPtr->directory = FLAGS_directory;
    LOG(INFO) << "Parsed url as " << reqPtr->getLogSafeString();
  }
  WdtTransferRequest &req = *reqPtr;
  if (FLAGS_protocol_version > 0) {
    req.protocolVersion = FLAGS_protocol_version;
  }

  if (FLAGS_destination.empty() && FLAGS_connection_url.empty()) {
    Receiver receiver(req);
    WdtTransferRequest augmentedReq = receiver.init();
    if (augmentedReq.errorCode == FEWER_PORTS) {
      if (FLAGS_treat_fewer_port_as_error) {
        LOG(ERROR) << "Receiver could not bind to all the ports";
        return FEWER_PORTS;
      }
    } else if (augmentedReq.errorCode != OK) {
      LOG(ERROR) << "Error setting up receiver";
      return augmentedReq.errorCode;
    }
    // In the log:
    LOG(INFO) << "Starting receiver with connection url "
              << augmentedReq.getLogSafeString();  // The url without secret
    // on stdout: the one with secret:
    std::cout << augmentedReq.genWdtUrlWithSecret() << std::endl;
    std::cout.flush();
    setAbortChecker(receiver);
    if (!FLAGS_recovery_id.empty()) {
      WdtOptions::getMutable().enable_download_resumption = true;
      receiver.setRecoveryId(FLAGS_recovery_id);
    }
    if (!FLAGS_run_as_daemon) {
      retCode = receiver.transferAsync();
      if (retCode == OK) {
        std::unique_ptr<TransferReport> report = receiver.finish();
        retCode = report->getSummary().getErrorCode();
      }
    } else {
      retCode = receiver.runForever();
      // not reached
    }
  } else {
    // Sender mode
    if (!FLAGS_manifest.empty()) {
      // Each line should have the filename and optionally
      // the filesize separated by a single space
      if (FLAGS_manifest == "-") {
        readManifest(std::cin, req);
      } else {
        std::ifstream fin(FLAGS_manifest);
        readManifest(fin, req);
        fin.close();
      }
      LOG(INFO) << "Using files lists, number of files " << req.fileInfo.size();
    }
    LOG(INFO) << "Making Sender with encryption set = "
              << req.encryptionData.isSet();

    // TODO: find something more useful for namespace (userid ? directory?)
    // (shardid at fb)
    retCode = wdt.wdtSend(WdtResourceController::kGlobalNamespace, req,
                          setupAbortChecker());
  }
  cancelAbort();
  LOG(INFO) << "Returning with code " << retCode << " "
            << errorCodeToStr(retCode);
  return retCode;
}
