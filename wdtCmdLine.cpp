/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Receiver.h>
#include <wdt/Wdt.h>
#include <wdt/WdtResourceController.h>

#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <signal.h>
#include <chrono>
#include <fstream>
#include <future>
#include <iostream>
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
DEFINE_bool(fork, false,
            "If true, forks the receiver, if false, no forking/stay in fg");

DEFINE_bool(run_as_daemon, false,
            "If true, run the receiver as never ending process");

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_string(manifest, "",
              "If specified, then we will read a list of files and optional "
              "sizes from this file, use - for stdin");
DEFINE_string(
    destination, "",
    "empty is server (destination) mode, non empty is destination host");

DEFINE_string(hostname, "", "override hostname in transfe request");

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
              " (incl. transfer_id and other parameters)."
              " Deprecated: use - arg instead for safe encryption key"
              " transmission");

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

DEFINE_string(namespace, "", "WDT namespace (e.g shard)");

DEFINE_string(dest_id, "",
              "Unique destination identifier (will default to hostname)");

DECLARE_bool(help);

using namespace facebook::wdt;

// TODO: move this to some util and/or delete
template <typename T>
std::ostream &operator<<(std::ostream &os, const std::set<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}

std::mutex abortMutex;
std::condition_variable abortCondVar;
bool isAbortCancelled = false;
std::shared_ptr<WdtAbortChecker> setupAbortChecker() {
  int abortSeconds = FLAGS_abort_after_seconds;
  if (abortSeconds <= 0) {
    return nullptr;
  }
  WLOG(INFO) << "Setting up abort " << abortSeconds << " seconds.";
  static std::atomic<bool> abortTrigger{false};
  auto res = std::make_shared<WdtAbortChecker>(abortTrigger);
  auto lambda = [=] {
    WLOG(INFO) << "Will abort in " << abortSeconds << " seconds.";
    std::unique_lock<std::mutex> lk(abortMutex);
    bool isNotAbort =
        abortCondVar.wait_for(lk, std::chrono::seconds(abortSeconds),
                              [&]() -> bool { return isAbortCancelled; });
    if (isNotAbort) {
      WLOG(INFO) << "Already finished normally, no abort.";
    } else {
      WLOG(INFO) << "Requesting abort.";
      abortTrigger.store(true);
    }
  };
  // Run this in a separate thread concurrently with sender/receiver
  static auto f = std::async(std::launch::async, lambda);
  return res;
}

void setAbortChecker(WdtBase &senderOrReceiver) {
  senderOrReceiver.setAbortChecker(setupAbortChecker());
}

void cancelAbort() {
  {
    std::unique_lock<std::mutex> lk(abortMutex);
    isAbortCancelled = true;
    abortCondVar.notify_one();
  }
  std::this_thread::yield();
}

void readManifest(std::istream &fin, WdtTransferRequest &req, bool dfltDirect) {
  std::string line;
  while (std::getline(fin, line)) {
    std::vector<std::string> fields;
    folly::split('\t', line, fields, true);
    if (fields.empty() || fields.size() > 3) {
      WLOG(FATAL) << "Invalid input manifest: " << line;
    }
    int64_t filesize = fields.size() > 1 ? folly::to<int64_t>(fields[1]) : -1;
    bool odirect = fields.size() > 2 ? folly::to<bool>(fields[2]) : dfltDirect;
    req.fileInfo.emplace_back(fields[0], filesize, odirect);
  }
  req.disableDirectoryTraversal = true;
}

namespace GFLAGS_NAMESPACE {
extern GFLAGS_DLL_DECL void (*gflags_exitfunc)(int);
}

bool badGflagFound = false;

static std::string usage;
void printUsage() {
  std::cerr << usage << std::endl;
}

void sigUSR1Handler(int) {
  ReportPerfSignalSubscriber::notify();
}

int main(int argc, char *argv[]) {
#ifdef WDTFBINIT
  WDTFBINIT
#endif

  FLAGS_logtostderr = true;
  // Ugliness in gflags' api; to be able to use program name
  GFLAGS_NAMESPACE::SetArgv(argc, const_cast<const char **>(argv));
  GFLAGS_NAMESPACE::SetVersionString(Protocol::getFullVersion());
  usage.assign("WDT Warp-speed Data Transfer. v ");
  usage.append(GFLAGS_NAMESPACE::VersionString());
  usage.append(". Sample usage:\nTo transfer from srchost to desthost:\n\t");
  usage.append("ssh dsthost ");
  usage.append(GFLAGS_NAMESPACE::ProgramInvocationShortName());
  usage.append(" -directory destdir | ssh srchost ");
  usage.append(GFLAGS_NAMESPACE::ProgramInvocationShortName());
  usage.append(" -directory srcdir -");
  usage.append(
      "\nPassing - as the argument to wdt means start the sender and"
      " read the");
  usage.append(
      "\nconnection URL produced by the receiver, including encryption"
      " key, from stdin.");
  usage.append("\nUse --help to see all the options.");
  GFLAGS_NAMESPACE::SetUsageMessage(usage);
  GFLAGS_NAMESPACE::gflags_exitfunc = [](int code) {
    if (code == 0 || FLAGS_help) {
      // By default gflags exit 1 with --help and 0 for --version (good)
      // let's also exit(0) for --help to be like most gnu command line
      exit(0);
    }
    // error cases:
    if (FLAGS_exit_on_bad_flags) {
      printUsage();
      exit(code);
    }
    badGflagFound = true;
  };
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  if (badGflagFound) {
    // will only work for receivers
    WLOG(ERROR) << "Continuing despite bad flags";
  } else {
    // Only non -flag argument allowed so far is "-" meaning
    // Read url from stdin and start a sender
    if (argc > 2 || (argc == 2 && (argv[1][0] != '-' || argv[1][1] != '\0'))) {
      printUsage();
      std::cerr << "Error: argument should be - (to read url from stdin) "
                << "or no arguments" << std::endl;
      exit(1);
    }
  }
  signal(SIGPIPE, SIG_IGN);
  signal(SIGUSR1, sigUSR1Handler);

  std::string connectUrl;
  if (!badGflagFound && argc == 2) {
    std::getline(std::cin, connectUrl);
    if (connectUrl.empty()) {
      WLOG(ERROR)
          << "Sender unable to read connection url from stdin - exiting";
      return URI_PARSE_ERROR;
    }
  } else {
    connectUrl = FLAGS_connection_url;
  }

  // Might be a sub class (fbonly wdtCmdLine.cpp)
  Wdt &wdt = WDTCLASS::initializeWdt(FLAGS_app_name);
  if (FLAGS_print_options) {
    wdt.printWdtOptions(std::cout);
    return 0;
  }
  WdtOptions &options = wdt.getWdtOptions();

  ErrorCode retCode = OK;

  // Odd ball case of log parsing
  if (FLAGS_parse_transfer_log) {
    // Log parsing mode
    options.enable_download_resumption = true;
    TransferLogManager transferLogManager(options, FLAGS_directory);
    transferLogManager.openLog();
    bool success = transferLogManager.parseAndPrint();
    WLOG_IF(ERROR, !success) << "Transfer log parsing failed";
    transferLogManager.closeLog();
    return success ? OK : ERROR;
  }

  // General case : Sender or Receiver
  std::unique_ptr<WdtTransferRequest> reqPtr;
  if (connectUrl.empty()) {
    reqPtr = std::make_unique<WdtTransferRequest>(
        options.start_port, options.num_ports, FLAGS_directory);
    reqPtr->hostName = FLAGS_destination;
    reqPtr->transferId = FLAGS_transfer_id;
    if (!FLAGS_test_only_encryption_secret.empty()) {
      reqPtr->encryptionData =
          EncryptionParams(parseEncryptionType(options.encryption_type),
                           FLAGS_test_only_encryption_secret);
    }
    reqPtr->ivChangeInterval = options.iv_change_interval_mb * kMbToB;
    reqPtr->tls = wdt.isTlsEnabled();
  } else {
    reqPtr = std::make_unique<WdtTransferRequest>(connectUrl);
    if (reqPtr->errorCode != OK) {
      WLOG(ERROR) << "Invalid url \"" << connectUrl
                  << "\" : " << errorCodeToStr(reqPtr->errorCode);
      return ERROR;
    }
    reqPtr->directory = FLAGS_directory;
    WLOG(INFO) << "Parsed url as " << reqPtr->getLogSafeString();
  }
  WdtTransferRequest &req = *reqPtr;
  req.wdtNamespace = FLAGS_namespace;
  if (!FLAGS_dest_id.empty()) {
    req.destIdentifier = FLAGS_dest_id;
  }
  if (FLAGS_protocol_version > 0) {
    req.protocolVersion = FLAGS_protocol_version;
  }
  if (!FLAGS_hostname.empty()) {
    reqPtr->hostName = FLAGS_hostname;
  }
  if (FLAGS_destination.empty() && connectUrl.empty()) {
    Receiver receiver(req);
    WdtOptions &recOptions = receiver.getWdtOptions();
    if (FLAGS_run_as_daemon) {
      // Backward compatible with static ports, you can still get dynamic
      // daemon ports using -start_port 0 like before
      recOptions.static_ports = true;
    }
    if (!FLAGS_recovery_id.empty()) {
      // TODO: add a test for this
      recOptions.enable_download_resumption = true;
      receiver.setRecoveryId(FLAGS_recovery_id);
    }
    wdt.wdtSetReceiverSocketCreator(receiver);
    WdtTransferRequest augmentedReq = receiver.init();
    retCode = augmentedReq.errorCode;
    if (retCode == FEWER_PORTS) {
      if (FLAGS_treat_fewer_port_as_error) {
        WLOG(ERROR) << "Receiver could not bind to all the ports";
        return FEWER_PORTS;
      }
      retCode = OK;
    } else if (augmentedReq.errorCode != OK) {
      WLOG(ERROR) << "Error setting up receiver " << errorCodeToStr(retCode);
      return retCode;
    }
    // In the log:
    WLOG(INFO) << "Starting receiver with connection url "
               << augmentedReq.getLogSafeString();  // The url without secret
    // on stdout: the one with secret:
    std::cout << augmentedReq.genWdtUrlWithSecret() << std::endl;
    std::cout.flush();
    if (FLAGS_fork) {
      pid_t cpid = fork();
      if (cpid == -1) {
        perror("Failed to fork()");
        exit(1);
      }
      if (cpid > 0) {
        WLOG(INFO) << "Detaching receiver";
        exit(0);
      }
      close(0);
      close(1);
    }
    setAbortChecker(receiver);
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
        readManifest(std::cin, req, options.odirect_reads);
      } else {
        std::ifstream fin(FLAGS_manifest);
        readManifest(fin, req, options.odirect_reads);
        fin.close();
      }
      WLOG(INFO) << "Using files lists, number of files "
                 << req.fileInfo.size();
    }
    WLOG(INFO) << "Making Sender with encryption set = "
               << req.encryptionData.isSet();

    retCode = wdt.wdtSend(req, setupAbortChecker());
  }
  cancelAbort();
  if (retCode == OK) {
    WLOG(INFO) << "Returning with OK exit code";
  } else {
    WLOG(ERROR) << "Returning with code " << retCode << " "
                << errorCodeToStr(retCode);
  }
  return retCode;
}
