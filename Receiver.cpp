/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Receiver.h>
#include <wdt/util/FileWriter.h>
#include <wdt/util/ServerSocket.h>
#include <wdt/util/SocketUtils.h>

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <folly/Checksum.h>

#include <fcntl.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
using std::vector;
namespace facebook {
namespace wdt {
void Receiver::addCheckpoint(Checkpoint checkpoint) {
  LOG(INFO) << "Adding global checkpoint " << checkpoint.port << " "
            << checkpoint.numBlocks << " " << checkpoint.lastBlockReceivedBytes;
  checkpoints_.emplace_back(checkpoint);
}

std::vector<Checkpoint> Receiver::getNewCheckpoints(int startIndex) {
  std::vector<Checkpoint> checkpoints;
  const int64_t numCheckpoints = checkpoints_.size();
  for (int64_t i = startIndex; i < numCheckpoints; i++) {
    checkpoints.emplace_back(checkpoints_[i]);
  }
  return checkpoints;
}

Receiver::Receiver(const WdtTransferRequest &transferRequest) {
  LOG(INFO) << "WDT Receiver " << Protocol::getFullVersion();
  // TODO: move to init and validate input transfer request (like empty dir)
  // and ports and pv - issue#95
  transferId_ = transferRequest.transferId;
  if (transferId_.empty()) {
    transferId_ = WdtBase::generateTransferId();
  }
  setProtocolVersion(transferRequest.protocolVersion);
  setDir(transferRequest.directory);
  ports_ = transferRequest.ports;
}

Receiver::Receiver(int port, int numSockets, const std::string &destDir)
    : Receiver(WdtTransferRequest(port, numSockets, destDir)) {
}

void Receiver::traverseDestinationDir(
    std::vector<FileChunksInfo> &fileChunksInfo) {
  DirectorySourceQueue dirQueue(destDir_, &abortCheckerCallback_);
  dirQueue.buildQueueSynchronously();
  auto &discoveredFilesInfo = dirQueue.getDiscoveredFilesMetaData();
  for (auto &fileInfo : discoveredFilesInfo) {
    FileChunksInfo chunkInfo(fileInfo->seqId, fileInfo->relPath,
                             fileInfo->size);
    chunkInfo.addChunk(Interval(0, fileInfo->size));
    fileChunksInfo.emplace_back(std::move(chunkInfo));
  }
  return;
}

void Receiver::startNewGlobalSession(const std::string &peerIp) {
  if (throttler_) {
    // If throttler is configured/set then register this session
    // in the throttler. This is guranteed to work in either of the
    // modes long running or not. We will de register from the throttler
    // when the current session ends
    throttler_->registerTransfer();
  }
  startTime_ = Clock::now();
  const auto &options = WdtOptions::get();
  if (options.enable_download_resumption) {
    bool verifySuccessful = transferLogManager_.verifySenderIp(peerIp);
    if (!verifySuccessful) {
      fileChunksInfo_.clear();
    }
  }
  hasNewTransferStarted_.store(true);
  LOG(INFO) << "Starting new transfer,  peerIp " << peerIp << " , transfer id "
            << transferId_;
}

bool Receiver::hasNewTransferStarted() const {
  return hasNewTransferStarted_.load();
}

void Receiver::endCurGlobalSession() {
  LOG(INFO) << "Ending the transfer " << transferId_;
  if (throttler_) {
    throttler_->deRegisterTransfer();
  }
  checkpoints_.clear();
  fileCreator_->clearAllocationMap();
  // TODO might consider moving closing the transfer log here
  hasNewTransferStarted_.store(false);
}

WdtTransferRequest Receiver::init() {
  const auto &options = WdtOptions::get();
  backlog_ = options.backlog;
  bufferSize_ = options.buffer_size;
  if (bufferSize_ < Protocol::kMaxHeader) {
    // round up to even k
    bufferSize_ = 2 * 1024 * ((Protocol::kMaxHeader - 1) / (2 * 1024) + 1);
    LOG(INFO) << "Specified -buffer_size " << options.buffer_size
              << " smaller than " << Protocol::kMaxHeader << " using "
              << bufferSize_ << " instead";
  }
  auto numThreads = ports_.size();
  fileCreator_.reset(
      new FileCreator(destDir_, numThreads, transferLogManager_));
  threadsController_ = new ThreadsController(numThreads);
  threadsController_->setNumFunnels(ReceiverThread::NUM_FUNNELS);
  threadsController_->setNumBarriers(ReceiverThread::NUM_BARRIERS);
  threadsController_->setNumConditions(ReceiverThread::NUM_CONDITIONS);
  receiverThreads_ = threadsController_->makeThreads<Receiver, ReceiverThread>(
      this, ports_.size(), ports_);
  size_t numSuccessfulInitThreads = 0;
  for (auto &receiverThread : receiverThreads_) {
    ErrorCode code = receiverThread->init();
    if (code == OK) {
      ++numSuccessfulInitThreads;
    }
  }
  LOG(INFO) << "Registered " << numSuccessfulInitThreads
            << " successful sockets";
  ErrorCode code = OK;
  if (numSuccessfulInitThreads != ports_.size()) {
    code = FEWER_PORTS;
    if (numSuccessfulInitThreads == 0) {
      code = ERROR;
    }
  }
  WdtTransferRequest transferRequest(getPorts());
  transferRequest.protocolVersion = protocolVersion_;
  transferRequest.transferId = transferId_;
  LOG(INFO) << "Transfer id " << transferRequest.transferId;
  if (transferRequest.hostName.empty()) {
    char hostName[1024];
    int ret = gethostname(hostName, sizeof(hostName));
    if (ret == 0) {
      transferRequest.hostName.assign(hostName);
    } else {
      PLOG(ERROR) << "Couldn't find the host name";
      code = ERROR;
    }
  }
  transferRequest.directory = getDir();
  transferRequest.errorCode = code;
  return transferRequest;
}

void Receiver::setDir(const std::string &destDir) {
  destDir_ = destDir;
  transferLogManager_.setRootDir(destDir_);
}

TransferLogManager &Receiver::getTransferLogManager() {
  return transferLogManager_;
}

std::unique_ptr<FileCreator> &Receiver::getFileCreator() {
  return fileCreator_;
}

const std::string &Receiver::getDir() {
  return destDir_;
}

void Receiver::setRecoveryId(const std::string &recoveryId) {
  recoveryId_ = recoveryId;
  LOG(INFO) << "recovery id " << recoveryId_;
}

Receiver::~Receiver() {
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an ongoing transfer and the destructor"
                 << " is being called. Trying to finish the transfer";
    abort(ABORTED_BY_APPLICATION);
  }
  finish();
}

vector<int32_t> Receiver::getPorts() const {
  vector<int32_t> ports;
  for (const auto &receiverThread : receiverThreads_) {
    ports.push_back(receiverThread->getPort());
  }
  return ports;
}

const std::vector<FileChunksInfo> &Receiver::getFileChunksInfo() const {
  return fileChunksInfo_;
}

int64_t Receiver::getTransferConfig() const {
  auto &options = WdtOptions::get();
  int64_t config = 0;
  if (options.shouldPreallocateFiles()) {
    config = 1;
  }
  if (options.resume_using_dir_tree) {
    config |= (1 << 1);
  }
  return config;
}

bool Receiver::hasPendingTransfer() {
  std::unique_lock<std::mutex> lock(mutex_);
  return !transferFinished_;
}

void Receiver::markTransferFinished(bool isFinished) {
  std::unique_lock<std::mutex> lock(mutex_);
  transferFinished_ = isFinished;
  if (isFinished) {
    conditionRecvFinished_.notify_one();
  }
}

std::unique_ptr<TransferReport> Receiver::finish() {
  std::unique_lock<std::mutex> instanceLock(instanceManagementMutex_);
  if (areThreadsJoined_) {
    VLOG(1) << "Threads have already been joined. Returning the "
            << "transfer report";
    return getTransferReport();
  }
  const auto &options = WdtOptions::get();
  if (!isJoinable_) {
    // TODO: don't complain about this when coming from runForever()
    LOG(WARNING) << "The receiver is not joinable. The threads will never"
                 << " finish and this method will never return";
  }
  for (auto &receiverThread : receiverThreads_) {
    receiverThread->finish();
  }
  // A very important step to mark the transfer finished
  // No other transferAsync, or runForever can be called on this
  // instance unless the current transfer has finished
  markTransferFinished(true);

  if (isJoinable_) {
    // Make sure to join the progress thread.
    progressTrackerThread_.join();
  }
  std::unique_ptr<TransferReport> report = getTransferReport();
  auto &summary = report->getSummary();
  bool transferSuccess = (report->getSummary().getCombinedErrorCode() == OK);
  fixAndCloseTransferLog(transferSuccess);
  auto totalSenderBytes = summary.getTotalSenderBytes();
  if (progressReporter_ && totalSenderBytes >= 0) {
    report->setTotalFileSize(totalSenderBytes);
    report->setTotalTime(durationSeconds(Clock::now() - startTime_));
    progressReporter_->end(report);
  }
  if (options.enable_perf_stat_collection) {
    PerfStatReport globalPerfReport;
    for (auto &receiverThread : receiverThreads_) {
      globalPerfReport += receiverThread->getPerfReport();
    }
    LOG(INFO) << globalPerfReport;
  }

  LOG(WARNING) << "WDT receiver's transfer has been finished";
  LOG(INFO) << *report;
  areThreadsJoined_ = true;
  return report;
}

std::unique_ptr<TransferReport> Receiver::getTransferReport() {
  TransferStats globalStats;
  for (const auto &receiverThread : receiverThreads_) {
    globalStats += receiverThread->getTransferStats();
  }
  globalStats.validate();
  std::unique_ptr<TransferReport> report =
      folly::make_unique<TransferReport>(std::move(globalStats));
  return report;
}

ErrorCode Receiver::transferAsync() {
  const auto &options = WdtOptions::get();
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }
  isJoinable_ = true;
  int progressReportIntervalMillis = options.progress_report_interval_millis;
  if (!progressReporter_ && progressReportIntervalMillis > 0) {
    // if progress reporter has not been set, use the default one
    progressReporter_ = folly::make_unique<ProgressReporter>();
  }
  start();
  return OK;
}

ErrorCode Receiver::runForever() {
  if (hasPendingTransfer()) {
    // finish is the only method that should be able to
    // change the value of transferFinished_
    LOG(ERROR) << "There is already a transfer running on this "
               << "instance of receiver";
    return ERROR;
  }

  const auto &options = WdtOptions::get();
  WDT_CHECK(!options.enable_download_resumption)
      << "Transfer resumption not supported in long running mode";

  // Enforce the full reporting to be false in the daemon mode.
  // These statistics are expensive, and useless as they will never
  // be received/reviewed in a forever running process.
  start();
  finish();
  // This method should never finish
  return ERROR;
}

void Receiver::progressTracker() {
  const auto &options = WdtOptions::get();
  // Progress tracker will check for progress after the time specified
  // in milliseconds.
  int progressReportIntervalMillis = options.progress_report_interval_millis;
  int throughputUpdateIntervalMillis =
      WdtOptions::get().throughput_update_interval_millis;
  if (progressReportIntervalMillis <= 0 || throughputUpdateIntervalMillis < 0 ||
      !isJoinable_) {
    return;
  }
  int throughputUpdateInterval =
      throughputUpdateIntervalMillis / progressReportIntervalMillis;

  int64_t lastEffectiveBytes = 0;
  std::chrono::time_point<Clock> lastUpdateTime = Clock::now();
  int intervalsSinceLastUpdate = 0;
  double currentThroughput = 0;
  LOG(INFO) << "Progress reporter updating every "
            << progressReportIntervalMillis << " ms";
  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis);
  int64_t totalSenderBytes = -1;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionRecvFinished_.wait_for(lock, waitingTime);
      if (transferFinished_ || getCurAbortCode() != OK) {
        break;
      }
    }
    double totalTime = durationSeconds(Clock::now() - startTime_);
    TransferStats globalStats;
    for (const auto &receiverThread : receiverThreads_) {
      globalStats += receiverThread->getTransferStats();
    }
    totalSenderBytes = globalStats.getTotalSenderBytes();
    if (totalSenderBytes == -1) {
      continue;
    }
    auto transferReport = folly::make_unique<TransferReport>(
        std::move(globalStats), totalTime, totalSenderBytes);
    intervalsSinceLastUpdate++;
    if (intervalsSinceLastUpdate >= throughputUpdateInterval) {
      auto curTime = Clock::now();
      int64_t curEffectiveBytes =
          transferReport->getSummary().getEffectiveDataBytes();
      double time = durationSeconds(curTime - lastUpdateTime);
      currentThroughput = (curEffectiveBytes - lastEffectiveBytes) / time;
      lastEffectiveBytes = curEffectiveBytes;
      lastUpdateTime = curTime;
      intervalsSinceLastUpdate = 0;
    }
    transferReport->setCurrentThroughput(currentThroughput);
    progressReporter_->progress(transferReport);
  }
}

void Receiver::start() {
  startTime_ = Clock::now();
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an existing transfer in progress on this object";
  }
  areThreadsJoined_ = false;
  LOG(INFO) << "Starting (receiving) server on ports [ " << getPorts()
            << "] Target dir : " << destDir_;
  markTransferFinished(false);
  const auto &options = WdtOptions::get();
  // TODO do the init stuff here
  if (options.enable_download_resumption) {
    WDT_CHECK(!options.skip_writes)
        << "Can not skip transfers with download resumption turned on";
    if (options.resume_using_dir_tree) {
      WDT_CHECK(!options.shouldPreallocateFiles())
          << "Can not resume using directory tree if preallocation is enabled";
    }
    transferLogManager_.openLog();
    ErrorCode code = transferLogManager_.parseAndMatch(
        recoveryId_, getTransferConfig(), fileChunksInfo_);
    if (code == OK && options.resume_using_dir_tree) {
      WDT_CHECK(fileChunksInfo_.empty());
      traverseDestinationDir(fileChunksInfo_);
    }
  }
  if (!throttler_) {
    configureThrottler();
  } else {
    LOG(INFO) << "Throttler set externally. Throttler : " << *throttler_;
  }
  while (true) {
    for (auto &receiverThread : receiverThreads_) {
      receiverThread->startThread();
    }
    if (!isJoinable_) {
      // If it is long running mode, finish the threads
      // processing the current transfer and re spawn them again
      // with the same sockets
      for (auto &receiverThread : receiverThreads_) {
        receiverThread->finish();
        receiverThread->reset();
      }
      threadsController_->reset();
      continue;
    }
    break;
  }
  if (isJoinable_) {
    if (progressReporter_) {
      progressReporter_->start();
    }
    std::thread trackerThread(&Receiver::progressTracker, this);
    progressTrackerThread_ = std::move(trackerThread);
  }
}

void Receiver::addTransferLogHeader(bool isBlockMode, bool isSenderResuming) {
  const auto &options = WdtOptions::get();
  if (!options.enable_download_resumption) {
    return;
  }
  bool invalidationEntryNeeded = false;
  if (!isSenderResuming) {
    LOG(INFO) << "Sender is not in resumption mode. Invalidating directory.";
    invalidationEntryNeeded = true;
  } else if (options.resume_using_dir_tree && isBlockMode) {
    LOG(INFO) << "Sender is running in block mode, but receiver is running in "
                 "size based resumption mode. Invalidating directory.";
    invalidationEntryNeeded = true;
  }
  if (invalidationEntryNeeded) {
    transferLogManager_.invalidateDirectory();
  }
  bool isInconsistentDirectory =
      (transferLogManager_.getResumptionStatus() == INCONSISTENT_DIRECTORY);
  bool shouldWriteHeader =
      (!options.resume_using_dir_tree || !isInconsistentDirectory);
  if (shouldWriteHeader) {
    transferLogManager_.writeLogHeader();
  }
}

void Receiver::fixAndCloseTransferLog(bool transferSuccess) {
  const auto &options = WdtOptions::get();
  if (!options.enable_download_resumption) {
    return;
  }

  bool isInconsistentDirectory =
      (transferLogManager_.getResumptionStatus() == INCONSISTENT_DIRECTORY);
  bool isInvalidLog =
      (transferLogManager_.getResumptionStatus() == INVALID_LOG);
  if (transferSuccess && isInconsistentDirectory) {
    // write log header to validate directory in case of success
    WDT_CHECK(options.resume_using_dir_tree);
    transferLogManager_.writeLogHeader();
  }
  transferLogManager_.closeLog();
  if (!transferSuccess) {
    return;
  }
  if (isInvalidLog) {
    transferLogManager_.renameBuggyLog();
  }
  if (!options.keep_transfer_log) {
    transferLogManager_.unlink();
  }
}
}
}  // namespace facebook::wdt
