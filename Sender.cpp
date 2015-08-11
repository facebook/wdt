/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Sender.h"

#include "ClientSocket.h"
#include "Throttler.h"
#include "SocketUtils.h"

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/Bits.h>
#include <folly/ScopeGuard.h>
#include <sys/stat.h>
#include <folly/Checksum.h>

#include <thread>

namespace facebook {
namespace wdt {

// Constants for different calculations
/*
 * If you change any of the multipliers be
 * sure to replace them in the description above.
 */

ThreadTransferHistory::ThreadTransferHistory(DirectorySourceQueue &queue,
                                             TransferStats &threadStats)
    : queue_(queue), threadStats_(threadStats) {
}

std::string ThreadTransferHistory::getSourceId(int64_t index) {
  folly::SpinLockGuard guard(lock_);
  std::string sourceId;
  const int64_t historySize = history_.size();
  if (index >= 0 && index < historySize) {
    sourceId = history_[index]->getIdentifier();
  } else {
    LOG(WARNING) << "Trying to read out of bounds data " << index << " "
                 << history_.size();
  }
  return sourceId;
}

bool ThreadTransferHistory::addSource(std::unique_ptr<ByteSource> &source) {
  folly::SpinLockGuard guard(lock_);
  if (globalCheckpoint_) {
    // already received an error for this thread
    VLOG(1) << "adding source after global checkpoint is received. returning "
               "the source to the queue";
    markSourceAsFailed(source);
    queue_.returnToQueue(source);
    return false;
  }
  history_.emplace_back(std::move(source));
  return true;
}

int64_t ThreadTransferHistory::setCheckpointAndReturnToQueue(
    int64_t numReceivedSources, bool globalCheckpoint) {
  folly::SpinLockGuard guard(lock_);
  const int64_t historySize = history_.size();
  if (numReceivedSources > historySize) {
    LOG(ERROR)
        << "checkpoint is greater than total number of sources transfereed "
        << history_.size() << " " << numReceivedSources;
    return -1;
  }
  if (numReceivedSources < numAcknowledged_) {
    LOG(ERROR) << "new checkpoint is less than older checkpoint "
               << numAcknowledged_ << " " << numReceivedSources;
    return -1;
  }
  globalCheckpoint_ |= globalCheckpoint;
  numAcknowledged_ = numReceivedSources;
  int64_t numFailedSources = historySize - numReceivedSources;
  std::vector<std::unique_ptr<ByteSource>> sourcesToReturn;
  for (int64_t i = 0; i < numFailedSources; i++) {
    std::unique_ptr<ByteSource> source = std::move(history_.back());
    history_.pop_back();
    markSourceAsFailed(source);
    sourcesToReturn.emplace_back(std::move(source));
  }
  queue_.returnToQueue(sourcesToReturn);
  return numFailedSources;
}

std::vector<TransferStats> ThreadTransferHistory::popAckedSourceStats() {
  const int64_t historySize = history_.size();
  WDT_CHECK(numAcknowledged_ == historySize);
  // no locking needed, as this should be called after transfer has finished
  std::vector<TransferStats> sourceStats;
  while (!history_.empty()) {
    sourceStats.emplace_back(std::move(history_.back()->getTransferStats()));
    history_.pop_back();
  }
  return sourceStats;
}

void ThreadTransferHistory::markAllAcknowledged() {
  folly::SpinLockGuard guard(lock_);
  numAcknowledged_ = history_.size();
}

int64_t ThreadTransferHistory::returnUnackedSourcesToQueue() {
  return setCheckpointAndReturnToQueue(numAcknowledged_, false);
}

void ThreadTransferHistory::markSourceAsFailed(
    std::unique_ptr<ByteSource> &source) {
  TransferStats &sourceStats = source->getTransferStats();
  auto dataBytes = sourceStats.getEffectiveDataBytes();
  auto headerBytes = sourceStats.getEffectiveHeaderBytes();
  sourceStats.subtractEffectiveBytes(headerBytes, dataBytes);
  sourceStats.decrNumBlocks();
  sourceStats.setErrorCode(SOCKET_WRITE_ERROR);
  sourceStats.incrFailedAttempts();

  threadStats_.subtractEffectiveBytes(headerBytes, dataBytes);
  threadStats_.decrNumBlocks();
  threadStats_.incrFailedAttempts();
}

const Sender::StateFunction Sender::stateMap_[] = {
    &Sender::connect,         &Sender::readLocalCheckPoint,
    &Sender::sendSettings,    &Sender::sendBlocks,
    &Sender::sendDoneCmd,     &Sender::sendSizeCmd,
    &Sender::checkForAbort,   &Sender::readFileChunks,
    &Sender::readReceiverCmd, &Sender::processDoneCmd,
    &Sender::processWaitCmd,  &Sender::processErrCmd,
    &Sender::processAbortCmd, &Sender::processVersionMismatch};

Sender::Sender(const std::string &destHost, const std::string &srcDir) {
  destHost_ = destHost;
  srcDir_ = srcDir;
  transferFinished_ = true;
  const auto &options = WdtOptions::get();
  int port = options.start_port;
  int numSockets = options.num_ports;
  for (int i = 0; i < numSockets; i++) {
    ports_.push_back(port + i);
  }
  std::unique_ptr<IAbortChecker> queueAbortChecker =
      folly::make_unique<QueueAbortChecker>(this);
  dirQueue_.reset(new DirectorySourceQueue(srcDir_, queueAbortChecker));
  VLOG(3) << "Configuring the  directory queue";
  dirQueue_->setIncludePattern(options.include_regex);
  dirQueue_->setExcludePattern(options.exclude_regex);
  dirQueue_->setPruneDirPattern(options.prune_dir_regex);
  dirQueue_->setFollowSymlinks(options.follow_symlinks);
  progressReportIntervalMillis_ = options.progress_report_interval_millis;
  progressReporter_ = folly::make_unique<ProgressReporter>();
}

Sender::Sender(const WdtTransferRequest &transferRequest)
    : Sender(transferRequest.hostName, transferRequest.directory,
             transferRequest.ports, transferRequest.fileInfo) {
  transferId_ = transferRequest.transferId;
  if (transferId_.empty()) {
    transferId_ = WdtBase::generateTransferId();
  }
  setProtocolVersion(transferRequest.protocolVersion);
}

Sender::Sender(const std::string &destHost, const std::string &srcDir,
               const std::vector<int32_t> &ports,
               const std::vector<FileInfo> &srcFileInfo)
    : Sender(destHost, srcDir) {
  ports_ = ports;
  dirQueue_->setFileInfo(srcFileInfo);
}

WdtTransferRequest Sender::init() {
  WdtTransferRequest transferRequest(getPorts());
  transferRequest.transferId = transferId_;
  transferRequest.protocolVersion = protocolVersion_;
  transferRequest.directory = srcDir_;
  transferRequest.hostName = destHost_;
  // TODO Figure out what to do with file info
  // transferRequest.fileInfo = dirQueue_->getFileInfo();
  transferRequest.errorCode = OK;
  return transferRequest;
}

Sender::~Sender() {
  if (!isTransferFinished()) {
    LOG(WARNING) << "Sender being deleted. Forcefully aborting the transfer";
    abort(ABORTED_BY_APPLICATION);
  }
  finish();
}

void Sender::setIncludeRegex(const std::string &includeRegex) {
  dirQueue_->setIncludePattern(includeRegex);
}

void Sender::setExcludeRegex(const std::string &excludeRegex) {
  dirQueue_->setExcludePattern(excludeRegex);
}

void Sender::setPruneDirRegex(const std::string &pruneDirRegex) {
  dirQueue_->setPruneDirPattern(pruneDirRegex);
}

void Sender::setSrcFileInfo(const std::vector<FileInfo> &srcFileInfo) {
  dirQueue_->setFileInfo(srcFileInfo);
}

void Sender::setFollowSymlinks(const bool followSymlinks) {
  dirQueue_->setFollowSymlinks(followSymlinks);
}

void Sender::setProgressReportIntervalMillis(
    const int progressReportIntervalMillis) {
  progressReportIntervalMillis_ = progressReportIntervalMillis;
}

const std::vector<int32_t> &Sender::getPorts() const {
  return ports_;
}

const std::string &Sender::getSrcDir() const {
  return srcDir_;
}

const std::string &Sender::getDestination() const {
  return destHost_;
}

std::unique_ptr<TransferReport> Sender::getTransferReport() {
  int64_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(Clock::now() - startTime_);
  std::unique_ptr<TransferReport> transferReport =
      folly::make_unique<TransferReport>(globalThreadStats_, totalTime,
                                         totalFileSize);
  return transferReport;
}

bool Sender::isTransferFinished() {
  std::unique_lock<std::mutex> lock(mutex_);
  return transferFinished_;
}

Clock::time_point Sender::getEndTime() {
  return endTime_;
}

std::unique_ptr<TransferReport> Sender::finish() {
  std::unique_lock<std::mutex> instanceLock(instanceManagementMutex_);
  VLOG(1) << "Sender::finish()";
  if (areThreadsJoined_) {
    VLOG(1) << "Threads have already been joined. Returning the"
            << " existing transfer report";
    return getTransferReport();
  }
  const auto &options = WdtOptions::get();
  const bool twoPhases = options.two_phases;
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  const int64_t numPorts = ports_.size();
  for (int64_t i = 0; i < numPorts; i++) {
    senderThreads_[i].join();
  }
  if (!twoPhases) {
    dirThread_.join();
  }
  WDT_CHECK(numActiveThreads_ == 0);
  if (progressReportEnabled) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionFinished_.notify_all();
    }
    progressReporterThread_.join();
  }

  bool allSourcesAcked = false;
  for (auto &stats : globalThreadStats_) {
    if (stats.getErrorCode() == OK) {
      // at least one thread finished correctly
      // that means all transferred sources are acked
      allSourcesAcked = true;
      break;
    }
  }

  std::vector<TransferStats> transferredSourceStats;
  for (auto &transferHistory : transferHistories_) {
    if (allSourcesAcked) {
      transferHistory.markAllAcknowledged();
    } else {
      transferHistory.returnUnackedSourcesToQueue();
    }
    if (WdtOptions::get().full_reporting) {
      std::vector<TransferStats> stats = transferHistory.popAckedSourceStats();
      transferredSourceStats.insert(transferredSourceStats.end(),
                                    std::make_move_iterator(stats.begin()),
                                    std::make_move_iterator(stats.end()));
    }
  }

  if (WdtOptions::get().full_reporting) {
    validateTransferStats(transferredSourceStats,
                          dirQueue_->getFailedSourceStats(),
                          globalThreadStats_);
  }
  int64_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(endTime_ - startTime_);
  std::unique_ptr<TransferReport> transferReport =
      folly::make_unique<TransferReport>(
          transferredSourceStats, dirQueue_->getFailedSourceStats(),
          globalThreadStats_, dirQueue_->getFailedDirectories(), totalTime,
          totalFileSize, dirQueue_->getCount());

  if (progressReportEnabled) {
    progressReporter_->end(transferReport);
  }
  if (options.enable_perf_stat_collection) {
    PerfStatReport report;
    for (auto &perfReport : perfReports_) {
      report += perfReport;
    }
    LOG(INFO) << report;
  }
  double directoryTime;
  directoryTime = dirQueue_->getDirectoryTime();
  LOG(INFO) << "Total sender time = " << totalTime << " seconds ("
            << directoryTime << " dirTime)"
            << ". Transfer summary : " << *transferReport
            << "\nTotal sender throughput = "
            << transferReport->getThroughputMBps() << " Mbytes/sec ("
            << transferReport->getSummary().getEffectiveTotalBytes() /
                   (totalTime - directoryTime) / kMbToB
            << " Mbytes/sec pure transf rate)";
  areThreadsJoined_ = true;
  return transferReport;
}

ErrorCode Sender::transferAsync() {
  return start();
}

std::unique_ptr<TransferReport> Sender::transfer() {
  start();
  return finish();
}

ErrorCode Sender::start() {
  areThreadsJoined_ = false;
  transferFinished_ = false;
  const auto &options = WdtOptions::get();
  const bool twoPhases = options.two_phases;
  WDT_CHECK(!(twoPhases && options.enable_download_resumption))
      << "Two phase is not supported with download resumption";
  LOG(INFO) << "Client (sending) to " << destHost_ << ", Using ports [ "
            << ports_ << "]";
  startTime_ = Clock::now();
  downloadResumptionEnabled_ = options.enable_download_resumption;
  dirThread_ = std::move(dirQueue_->buildQueueAsynchronously());
  if (twoPhases) {
    dirThread_.join();
  }
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  if (throttler_) {
    LOG(INFO) << "Skipping throttler setup. External throttler set."
              << "Throttler details : " << *throttler_;
  } else {
    configureThrottler();
  }

  // WARNING: Do not MERGE the follwing two loops. ThreadTransferHistory keeps a
  // reference of TransferStats. And, any emplace operation on a vector
  // invalidates all its references
  const int64_t numPorts = ports_.size();
  for (int64_t i = 0; i < numPorts; i++) {
    globalThreadStats_.emplace_back(true);
  }
  for (int64_t i = 0; i < numPorts; i++) {
    transferHistories_.emplace_back(*dirQueue_, globalThreadStats_[i]);
  }
  perfReports_.resize(numPorts);
  negotiatedProtocolVersions_.resize(numPorts, 0);
  numActiveThreads_ = numPorts;
  for (int64_t i = 0; i < numPorts; i++) {
    globalThreadStats_[i].setId(folly::to<std::string>(i));
    senderThreads_.emplace_back(&Sender::sendOne, this, i);
  }
  if (progressReportEnabled) {
    progressReporter_->start();
    std::thread reporterThread(&Sender::reportProgress, this);
    progressReporterThread_ = std::move(reporterThread);
  }
  return OK;
}

void Sender::validateTransferStats(
    const std::vector<TransferStats> &transferredSourceStats,
    const std::vector<TransferStats> &failedSourceStats,
    const std::vector<TransferStats> &threadStats) {
  int64_t sourceFailedAttempts = 0;
  int64_t sourceDataBytes = 0;
  int64_t sourceEffectiveDataBytes = 0;
  int64_t sourceNumBlocks = 0;

  int64_t threadFailedAttempts = 0;
  int64_t threadDataBytes = 0;
  int64_t threadEffectiveDataBytes = 0;
  int64_t threadNumBlocks = 0;

  for (const auto &stat : transferredSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
    sourceNumBlocks += stat.getNumBlocks();
  }
  for (const auto &stat : failedSourceStats) {
    sourceFailedAttempts += stat.getFailedAttempts();
    sourceDataBytes += stat.getDataBytes();
    sourceEffectiveDataBytes += stat.getEffectiveDataBytes();
    sourceNumBlocks += stat.getNumBlocks();
  }
  for (const auto &stat : threadStats) {
    threadFailedAttempts += stat.getFailedAttempts();
    threadDataBytes += stat.getDataBytes();
    threadEffectiveDataBytes += stat.getEffectiveDataBytes();
    threadNumBlocks += stat.getNumBlocks();
  }

  WDT_CHECK(sourceFailedAttempts == threadFailedAttempts);
  WDT_CHECK(sourceDataBytes == threadDataBytes);
  WDT_CHECK(sourceEffectiveDataBytes == threadEffectiveDataBytes);
  WDT_CHECK(sourceNumBlocks == threadNumBlocks);
}

void Sender::setSocketCreator(const SocketCreator socketCreator) {
  socketCreator_ = socketCreator;
}

std::unique_ptr<ClientSocket> Sender::connectToReceiver(const int port,
                                                        ErrorCode &errCode) {
  auto startTime = Clock::now();
  const auto &options = WdtOptions::get();
  int connectAttempts = 0;
  std::unique_ptr<ClientSocket> socket;
  if (!socketCreator_) {
    // socket creator not set, creating ClientSocket
    socket = folly::make_unique<ClientSocket>(
        destHost_, folly::to<std::string>(port), &abortCheckerCallback_);
  } else {
    socket = socketCreator_(destHost_, folly::to<std::string>(port),
                            &abortCheckerCallback_);
  }
  double retryInterval = options.sleep_millis;
  int maxRetries = options.max_retries;
  if (maxRetries < 1) {
    LOG(ERROR) << "Invalid max_retries " << maxRetries << " using 1 instead";
    maxRetries = 1;
  }
  for (int i = 1; i <= maxRetries; ++i) {
    ++connectAttempts;
    errCode = socket->connect();
    if (errCode == OK) {
      break;
    } else if (errCode == CONN_ERROR) {
      return nullptr;
    }
    if (getCurAbortCode() != OK) {
      errCode = ABORT;
      return nullptr;
    }
    if (i != maxRetries) {
      // sleep between attempts but not after the last
      VLOG(1) << "Sleeping after failed attempt " << i;
      usleep(retryInterval * 1000);
    }
  }
  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  if (errCode != OK) {
    LOG(ERROR) << "Unable to connect to " << destHost_ << " " << port
               << " despite " << connectAttempts << " retries in "
               << elapsedSecsConn << " seconds.";
    errCode = CONN_ERROR;
    return nullptr;
  }
  ((connectAttempts > 1) ? LOG(WARNING) : LOG(INFO))
      << "Connection took " << connectAttempts << " attempt(s) and "
      << elapsedSecsConn << " seconds. port " << port;
  return socket;
}

Sender::SenderState Sender::connect(ThreadData &data) {
  VLOG(1) << "entered CONNECT state " << data.threadIndex_;
  int port = ports_[data.threadIndex_];
  TransferStats &threadStats = data.threadStats_;
  auto &socket = data.socket_;

  if (socket) {
    socket->close();
  }

  ErrorCode code;
  socket = connectToReceiver(port, code);
  if (code == ABORT) {
    threadStats.setErrorCode(ABORT);
    if (getCurAbortCode() == VERSION_MISMATCH) {
      return PROCESS_VERSION_MISMATCH;
    }
    return END;
  }
  if (code != OK) {
    threadStats.setErrorCode(code);
    return END;
  }
  // clearing the totalSizeSent_ flag. This way if anything breaks, we resendthe
  // total size.
  data.totalSizeSent_ = false;
  auto nextState =
      threadStats.getErrorCode() == OK ? SEND_SETTINGS : READ_LOCAL_CHECKPOINT;
  // clear the error code, as this is a new transfer
  threadStats.setErrorCode(OK);
  return nextState;
}

Sender::SenderState Sender::readLocalCheckPoint(ThreadData &data) {
  LOG(INFO) << "entered READ_LOCAL_CHECKPOINT state " << data.threadIndex_;
  int port = ports_[data.threadIndex_];
  TransferStats &threadStats = data.threadStats_;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();

  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  char *buf = data.buf_;
  int64_t numRead = data.socket_->read(buf, Protocol::kMaxLocalCheckpoint);
  if (numRead != Protocol::kMaxLocalCheckpoint) {
    VLOG(1) << "read mismatch " << Protocol::kMaxLocalCheckpoint << " "
            << numRead << " port " << port;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }
  if (!Protocol::decodeCheckpoints(
          buf, decodeOffset, Protocol::kMaxLocalCheckpoint, checkpoints)) {
    LOG(ERROR) << "checkpoint decode failure "
               << folly::humanify(
                      std::string(buf, Protocol::kMaxLocalCheckpoint));
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  if (checkpoints.size() != 1 || checkpoints[0].first != port) {
    LOG(ERROR) << "illegal local checkpoint "
               << folly::humanify(
                      std::string(buf, Protocol::kMaxLocalCheckpoint));
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  auto checkpoint = checkpoints[0].second;
  VLOG(1) << "received local checkpoint " << port << " " << checkpoint;

  if (checkpoint == -1) {
    // Receiver failed while sending DONE cmd
    return READ_RECEIVER_CMD;
  }

  auto numReturned =
      transferHistory.setCheckpointAndReturnToQueue(checkpoint, false);
  if (numReturned == -1) {
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  VLOG(1) << numRead << " number of source(s) returned to queue";
  return SEND_SETTINGS;
}

Sender::SenderState Sender::sendSettings(ThreadData &data) {
  VLOG(1) << "entered SEND_SETTINGS state " << data.threadIndex_;

  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  auto &socket = data.socket_;
  auto &options = WdtOptions::get();
  int64_t readTimeoutMillis = options.read_timeout_millis;
  int64_t writeTimeoutMillis = options.write_timeout_millis;
  int64_t off = 0;
  buf[off++] = Protocol::SETTINGS_CMD;
  bool sendFileChunks;
  {
    std::lock_guard<std::mutex> lock(mutex_);
    sendFileChunks =
        downloadResumptionEnabled_ &&
        protocolVersion_ >= Protocol::DOWNLOAD_RESUMPTION_VERSION &&
        !fileChunksReceived_;
  }
  Settings settings;
  settings.readTimeoutMillis = readTimeoutMillis;
  settings.writeTimeoutMillis = writeTimeoutMillis;
  settings.transferId = transferId_;
  settings.enableChecksum = options.enable_checksum;
  settings.sendFileChunks = sendFileChunks;
  Protocol::encodeSettings(protocolVersion_, buf, off, Protocol::kMaxSettings,
                           settings);
  int64_t toWrite = sendFileChunks ? Protocol::kMinBufLength : off;
  int64_t written = socket->write(buf, toWrite);
  if (written != toWrite) {
    LOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return CONNECT;
  }
  threadStats.addHeaderBytes(toWrite);
  return sendFileChunks ? READ_FILE_CHUNKS : SEND_BLOCKS;
}

Sender::SenderState Sender::sendBlocks(ThreadData &data) {
  VLOG(1) << "entered SEND_BLOCKS state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  auto &totalSizeSent = data.totalSizeSent_;

  if (protocolVersion_ >= Protocol::RECEIVER_PROGRESS_REPORT_VERSION &&
      !totalSizeSent && dirQueue_->fileDiscoveryFinished()) {
    return SEND_SIZE_CMD;
  }

  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source = dirQueue_->getNextSource(transferStatus);
  if (!source) {
    return SEND_DONE_CMD;
  }
  WDT_CHECK(!source->hasError());
  TransferStats transferStats =
      sendOneByteSource(data.socket_, source, transferStatus);
  threadStats += transferStats;
  source->addTransferStats(transferStats);
  source->close();
  if (transferStats.getErrorCode() == OK) {
    if (!transferHistory.addSource(source)) {
      // global checkpoint received for this thread. no point in
      // continuing
      LOG(ERROR) << "global checkpoint received, no point in continuing";
      threadStats.setErrorCode(CONN_ERROR);
      return END;
    }
  } else {
    dirQueue_->returnToQueue(source);
    return CHECK_FOR_ABORT;
  }
  return SEND_BLOCKS;
}

Sender::SenderState Sender::sendSizeCmd(ThreadData &data) {
  VLOG(1) << "entered SEND_SIZE_CMD state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  auto &socket = data.socket_;
  auto &totalSizeSent = data.totalSizeSent_;
  int64_t off = 0;
  buf[off++] = Protocol::SIZE_CMD;

  Protocol::encodeSize(buf, off, Protocol::kMaxSize, dirQueue_->getTotalSize());
  int64_t written = socket->write(buf, off);
  if (written != off) {
    LOG(ERROR) << "Socket write error " << off << " " << written;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats.addHeaderBytes(off);
  totalSizeSent = true;
  return SEND_BLOCKS;
}

Sender::SenderState Sender::sendDoneCmd(ThreadData &data) {
  VLOG(1) << "entered SEND_DONE_CMD state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  auto &socket = data.socket_;
  int64_t off = 0;
  buf[off++] = Protocol::DONE_CMD;

  auto pair = dirQueue_->getNumBlocksAndStatus();
  int64_t numBlocksDiscovered = pair.first;
  ErrorCode transferStatus = pair.second;
  buf[off++] = transferStatus;

  Protocol::encodeDone(buf, off, Protocol::kMaxDone, numBlocksDiscovered);

  int toWrite = Protocol::kMinBufLength;
  int64_t written = socket->write(buf, toWrite);
  if (written != toWrite) {
    LOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats.addHeaderBytes(toWrite);
  VLOG(1) << "Wrote done cmd on " << socket->getFd() << " waiting for reply...";
  return READ_RECEIVER_CMD;
}

Sender::SenderState Sender::checkForAbort(ThreadData &data) {
  LOG(INFO) << "entered CHECK_FOR_ABORT state " << data.threadIndex_;
  char *buf = data.buf_;
  auto &threadStats = data.threadStats_;
  auto &socket = data.socket_;

  auto numRead = socket->read(buf, 1);
  if (numRead != 1) {
    VLOG(1) << "No abort cmd found";
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[0];
  if (cmd != Protocol::ABORT_CMD) {
    VLOG(1) << "Unexpected result found while reading for abort " << buf[0];
    return CONNECT;
  }
  threadStats.addHeaderBytes(1);
  return PROCESS_ABORT_CMD;
}

Sender::SenderState Sender::readFileChunks(ThreadData &data) {
  LOG(INFO) << "entered READ_FILE_CHUNKS state " << data.threadIndex_;
  char *buf = data.buf_;
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;
  int64_t numRead = socket->read(buf, 1);
  if (numRead != 1) {
    LOG(ERROR) << "Socket read error 1 " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats.addHeaderBytes(numRead);
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[0];
  if (cmd == Protocol::ABORT_CMD) {
    return PROCESS_ABORT_CMD;
  }
  if (cmd == Protocol::WAIT_CMD) {
    return READ_FILE_CHUNKS;
  }
  if (cmd == Protocol::ACK_CMD) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (!fileChunksReceived_) {
        LOG(ERROR) << "Sender has not yet received file chunks, but receiver "
                      "thinks it has already sent it";
        threadStats.setErrorCode(PROTOCOL_ERROR);
        return END;
      }
    }
    return SEND_BLOCKS;
  }
  if (cmd != Protocol::CHUNKS_CMD) {
    LOG(ERROR) << "Unexpected cmd " << cmd;
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  int64_t toRead = Protocol::kChunksCmdLen;
  numRead = socket->read(buf, toRead);
  if (numRead != toRead) {
    LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats.addHeaderBytes(numRead);
  int64_t off = 0;
  int64_t bufSize, numFiles;
  Protocol::decodeChunksCmd(buf, off, bufSize, numFiles);
  LOG(INFO) << "File chunk list has " << numFiles
            << " entries and is broken in buffers of length " << bufSize;
  std::unique_ptr<char[]> chunkBuffer(new char[bufSize]);
  std::vector<FileChunksInfo> fileChunksInfoList;
  while (true) {
    int64_t numFileChunks = fileChunksInfoList.size();
    if (numFileChunks > numFiles) {
      // We should never be able to read more file chunks than mentioned in the
      // chunks cmd. Chunks cmd has buffer size used to transfer chunks and also
      // number of chunks. This chunks are read and parsed and added to
      // fileChunksInfoList. Number of chunks we decode should match with the
      // number mentioned in the Chunks cmd.
      LOG(ERROR) << "Number of file chunks received is more than the number "
                    "mentioned in CHUNKS_CMD "
                 << numFileChunks << " " << numFiles;
      threadStats.setErrorCode(PROTOCOL_ERROR);
      return END;
    }
    if (numFileChunks == numFiles) {
      break;
    }
    toRead = sizeof(int32_t);
    numRead = socket->read(buf, toRead);
    if (numRead != toRead) {
      LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats.setErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    toRead = folly::loadUnaligned<int32_t>(buf);
    toRead = folly::Endian::little(toRead);
    numRead = socket->read(chunkBuffer.get(), toRead);
    if (numRead != toRead) {
      LOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats.setErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    threadStats.addHeaderBytes(numRead);
    off = 0;
    // decode function below adds decoded file chunks to fileChunksInfoList
    bool success = Protocol::decodeFileChunksInfoList(
        chunkBuffer.get(), off, toRead, fileChunksInfoList);
    if (!success) {
      LOG(ERROR) << "Unable to decode file chunks list";
      threadStats.setErrorCode(PROTOCOL_ERROR);
      return END;
    }
  }
  {
    std::lock_guard<std::mutex> lock(mutex_);
    if (fileChunksReceived_) {
      LOG(WARNING) << "File chunks list received multiple times";
    } else {
      dirQueue_->setPreviouslyReceivedChunks(fileChunksInfoList);
      fileChunksReceived_ = true;
    }
  }
  // send ack for file chunks list
  buf[0] = Protocol::ACK_CMD;
  int64_t toWrite = 1;
  int64_t written = socket->write(buf, toWrite);
  if (toWrite != written) {
    LOG(ERROR) << "Socket write error " << toWrite << " " << written;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats.addHeaderBytes(written);
  return SEND_BLOCKS;
}

Sender::SenderState Sender::readReceiverCmd(ThreadData &data) {
  VLOG(1) << "entered READ_RECEIVER_CMD state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  int64_t numRead = data.socket_->read(buf, 1);
  if (numRead != 1) {
    LOG(ERROR) << "READ unexpected " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[0];
  if (cmd == Protocol::ERR_CMD) {
    return PROCESS_ERR_CMD;
  }
  if (cmd == Protocol::WAIT_CMD) {
    return PROCESS_WAIT_CMD;
  }
  if (cmd == Protocol::DONE_CMD) {
    return PROCESS_DONE_CMD;
  }
  if (cmd == Protocol::ABORT_CMD) {
    return PROCESS_ABORT_CMD;
  }
  threadStats.setErrorCode(PROTOCOL_ERROR);
  return END;
}

Sender::SenderState Sender::processDoneCmd(ThreadData &data) {
  VLOG(1) << "entered PROCESS_DONE_CMD state " << data.threadIndex_;
  int port = ports_[data.threadIndex_];
  char *buf = data.buf_;
  auto &socket = data.socket_;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  transferHistory.markAllAcknowledged();

  // send ack for DONE
  buf[0] = Protocol::DONE_CMD;
  socket->write(buf, 1);

  socket->shutdown();
  auto numRead = socket->read(buf, Protocol::kMinBufLength);
  if (numRead != 0) {
    LOG(WARNING) << "EOF not found when expected";
    return END;
  }
  VLOG(1) << "done with transfer, port " << port;
  return END;
}

Sender::SenderState Sender::processWaitCmd(ThreadData &data) {
  LOG(INFO) << "entered PROCESS_WAIT_CMD state " << data.threadIndex_;
  int port = ports_[data.threadIndex_];
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  VLOG(1) << "received WAIT_CMD, port " << port;
  transferHistory.markAllAcknowledged();
  return READ_RECEIVER_CMD;
}

Sender::SenderState Sender::processErrCmd(ThreadData &data) {
  int port = ports_[data.threadIndex_];
  LOG(INFO) << "entered PROCESS_ERR_CMD state " << data.threadIndex_ << " port "
            << port;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  TransferStats &threadStats = data.threadStats_;
  auto &transferHistories = data.transferHistories_;
  auto &socket = data.socket_;
  char *buf = data.buf_;

  int64_t toRead = sizeof(int16_t);
  int64_t numRead = socket->read(buf, toRead);
  if (numRead != toRead) {
    LOG(ERROR) << "read unexpected " << toRead << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  int16_t checkpointsLen = folly::loadUnaligned<int16_t>(buf);
  checkpointsLen = folly::Endian::little(checkpointsLen);
  char checkpointBuf[checkpointsLen];
  numRead = socket->read(checkpointBuf, checkpointsLen);
  if (numRead != checkpointsLen) {
    LOG(ERROR) << "read unexpected " << checkpointsLen << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  if (!Protocol::decodeCheckpoints(checkpointBuf, decodeOffset, checkpointsLen,
                                   checkpoints)) {
    LOG(ERROR) << "checkpoint decode failure "
               << folly::humanify(std::string(checkpointBuf, checkpointsLen));
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  transferHistory.markAllAcknowledged();
  for (auto &checkpoint : checkpoints) {
    auto errPort = checkpoint.first;
    auto errPoint = checkpoint.second;
    auto it = std::find(ports_.begin(), ports_.end(), errPort);
    if (it == ports_.end()) {
      LOG(ERROR) << "Invalid checkpoint " << errPoint
                 << ". No sender thread running on port " << errPort;
      continue;
    }
    auto errThread = it - ports_.begin();
    VLOG(1) << "received global checkpoint " << errThread << " -> " << errPoint;
    transferHistories[errThread].setCheckpointAndReturnToQueue(errPoint, true);
  }
  return SEND_BLOCKS;
}

Sender::SenderState Sender::processAbortCmd(ThreadData &data) {
  LOG(INFO) << "entered PROCESS_ABORT_CMD state " << data.threadIndex_;
  char *buf = data.buf_;
  auto &threadStats = data.threadStats_;
  auto &socket = data.socket_;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();

  threadStats.setErrorCode(ABORT);
  int toRead = Protocol::kAbortLength;
  auto numRead = socket->read(buf, toRead);
  if (numRead != toRead) {
    // can not read checkpoint, but still must exit because of ABORT
    LOG(ERROR) << "Error while trying to read ABORT cmd " << numRead << " "
               << toRead;
    return END;
  }
  int64_t offset = 0;
  int32_t negotiatedProtocol;
  ErrorCode remoteError;
  int64_t checkpoint;
  Protocol::decodeAbort(buf, offset, negotiatedProtocol, remoteError,
                        checkpoint);
  threadStats.setRemoteErrorCode(remoteError);
  std::string failedFileName = transferHistory.getSourceId(checkpoint);
  LOG(WARNING) << "Received abort on " << data.threadIndex_
               << " remote protocol version " << negotiatedProtocol
               << " remote error code " << errorCodeToStr(remoteError)
               << " file " << failedFileName << " checkpoint " << checkpoint;
  abort(remoteError);
  if (remoteError == VERSION_MISMATCH) {
    if (Protocol::negotiateProtocol(negotiatedProtocol, protocolVersion_) ==
        negotiatedProtocol) {
      // sender can support this negotiated version
      negotiatedProtocolVersions_[data.threadIndex_] = negotiatedProtocol;
      return PROCESS_VERSION_MISMATCH;
    } else {
      LOG(ERROR) << "Sender can not support receiver version "
                 << negotiatedProtocol;
      threadStats.setRemoteErrorCode(VERSION_INCOMPATIBLE);
    }
  }
  return END;
}

Sender::SenderState Sender::processVersionMismatch(ThreadData &data) {
  LOG(INFO) << "entered PROCESS_VERSION_MISMATCH state " << data.threadIndex_;
  auto &threadStats = data.threadStats_;

  WDT_CHECK(threadStats.getErrorCode() == ABORT);
  std::unique_lock<std::mutex> lock(mutex_);
  if (protoNegotiationStatus_ != V_MISMATCH_WAIT) {
    LOG(WARNING) << "Protocol version already negotiated, but transfer still "
                    "aborted due to version mismatch, port "
                 << ports_[data.threadIndex_];
    return END;
  }
  numWaitingWithAbort_++;
  while (protoNegotiationStatus_ == V_MISMATCH_WAIT &&
         numWaitingWithAbort_ != numActiveThreads_) {
    WDT_CHECK(numWaitingWithAbort_ < numActiveThreads_);
    conditionAllAborted_.wait(lock);
  }
  numWaitingWithAbort_--;
  if (protoNegotiationStatus_ == V_MISMATCH_FAILED) {
    return END;
  }
  if (protoNegotiationStatus_ == V_MISMATCH_RESOLVED) {
    threadStats.setRemoteErrorCode(OK);
    return CONNECT;
  }
  protoNegotiationStatus_ = V_MISMATCH_FAILED;

  for (const auto &stat : globalThreadStats_) {
    if (stat.getErrorCode() == OK) {
      // Some other thread finished successfully, should never happen in case of
      // version mismatch
      return END;
    }
  }

  const int numHistories = transferHistories_.size();
  for (int i = 0; i < numHistories; i++) {
    auto &history = transferHistories_[i];
    if (history.getNumAcked() > 0) {
      LOG(ERROR) << "Even though the transfer aborted due to VERSION_MISMATCH, "
                    "some blocks got acked by the receiver, port "
                 << ports_[i] << " numAcked " << history.getNumAcked();
      return END;
    }
    history.returnUnackedSourcesToQueue();
  }

  int negotiatedProtocol = 0;
  for (int protocolVersion : negotiatedProtocolVersions_) {
    if (protocolVersion > 0) {
      if (negotiatedProtocol > 0 && negotiatedProtocol != protocolVersion) {
        LOG(ERROR) << "Different threads negotiated different protocols "
                   << negotiatedProtocol << " " << protocolVersion;
        return END;
      }
      negotiatedProtocol = protocolVersion;
    }
  }
  WDT_CHECK_GT(negotiatedProtocol, 0);

  LOG_IF(INFO, negotiatedProtocol != protocolVersion_)
      << "Changing protocol version to " << negotiatedProtocol
      << ", previous version " << protocolVersion_;
  protocolVersion_ = negotiatedProtocol;
  threadStats.setRemoteErrorCode(OK);
  protoNegotiationStatus_ = V_MISMATCH_RESOLVED;
  clearAbort();
  conditionAllAborted_.notify_all();
  return CONNECT;
}

void Sender::sendOne(int threadIndex) {
  INIT_PERF_STAT_REPORT
  std::vector<ThreadTransferHistory> &transferHistories = transferHistories_;
  TransferStats &threadStats = globalThreadStats_[threadIndex];
  Clock::time_point startTime = Clock::now();
  int port = ports_[threadIndex];
  auto completionGuard = folly::makeGuard([&] {
    std::unique_lock<std::mutex> lock(mutex_);
    numActiveThreads_--;
    if (numActiveThreads_ == 0) {
      LOG(INFO) << "Last thread finished "
                << durationSeconds(Clock::now() - startTime_);
      endTime_ = Clock::now();
      transferFinished_ = true;
    }
    if (throttler_) {
      throttler_->deRegisterTransfer();
    }
    conditionAllAborted_.notify_one();
  });
  if (throttler_) {
    throttler_->registerTransfer();
  }
  ThreadData threadData(threadIndex, threadStats, transferHistories);
  SenderState state = CONNECT;
  while (state != END) {
    ErrorCode abortCode = getCurAbortCode();
    if (abortCode != OK) {
      LOG(ERROR) << "Transfer aborted " << port << " "
                 << errorCodeToStr(abortCode);
      threadStats.setErrorCode(ABORT);
      if (abortCode == VERSION_MISMATCH) {
        state = PROCESS_VERSION_MISMATCH;
      } else {
        break;
      }
    }
    state = (this->*stateMap_[state])(threadData);
  }

  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Port " << port << " done. " << threadStats
            << " Total throughput = "
            << threadStats.getEffectiveTotalBytes() / totalTime / kMbToB
            << " Mbytes/sec";
  perfReports_[threadIndex] = *perfStatReport;
  return;
}

TransferStats Sender::sendOneByteSource(
    const std::unique_ptr<ClientSocket> &socket,
    const std::unique_ptr<ByteSource> &source, ErrorCode transferStatus) {
  TransferStats stats;
  auto &options = WdtOptions::get();
  char headerBuf[Protocol::kMaxHeader];
  int64_t off = 0;
  headerBuf[off++] = Protocol::FILE_CMD;
  headerBuf[off++] = transferStatus;
  char *headerLenPtr = headerBuf + off;
  off += sizeof(int16_t);
  const int64_t expectedSize = source->getSize();
  int64_t actualSize = 0;

  const SourceMetaData &metadata = source->getMetaData();
  BlockDetails blockDetails;
  blockDetails.fileName = metadata.relPath;
  blockDetails.seqId = metadata.seqId;
  blockDetails.fileSize = metadata.size;
  blockDetails.offset = source->getOffset();
  blockDetails.dataSize = source->getSize();
  blockDetails.allocationStatus = metadata.allocationStatus;
  blockDetails.prevSeqId = metadata.prevSeqId;

  Protocol::encodeHeader(protocolVersion_, headerBuf, off, Protocol::kMaxHeader,
                         blockDetails);
  int16_t littleEndianOff = folly::Endian::little((int16_t)off);
  folly::storeUnaligned<int16_t>(headerLenPtr, littleEndianOff);
  int64_t written = socket->write(headerBuf, off);
  if (written != off) {
    PLOG(ERROR) << "Write error/mismatch " << written << " " << off
                << ". fd = " << socket->getFd()
                << ". port = " << socket->getPort();
    stats.setErrorCode(SOCKET_WRITE_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  stats.addHeaderBytes(written);
  int64_t byteSourceHeaderBytes = written;
  int64_t throttlerInstanceBytes = byteSourceHeaderBytes;
  int64_t totalThrottlerBytes = 0;
  VLOG(3) << "Sent " << written << " on " << socket->getFd() << " : "
          << folly::humanify(std::string(headerBuf, off));
  int32_t checksum = 0;
  while (!source->finished()) {
    int64_t size;
    char *buffer = source->read(size);
    if (source->hasError()) {
      LOG(ERROR) << "Failed reading file " << source->getIdentifier()
                 << " for fd " << socket->getFd();
      break;
    }
    WDT_CHECK(buffer && size > 0);
    if (protocolVersion_ >= Protocol::CHECKSUM_VERSION &&
        options.enable_checksum) {
      checksum = folly::crc32c((const uint8_t *)buffer, size, checksum);
    }
    written = 0;
    if (throttler_) {
      /**
       * If throttling is enabled we call limit(deltaBytes) which
       * used both the methods of throttling peak and average.
       * Always call it with bytes being written to the wire, throttler
       * will do the rest.
       * The first time throttle is called with the header bytes
       * included. In the next iterations throttler is only called
       * with the bytes being written.
       */
      throttlerInstanceBytes += size;
      throttler_->limit(throttlerInstanceBytes);
      totalThrottlerBytes += throttlerInstanceBytes;
      throttlerInstanceBytes = 0;
    }
    do {
      int64_t w = socket->write(buffer + written, size - written);
      if (w < 0) {
        // TODO: retries, close connection etc...
        PLOG(ERROR) << "Write error " << written << " (" << size << ")"
                    << ". fd = " << socket->getFd()
                    << ". port = " << socket->getPort();
        stats.setErrorCode(SOCKET_WRITE_ERROR);
        stats.incrFailedAttempts();
        return stats;
      }
      stats.addDataBytes(w);
      written += w;
      if (w != size) {
        VLOG(1) << "Short write " << w << " sub total now " << written << " on "
                << socket->getFd() << " out of " << size;
      } else {
        VLOG(3) << "Wrote all of " << size << " on " << socket->getFd();
      }
      if (getCurAbortCode() != OK) {
        LOG(ERROR) << "Transfer aborted during block transfer "
                   << socket->getPort() << " " << source->getIdentifier();
        stats.setErrorCode(ABORT);
        stats.incrFailedAttempts();
        return stats;
      }
    } while (written < size);
    if (written > size) {
      LOG(ERROR) << "Write error " << written << " > " << size;
      stats.setErrorCode(SOCKET_WRITE_ERROR);
      stats.incrFailedAttempts();
      return stats;
    }
    actualSize += written;
  }
  if (actualSize != expectedSize) {
    // Can only happen if sender thread can not read complete source byte
    // stream
    LOG(ERROR) << "UGH " << source->getIdentifier() << " " << expectedSize
               << " " << actualSize;
    struct stat fileStat;
    if (stat(metadata.fullPath.c_str(), &fileStat) != 0) {
      PLOG(ERROR) << "stat failed on path " << metadata.fullPath;
    } else {
      LOG(WARNING) << "file " << source->getIdentifier() << " previous size "
                   << metadata.size << " current size " << fileStat.st_size;
    }
    stats.setErrorCode(BYTE_SOURCE_READ_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  if (throttler_ && actualSize > 0) {
    WDT_CHECK(totalThrottlerBytes == actualSize + byteSourceHeaderBytes)
        << totalThrottlerBytes << " " << (actualSize + totalThrottlerBytes);
  }
  if (protocolVersion_ >= Protocol::CHECKSUM_VERSION &&
      options.enable_checksum) {
    off = 0;
    headerBuf[off++] = Protocol::FOOTER_CMD;
    Protocol::encodeFooter(headerBuf, off, Protocol::kMaxFooter, checksum);
    int toWrite = off;
    written = socket->write(headerBuf, toWrite);
    if (written != toWrite) {
      LOG(ERROR) << "Write mismatch " << written << " " << toWrite;
      stats.setErrorCode(SOCKET_WRITE_ERROR);
      stats.incrFailedAttempts();
      return stats;
    }
    stats.addHeaderBytes(toWrite);
  }
  stats.setErrorCode(OK);
  stats.incrNumBlocks();
  stats.addEffectiveBytes(stats.getHeaderBytes(), stats.getDataBytes());
  return stats;
}

void Sender::reportProgress() {
  WDT_CHECK(progressReportIntervalMillis_ > 0);
  int throughputUpdateIntervalMillis =
      WdtOptions::get().throughput_update_interval_millis;
  WDT_CHECK(throughputUpdateIntervalMillis >= 0);
  int throughputUpdateInterval =
      throughputUpdateIntervalMillis / progressReportIntervalMillis_;

  int64_t lastEffectiveBytes = 0;
  std::chrono::time_point<Clock> lastUpdateTime = Clock::now();
  int intervalsSinceLastUpdate = 0;
  double currentThroughput = 0;

  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis_);
  LOG(INFO) << "Progress reporter tracking every "
            << progressReportIntervalMillis_ << " ms";
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionFinished_.wait_for(lock, waitingTime);
      if (transferFinished_) {
        break;
      }
    }
    if (!dirQueue_->fileDiscoveryFinished()) {
      continue;
    }

    std::unique_ptr<TransferReport> transferReport = getTransferReport();
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
}
}  // namespace facebook::wdt
