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

// Constants for different calculations
/*
 * If you change any of the multipliers be
 * sure to replace them in the description above.
 */
const double kPeakMultiplier = 1.2;
const int kBucketMultiplier = 2;
const double kTimeMultiplier = 0.25;

namespace {

template <typename T>
double durationSeconds(T d) {
  return std::chrono::duration_cast<std::chrono::duration<double>>(d).count();
}

}  // anonymous namespace

namespace facebook {
namespace wdt {

template <typename T>
std::ostream &operator<<(std::ostream &os, const std::vector<T> &v) {
  std::copy(v.begin(), v.end(), std::ostream_iterator<T>(os, " "));
  return os;
}

ThreadTransferHistory::ThreadTransferHistory(DirectorySourceQueue &queue,
                                             TransferStats &threadStats)
    : queue_(queue), threadStats_(threadStats) {
}

std::string ThreadTransferHistory::getSourceId(int64_t index) {
  folly::SpinLockGuard guard(lock_);
  std::string sourceId;
  if (index >= 0 && index < history_.size()) {
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
  if (numReceivedSources > history_.size()) {
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
  int64_t numFailedSources = history_.size() - numReceivedSources;
  std::vector<std::unique_ptr<ByteSource>> sourcesToReturn;
  while (history_.size() > numReceivedSources) {
    std::unique_ptr<ByteSource> source = std::move(history_.back());
    history_.pop_back();
    markSourceAsFailed(source);
    sourcesToReturn.emplace_back(std::move(source));
  }
  queue_.returnToQueue(sourcesToReturn);
  return numFailedSources;
}

std::vector<TransferStats> ThreadTransferHistory::popAckedSourceStats() {
  WDT_CHECK(numAcknowledged_ == history_.size());
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
    &Sender::connect, &Sender::readLocalCheckPoint, &Sender::sendSettings,
    &Sender::sendBlocks, &Sender::sendDoneCmd, &Sender::sendSizeCmd,
    &Sender::checkForAbort, &Sender::readReceiverCmd, &Sender::processDoneCmd,
    &Sender::processWaitCmd, &Sender::processErrCmd, &Sender::processAbortCmd};

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
  dirQueue_.reset(new DirectorySourceQueue(srcDir_));
  VLOG(3) << "Configuring the  directory queue";
  dirQueue_->setIncludePattern(options.include_regex);
  dirQueue_->setExcludePattern(options.exclude_regex);
  dirQueue_->setPruneDirPattern(options.prune_dir_regex);
  dirQueue_->setFollowSymlinks(options.follow_symlinks);
  progressReportIntervalMillis_ = options.progress_report_interval_millis;
  progressReporter_ = folly::make_unique<ProgressReporter>();
}

Sender::Sender(const std::string &destHost, const std::string &srcDir,
               const std::vector<int32_t> &ports,
               const std::vector<FileInfo> &srcFileInfo)
    : Sender(destHost, srcDir) {
  ports_ = ports;
  dirQueue_->setFileInfo(srcFileInfo);
}

Sender::~Sender() {
  if (!isTransferFinished()) {
    LOG(WARNING) << "Sender being deleted. Forcefully aborting the transfer";
    abort();
    finish();
  }
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

void Sender::setSenderId(const std::string &senderId) {
  WDT_CHECK(senderId.length() <= Protocol::kMaxTransferIdLength);
  senderId_ = senderId;
  LOG(INFO) << "sender id " << senderId_;
}

void Sender::setProtocolVersion(int protocolVersion) {
  WDT_CHECK(Protocol::negotiateProtocol(protocolVersion) == protocolVersion)
      << "Can not support wdt version " << protocolVersion;
  protocolVersion_ = protocolVersion;
  LOG(INFO) << "using wdt protocol version " << protocolVersion_;
}

const std::string &Sender::getSenderId() const {
  return senderId_;
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

void Sender::setProgressReporter(
    std::unique_ptr<ProgressReporter> &progressReporter) {
  progressReporter_ = std::move(progressReporter);
}

std::unique_ptr<TransferReport> Sender::getTransferReport() {
  size_t totalFileSize = dirQueue_->getTotalSize();
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

void Sender::fillThrottlerOptions(ThrottlerOptions &throttlerOptions) {
  VLOG(1) << "Configuring throttler options";
  const auto &options = WdtOptions::get();
  int numSockets = ports_.size();
  double avgRateBytesPerSec = options.avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = options.max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = options.throttler_bucket_limit * kMbToB;
  double perThreadAvgRateBytesPerSec = avgRateBytesPerSec / numSockets;
  double perThreadPeakRateBytesPerSec = peakRateBytesPerSec / numSockets;
  double perThreadBucketLimit = bucketLimitBytes / numSockets;
  if (avgRateBytesPerSec < 1.0 && avgRateBytesPerSec >= 0) {
    LOG(FATAL) << "Realistic average rate should be greater than 1.0 bytes/sec";
  }
  if (perThreadPeakRateBytesPerSec < perThreadAvgRateBytesPerSec &&
      perThreadPeakRateBytesPerSec >= 0) {
    LOG(WARNING) << "Per thread peak rate should be greater "
                 << "than per thread average rate. "
                 << "Making peak rate 1.2 times the average rate";
    perThreadPeakRateBytesPerSec =
        kPeakMultiplier * perThreadAvgRateBytesPerSec;
  }
  if (perThreadBucketLimit <= 0 && perThreadPeakRateBytesPerSec > 0) {
    perThreadBucketLimit =
        kTimeMultiplier * kBucketMultiplier * perThreadPeakRateBytesPerSec;
    LOG(INFO) << "Burst limit not specified but peak "
              << "rate is configured. Auto configuring to "
              << perThreadBucketLimit / kMbToB << " mbytes";
  }
  VLOG(1) << "Per thread (Avg Rate, Peak Rate) = "
          << "(" << perThreadAvgRateBytesPerSec << ", "
          << perThreadPeakRateBytesPerSec << ")";

  throttlerOptions.avgRateBytes = perThreadAvgRateBytesPerSec;
  throttlerOptions.maxRateBytes = perThreadPeakRateBytesPerSec;
  throttlerOptions.bucketLimitBytes = perThreadBucketLimit;
}

std::unique_ptr<TransferReport> Sender::finish() {
  const auto &options = WdtOptions::get();
  const bool twoPhases = options.two_phases;
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  double directoryTime;
  if (!twoPhases) {
    dirThread_.join();
  }
  directoryTime = dirQueue_->getDirectoryTime();
  for (int i = 0; i < ports_.size(); i++) {
    senderThreads_[i].join();
  }
  WDT_CHECK(numActiveThreads_ == 0);
  if (progressReportEnabled) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      transferFinished_ = true;
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
  size_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(endTime_ - startTime_);
  std::unique_ptr<TransferReport> transferReport =
      folly::make_unique<TransferReport>(
          transferredSourceStats, dirQueue_->getFailedSourceStats(),
          globalThreadStats_, dirQueue_->getFailedDirectories(), totalTime,
          totalFileSize, dirQueue_->getCount());
  if (progressReportEnabled) {
    progressReporter_->end(transferReport);
  }
  LOG(INFO) << "Total sender time = " << totalTime << " seconds ("
            << directoryTime << " dirTime)"
            << ". Transfer summary : " << *transferReport
            << "\nTotal sender throughput = "
            << transferReport->getThroughputMBps() << " Mbytes/sec ("
            << transferReport->getSummary().getEffectiveTotalBytes() /
                   (totalTime - directoryTime) / kMbToB
            << " Mbytes/sec pure transf rate)";
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
  const auto &options = WdtOptions::get();
  const bool twoPhases = options.two_phases;
  const size_t bufferSize = options.buffer_size;
  LOG(INFO) << "Client (sending) to " << destHost_ << ", Using ports [ "
            << ports_ << "]";
  startTime_ = Clock::now();
  dirThread_ = std::move(dirQueue_->buildQueueAsynchronously());
  bool progressReportEnabled =
      progressReporter_ && progressReportIntervalMillis_ > 0;
  if (twoPhases) {
    dirThread_.join();
  }
  fillThrottlerOptions(perThreadThrottlerOptions_);
  // WARNING: Do not MERGE the follwing two loops. ThreadTransferHistory keeps a
  // reference of TransferStats. And, any emplace operation on a vector
  // invalidates all its references
  for (int i = 0; i < ports_.size(); i++) {
    globalThreadStats_.emplace_back(true);
  }
  for (int i = 0; i < ports_.size(); i++) {
    transferHistories_.emplace_back(*dirQueue_, globalThreadStats_[i]);
  }
  numActiveThreads_ = ports_.size();
  transferFinished_ = false;
  for (int i = 0; i < ports_.size(); i++) {
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
  size_t sourceFailedAttempts = 0;
  size_t sourceDataBytes = 0;
  size_t sourceEffectiveDataBytes = 0;
  size_t sourceNumBlocks = 0;

  size_t threadFailedAttempts = 0;
  size_t threadDataBytes = 0;
  size_t threadEffectiveDataBytes = 0;
  size_t threadNumBlocks = 0;

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

std::unique_ptr<ClientSocket> Sender::makeSocket(const std::string &destHost,
                                                 int port) {
  return folly::make_unique<ClientSocket>(destHost,
                                          folly::to<std::string>(port));
}

std::unique_ptr<ClientSocket> Sender::connectToReceiver(const int port,
                                                        ErrorCode &errCode) {
  auto startTime = Clock::now();
  const auto &options = WdtOptions::get();
  int connectAttempts = 0;
  std::unique_ptr<ClientSocket> socket = makeSocket(destHost_, port);
  double retryInterval = options.sleep_millis;
  for (int i = 1; i <= options.max_retries; ++i) {
    ++connectAttempts;
    errCode = socket->connect();
    if (errCode == OK) {
      break;
    } else if (errCode == CONN_ERROR) {
      return nullptr;
    }
    if (i != options.max_retries) {
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
  ThreadTransferHistory &transferHistory = data.getTransferHistory();

  if (socket) {
    socket->close();
  }

  ErrorCode code;
  socket = connectToReceiver(port, code);
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
  size_t decodeOffset = 0;
  char *buf = data.buf_;
  ssize_t numRead = data.socket_->read(buf, Protocol::kMaxLocalCheckpoint);
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
  size_t off = 0;
  buf[off++] = Protocol::SETTINGS_CMD;

  bool success = Protocol::encodeSettings(
      protocolVersion_, buf, off, Protocol::kMaxSettings, readTimeoutMillis,
      writeTimeoutMillis, senderId_, options.enable_checksum);
  WDT_CHECK(success);
  ssize_t written = socket->write(buf, off);
  if (written != off) {
    LOG(ERROR) << "Socket write failure " << written << " " << off;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return CONNECT;
  }
  threadStats.addHeaderBytes(off);
  return SEND_BLOCKS;
}

Sender::SenderState Sender::sendBlocks(ThreadData &data) {
  VLOG(1) << "entered SEND_BLOCKS state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  DirectorySourceQueue &queue = data.queue_;
  auto &totalSizeSent = data.totalSizeSent_;

  if (protocolVersion_ >= Protocol::RECEIVER_PROGRESS_REPORT_VERSION &&
      !totalSizeSent && queue.fileDiscoveryFinished()) {
    return SEND_SIZE_CMD;
  }

  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source = queue.getNextSource(transferStatus);
  if (!source) {
    return SEND_DONE_CMD;
  }
  WDT_CHECK(!source->hasError());
  size_t totalBytes = threadStats.getTotalBytes(false);
  TransferStats transferStats = sendOneByteSource(
      data.socket_, data.throttler_, source, totalBytes, transferStatus);
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
    queue.returnToQueue(source);
    return CHECK_FOR_ABORT;
  }
  return SEND_BLOCKS;
}

Sender::SenderState Sender::sendSizeCmd(ThreadData &data) {
  VLOG(1) << "entered SEND_SIZE_CMD state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  auto &socket = data.socket_;
  auto &queue = data.queue_;
  auto &totalSizeSent = data.totalSizeSent_;
  size_t off = 0;
  buf[off++] = Protocol::SIZE_CMD;

  Protocol::encodeSize(buf, off, Protocol::kMaxSize, queue.getTotalSize());
  ssize_t written = socket->write(buf, off);
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
  auto &queue = data.queue_;
  size_t off = 0;
  buf[off++] = Protocol::DONE_CMD;

  auto pair = queue.getNumBlocksAndStatus();
  int64_t numBlocksDiscovered = pair.first;
  ErrorCode transferStatus = pair.second;
  buf[off++] = transferStatus;

  bool success =
      Protocol::encodeDone(buf, off, Protocol::kMaxDone, numBlocksDiscovered);
  WDT_CHECK(success);

  int toWrite = Protocol::kMinBufLength;
  ssize_t written = socket->write(buf, toWrite);
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
    LOG(ERROR) << "Unexpected result found while reading for abort";
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return END;
  }
  threadStats.addHeaderBytes(1);
  return PROCESS_ABORT_CMD;
}

Sender::SenderState Sender::readReceiverCmd(ThreadData &data) {
  VLOG(1) << "entered READ_RECEIVER_CMD state " << data.threadIndex_;
  TransferStats &threadStats = data.threadStats_;
  char *buf = data.buf_;
  ssize_t numRead = data.socket_->read(buf, 1);
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
  TransferStats &threadStats = data.threadStats_;
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
  LOG(INFO) << "entered PROCESS_ERR_CMD state " << data.threadIndex_;
  int port = ports_[data.threadIndex_];
  ThreadTransferHistory &transferHistory = data.getTransferHistory();
  TransferStats &threadStats = data.threadStats_;
  auto &transferHistories = data.transferHistories_;
  auto &socket = data.socket_;
  char *buf = data.buf_;

  auto toRead = sizeof(uint16_t);
  auto numRead = socket->read(buf, toRead);
  if (numRead != toRead) {
    LOG(ERROR) << "read unexpected " << toRead << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  uint16_t checkpointsLen = folly::loadUnaligned<uint16_t>(buf);
  checkpointsLen = folly::Endian::little(checkpointsLen);
  char checkpointBuf[checkpointsLen];
  numRead = socket->read(checkpointBuf, checkpointsLen);
  if (numRead != checkpointsLen) {
    LOG(ERROR) << "read unexpected " << checkpointsLen << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  std::vector<Checkpoint> checkpoints;
  size_t decodeOffset = 0;
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
  int toRead = 1 + sizeof(int32_t) + sizeof(int64_t);
  auto numRead = socket->read(buf, toRead);
  if (numRead != toRead) {
    // can not read checkpoint, but still must exit because of ABORT
    LOG(ERROR) << "Error while trying to read ABORT cmd " << numRead << " "
               << toRead;
    return END;
  }

  int offset = 0;
  int32_t protocolVersion = folly::loadUnaligned<int32_t>(buf + offset);
  protocolVersion = folly::Endian::little(protocolVersion);
  offset += sizeof(int32_t);
  ErrorCode remoteError = (ErrorCode)buf[offset++];
  threadStats.setRemoteErrorCode(remoteError);
  int64_t checkpoint = folly::loadUnaligned<int64_t>(buf + offset);
  checkpoint = folly::Endian::little(checkpoint);
  std::string failedFileName = transferHistory.getSourceId(checkpoint);
  LOG(WARNING) << "Received abort on " << data.threadIndex_
               << " remote protocol version " << protocolVersion
               << " remote error code " << errorCodeToStr(remoteError)
               << " file " << failedFileName << " checkpoint " << checkpoint;
  abort();
  return END;
}

void Sender::sendOne(int threadIndex) {
  std::vector<ThreadTransferHistory> &transferHistories = transferHistories_;
  TransferStats &threadStats = globalThreadStats_[threadIndex];
  DirectorySourceQueue &queue(*dirQueue_);
  Clock::time_point startTime = Clock::now();
  const auto &options = WdtOptions::get();
  int port = ports_[threadIndex];
  folly::ScopeGuard completionGuard = folly::makeGuard([&] {
    std::unique_lock<std::mutex> lock(mutex_);
    numActiveThreads_--;
    if (numActiveThreads_ == 0) {
      LOG(WARNING) << "Last thread finished "
                   << durationSeconds(Clock::now() - startTime_);
      endTime_ = Clock::now();
      transferFinished_ = true;
    }
  });
  std::unique_ptr<Throttler> throttler;
  double avgRateBytes = perThreadThrottlerOptions_.avgRateBytes;
  double maxRateBytes = perThreadThrottlerOptions_.maxRateBytes;
  double bucketLimitBytes = perThreadThrottlerOptions_.bucketLimitBytes;
  const bool doThrottling = (avgRateBytes > 0 || maxRateBytes > 0);
  if (doThrottling) {
    throttler = folly::make_unique<Throttler>(
        startTime, avgRateBytes, maxRateBytes, bucketLimitBytes,
        options.throttler_log_time_millis);
  } else {
    VLOG(1) << "No throttling in effect";
  }

  ThreadData threadData(threadIndex, queue, threadStats, transferHistories);
  if (doThrottling) {
    threadData.throttler_ = std::move(throttler);
  }

  SenderState state = CONNECT;
  while (state != END) {
    if (isAborted()) {
      LOG(ERROR) << "Transfer aborted " << port;
      threadStats.setErrorCode(ABORT);
      break;
    }
    state = (this->*stateMap_[state])(threadData);
  }

  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Got reply - all done for port :" << port << ". "
            << "Transfer stat : " << threadStats << " Total throughput = "
            << threadStats.getEffectiveTotalBytes() / totalTime / kMbToB
            << " Mbytes/sec";
  return;
}

void Sender::abort() {
  std::lock_guard<std::mutex> lock(mutex_);
  transferAborted_ = true;
}

bool Sender::isAborted() {
  std::lock_guard<std::mutex> lock(mutex_);
  return transferAborted_;
}

TransferStats Sender::sendOneByteSource(
    const std::unique_ptr<ClientSocket> &socket,
    const std::unique_ptr<Throttler> &throttler,
    const std::unique_ptr<ByteSource> &source, const size_t totalBytes,
    ErrorCode transferStatus) {
  TransferStats stats;
  auto &options = WdtOptions::get();
  char headerBuf[Protocol::kMaxHeader];
  size_t off = 0;
  headerBuf[off++] = Protocol::FILE_CMD;
  headerBuf[off++] = transferStatus;
  char *headerLenPtr = headerBuf + off;
  off += sizeof(uint16_t);
  const size_t expectedSize = source->getSize();
  size_t actualSize = 0;
  Protocol::encodeHeader(headerBuf, off, Protocol::kMaxHeader,
                         source->getIdentifier(), source->getSeqId(),
                         expectedSize, source->getOffset(),
                         source->getTotalSize());
  uint16_t littleEndianOff = folly::Endian::little((uint16_t)off);
  folly::storeUnaligned<uint16_t>(headerLenPtr, littleEndianOff);
  ssize_t written = socket->write(headerBuf, off);
  if (written != off) {
    PLOG(ERROR) << "Write error/mismatch " << written << " " << off
                << ". fd = " << socket->getFd()
                << ". port = " << socket->getPort();
    stats.setErrorCode(SOCKET_WRITE_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  stats.addHeaderBytes(written);
  VLOG(3) << "Sent " << written << " on " << socket->getFd() << " : "
          << folly::humanify(std::string(headerBuf, off));
  uint32_t checksum = 0;
  while (!source->finished()) {
    size_t size;
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
    if (throttler) {
      /**
       * If throttling is enabled we call limit(totalBytes) which
       * used both the methods of throttling peak and average.
       * Always call it with totalBytes written till now, throttler
       * will do the rest. Total bytes includes header and the data bytes.
       * The throttler was constructed at the time when the header
       * was being written and it is okay to start throttling with the
       * next expected write.
       */
      throttler->limit(totalBytes + stats.getTotalBytes() + size);
    }
    do {
      ssize_t w = socket->write(buffer + written, size - written);
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
    if (stat(source->getFullPath().c_str(), &fileStat) != 0) {
      PLOG(ERROR) << "stat failed on path " << source->getFullPath();
    } else {
      LOG(WARNING) << "file " << source->getIdentifier() << " previous size "
                   << source->getTotalSize() << " current size "
                   << fileStat.st_size;
    }
    stats.setErrorCode(BYTE_SOURCE_READ_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  if (protocolVersion_ >= Protocol::CHECKSUM_VERSION &&
      options.enable_checksum) {
    off = 0;
    headerBuf[off++] = Protocol::FOOTER_CMD;
    bool success =
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
  int throughputUpdateInterval = WdtOptions::get().throughput_update_interval;
  WDT_CHECK(throughputUpdateInterval >= 0);

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
