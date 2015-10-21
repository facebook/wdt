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
#include "SenderThread.h"
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
void Sender::endCurTransfer() {
  endTime_ = Clock::now();
  LOG(INFO) << "Last thread finished " << durationSeconds(endTime_ - startTime_)
            << " for transfer id " << transferId_;
  transferFinished_ = true;
  if (throttler_) {
    throttler_->deRegisterTransfer();
  }
}

void Sender::startNewTransfer() {
  if (throttler_) {
    throttler_->registerTransfer();
  }
  LOG(INFO) << "Starting a new transfer " << transferId_ << " to " << destHost_;
}

Sender::Sender(const std::string &destHost, const std::string &srcDir)
    : queueAbortChecker_(this), destHost_(destHost) {
  LOG(INFO) << "WDT Sender " << Protocol::getFullVersion();
  srcDir_ = srcDir;
  transferFinished_ = true;
  const auto &options = WdtOptions::get();
  int port = options.start_port;
  int numSockets = options.num_ports;
  for (int i = 0; i < numSockets; i++) {
    ports_.push_back(port + i);
  }
  dirQueue_.reset(new DirectorySourceQueue(srcDir_, &queueAbortChecker_));
  VLOG(3) << "Configuring the  directory queue";
  dirQueue_->setIncludePattern(options.include_regex);
  dirQueue_->setExcludePattern(options.exclude_regex);
  dirQueue_->setPruneDirPattern(options.prune_dir_regex);
  dirQueue_->setFollowSymlinks(options.follow_symlinks);
  dirQueue_->setBlockSizeMbytes(options.block_size_mbytes);
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
  transferHistoryController_ =
      folly::make_unique<TransferHistoryController>(*dirQueue_);
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

ProtoNegotiationStatus Sender::getNegotiationStatus() {
  return protoNegotiationStatus_;
}

std::vector<int> Sender::getNegotiatedProtocols() const {
  std::vector<int> ret;
  for (const auto &senderThread : senderThreads_) {
    ret.push_back(senderThread->getNegotiatedProtocol());
  }
  return ret;
}

void Sender::setProtoNegotiationStatus(ProtoNegotiationStatus status) {
  protoNegotiationStatus_ = status;
}

bool Sender::isSendFileChunks() const {
  return (downloadResumptionEnabled_ &&
          protocolVersion_ >= Protocol::DOWNLOAD_RESUMPTION_VERSION);
}

bool Sender::isFileChunksReceived() const {
  std::lock_guard<std::mutex> lock(mutex_);
  return fileChunksReceived_;
}

void Sender::setFileChunksInfo(
    std::vector<FileChunksInfo> &fileChunksInfoList) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (fileChunksReceived_) {
    LOG(WARNING) << "File chunks list received multiple times";
    return;
  }
  dirQueue_->setPreviouslyReceivedChunks(fileChunksInfoList);
  fileChunksReceived_ = true;
}

const std::string &Sender::getDestination() const {
  return destHost_;
}

std::unique_ptr<TransferReport> Sender::getTransferReport() {
  int64_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(Clock::now() - startTime_);
  auto globalStats = getGlobalTransferStats();
  std::unique_ptr<TransferReport> transferReport =
      folly::make_unique<TransferReport>(std::move(globalStats), totalTime,
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

TransferStats Sender::getGlobalTransferStats() const {
  TransferStats globalStats;
  for (const auto &thread : senderThreads_) {
    globalStats += thread->getTransferStats();
  }
  return globalStats;
}

ErrorCode Sender::verifyVersionMismatchStats() const {
  for (const auto &senderThread : senderThreads_) {
    const auto &threadStats = senderThread->getTransferStats();
    if (threadStats.getErrorCode() == OK) {
      LOG(ERROR) << "Found a thread that completed transfer successfully "
                 << "despite version mismatch. " << senderThread->getPort();
      return ERROR;
    }
  }
  return OK;
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
  for (auto &senderThread : senderThreads_) {
    senderThread->finish();
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
  std::vector<TransferStats> threadStats;
  for (auto &senderThread : senderThreads_) {
    threadStats.push_back(std::move(senderThread->moveStats()));
  }
  bool allSourcesAcked = false;
  for (auto &senderThread : senderThreads_) {
    auto &stats = senderThread->getTransferStats();
    if (stats.getErrorCode() == OK) {
      // at least one thread finished correctly
      // that means all transferred sources are acked
      allSourcesAcked = true;
      break;
    }
  }

  std::vector<TransferStats> transferredSourceStats;
  for (auto port : ports_) {
    auto &transferHistory =
        transferHistoryController_->getTransferHistory(port);
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
                          dirQueue_->getFailedSourceStats());
  }
  int64_t totalFileSize = dirQueue_->getTotalSize();
  double totalTime = durationSeconds(endTime_ - startTime_);
  std::unique_ptr<TransferReport> transferReport =
      folly::make_unique<TransferReport>(
          transferredSourceStats, dirQueue_->getFailedSourceStats(),
          threadStats, dirQueue_->getFailedDirectories(), totalTime,
          totalFileSize, dirQueue_->getCount());

  if (progressReportEnabled) {
    progressReporter_->end(transferReport);
  }
  if (options.enable_perf_stat_collection) {
    PerfStatReport report;
    for (auto &senderThread : senderThreads_) {
      report += senderThread->getPerfReport();
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
            << " Mbytes/sec pure transfer rate)";
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
  threadsController_ = new ThreadsController(ports_.size());
  threadsController_->setNumBarriers(SenderThread::NUM_BARRIERS);
  threadsController_->setNumFunnels(SenderThread::NUM_FUNNELS);
  threadsController_->setNumConditions(SenderThread::NUM_CONDITIONS);
  senderThreads_ = threadsController_->makeThreads<Sender, SenderThread>(
      this, ports_.size(), ports_);
  for (auto &senderThread : senderThreads_) {
    senderThread->startThread();
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
    const std::vector<TransferStats> &failedSourceStats) {
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
  for (const auto &senderThread : senderThreads_) {
    const auto &stat = senderThread->getTransferStats();
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

std::unique_ptr<ClientSocket> Sender::connectToReceiver(
    const int port, IAbortChecker const *abortChecker, ErrorCode &errCode) {
  auto startTime = Clock::now();
  const auto &options = WdtOptions::get();
  int connectAttempts = 0;
  std::unique_ptr<ClientSocket> socket;
  std::string portStr = folly::to<std::string>(port);
  if (!socketCreator_) {
    // socket creator not set, creating ClientSocket
    socket = folly::make_unique<ClientSocket>(destHost_, portStr, abortChecker);
  } else {
    socket = socketCreator_(destHost_, portStr, abortChecker);
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
  blockDetails.dataSize = expectedSize;
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
                << ". file = " << metadata.relPath
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
                    << ". file = " << metadata.relPath
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
