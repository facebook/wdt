#include "Receiver.h"
#include "ServerSocket.h"
#include "FileWriter.h"

#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <folly/ScopeGuard.h>
#include <folly/Bits.h>
#include <folly/Checksum.h>

#include <fcntl.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
using std::vector;
namespace facebook {
namespace wdt {

const static int kTimeoutBufferMillis = 1000;
const static int kWaitTinmeoutFactor = 5;

size_t readAtLeast(ServerSocket &s, char *buf, size_t max, ssize_t atLeast,
                   ssize_t len) {
  VLOG(4) << "readAtLeast len " << len << " max " << max << " atLeast "
          << atLeast << " from " << s.getFd();
  CHECK(len >= 0) << "negative len " << len;
  CHECK(atLeast >= 0) << "negative atLeast " << atLeast;
  int count = 0;
  while (len < atLeast) {
    // because we want to process data as soon as it arrives, tryFull option for
    // read is false
    ssize_t n = s.read(buf + len, max - len, false);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << s.getPort() << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      VLOG(2) << "Eof on " << s.getPort() << " after " << count << " read "
              << len;
      return len;
    }
    len += n;
    count++;
  }
  VLOG(3) << "took " << count << " read to get " << len << " from "
          << s.getFd();
  return len;
}

size_t readAtMost(ServerSocket &s, char *buf, size_t max, size_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  VLOG(3) << "readAtMost target " << target;
  // because we want to process data as soon as it arrives, tryFull option for
  // read is false
  ssize_t n = s.read(buf, target, false);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << s.getPort() << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(WARNING) << "Eof on " << s.getFd();
    return n;
  }
  VLOG(3) << "readAtMost " << n << " / " << atMost << " from " << s.getFd();
  return n;
}

const Receiver::StateFunction Receiver::stateMap_[] = {
    &Receiver::listen, &Receiver::acceptFirstConnection,
    &Receiver::acceptWithTimeout, &Receiver::sendLocalCheckpoint,
    &Receiver::readNextCmd, &Receiver::processFileCmd,
    &Receiver::processExitCmd, &Receiver::processSettingsCmd,
    &Receiver::processDoneCmd, &Receiver::processSizeCmd,
    &Receiver::sendGlobalCheckpoint, &Receiver::sendDoneCmd,
    &Receiver::sendAbortCmd, &Receiver::waitForFinishOrNewCheckpoint,
    &Receiver::waitForFinishWithThreadError};

void Receiver::addCheckpoint(Checkpoint checkpoint) {
  LOG(INFO) << "Adding global checkpoint " << checkpoint.first << " "
            << checkpoint.second;
  checkpoints_.emplace_back(checkpoint);
  conditionAllFinished_.notify_all();
}

std::vector<Checkpoint> Receiver::getNewCheckpoints(int startIndex) {
  std::vector<Checkpoint> checkpoints;
  for (int i = startIndex; i < checkpoints_.size(); i++) {
    checkpoints.emplace_back(checkpoints_[i]);
  }
  return checkpoints;
}

Receiver::Receiver(int port, int numSockets) {
  isJoinable_ = false;
  transferFinished_ = true;
  const auto &options = WdtOptions::get();
  if (port == 0) {
    LOG(INFO) << "Auto configure mode. Selecting ephemeral ports";
    for (int i = 0; i < numSockets; i++) {
      ServerSocket socket(0, options.backlog, &abortCheckerCallback_);
      WDT_CHECK(socket.listen() == OK);
      threadServerSockets_.push_back(std::move(socket));
    }
    return;
  }
  for (int i = 0; i < numSockets; i++) {
    threadServerSockets_.emplace_back(port + i, options.backlog,
                                      &abortCheckerCallback_);
  }
}

Receiver::Receiver(int port, int numSockets, const std::string &destDir)
    : Receiver(port, numSockets) {
  this->destDir_ = destDir;
}

int32_t Receiver::registerPorts(bool stopOnFailure) {
  int32_t numSuccess = 0;
  for (ServerSocket &socket : threadServerSockets_) {
    int max_retries = WdtOptions::get().max_retries;
    for (int retries = 0; retries < max_retries; retries++) {
      if (socket.listen() == OK) {
        break;
      }
    }
    if (socket.listen() == OK) {
      numSuccess++;
      continue;
    }
    if (stopOnFailure) {
      break;
    }
  }
  return numSuccess;
}

void Receiver::setDir(const std::string &destDir) {
  destDir_ = destDir;
}

const std::string &Receiver::getDir() {
  return destDir_;
}

const std::string &Receiver::getReceiverId() {
  return receiverId_;
}

void Receiver::setReceiverId(const std::string &receiverId) {
  WDT_CHECK(receiverId.length() <= Protocol::kMaxTransferIdLength);
  receiverId_ = receiverId;
  LOG(INFO) << "receiver id " << receiverId_;
}

void Receiver::setProtocolVersion(int protocolVersion) {
  WDT_CHECK(Protocol::negotiateProtocol(protocolVersion) == protocolVersion)
      << "Can not support wdt version " << protocolVersion;
  protocolVersion_ = protocolVersion;
  LOG(INFO) << "using wdt protocol version " << protocolVersion_;
}

Receiver::~Receiver() {
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an ongoing transfer and the destructor"
                 << " is being called. Trying to finish the transfer";
    abort();
  }
  finish();
}

vector<int32_t> Receiver::getPorts() {
  vector<int32_t> ports;
  for (const auto &socket : threadServerSockets_) {
    ports.push_back(socket.getPort());
  }
  return ports;
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
    LOG(INFO) << "Threads have already been joined. Returning the "
              << "transfer report";
    return getTransferReport();
  }
  const auto &options = WdtOptions::get();
  if (!isJoinable_) {
    LOG(WARNING) << "The receiver is not joinable. The threads will never"
                 << " finish and this method will never return";
  }
  for (int i = 0; i < threadServerSockets_.size(); i++) {
    receiverThreads_[i].join();
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

  if (progressReporter_ && totalSenderBytes_ >= 0) {
    report->setTotalFileSize(totalSenderBytes_);
    report->setTotalTime(durationSeconds(Clock::now() - startTime_));
    progressReporter_->end(report);
  }
  if (options.enable_perf_stat_collection) {
    PerfStatReport report;
    for (auto &perfReport : perfReports_) {
      report += perfReport;
    }
    LOG(INFO) << report;
  }

  LOG(WARNING) << "WDT receiver's transfer has been finished";
  LOG(INFO) << *report;
  receiverThreads_.clear();
  threadServerSockets_.clear();
  threadStats_.clear();
  areThreadsJoined_ = true;
  return report;
}

std::unique_ptr<TransferReport> Receiver::getTransferReport() {
  std::unique_ptr<TransferReport> report =
      folly::make_unique<TransferReport>(threadStats_);
  const TransferStats &summary = report->getSummary();

  if (numBlocksSend_ == -1 || numBlocksSend_ != summary.getNumBlocks()) {
    // either none of the threads finished properly or not all of the blocks
    // were transferred
    report->setErrorCode(ERROR);
  } else {
    report->setErrorCode(OK);
  }
  return report;
}

void Receiver::configureThrottler() {
  WDT_CHECK(!throttler_);
  VLOG(1) << "Configuring throttler options";
  const auto &options = WdtOptions::get();
  double avgRateBytesPerSec = options.avg_mbytes_per_sec * kMbToB;
  double peakRateBytesPerSec = options.max_mbytes_per_sec * kMbToB;
  double bucketLimitBytes = options.throttler_bucket_limit * kMbToB;
  throttler_ = Throttler::makeThrottler(avgRateBytesPerSec, peakRateBytesPerSec,
                                        bucketLimitBytes,
                                        options.throttler_log_time_millis);
  if (throttler_) {
    LOG(INFO) << "Enabling throttling " << *throttler_;
  } else {
    LOG(INFO) << "Throttling not enabled";
  }
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
  int throughputUpdateInterval = WdtOptions::get().throughput_update_interval;
  if (progressReportIntervalMillis <= 0 || throughputUpdateInterval < 0 ||
      !isJoinable_) {
    return;
  }

  int64_t lastEffectiveBytes = 0;
  std::chrono::time_point<Clock> lastUpdateTime = Clock::now();
  int intervalsSinceLastUpdate = 0;
  double currentThroughput = 0;

  LOG(INFO) << "Progress reporter updating every "
            << progressReportIntervalMillis << " ms";
  auto waitingTime = std::chrono::milliseconds(progressReportIntervalMillis);
  int64_t totalSenderBytes;
  while (true) {
    {
      std::unique_lock<std::mutex> lock(mutex_);
      conditionRecvFinished_.wait_for(lock, waitingTime);
      if (transferFinished_ || wasAbortRequested()) {
        break;
      }
      if (totalSenderBytes_ == -1) {
        continue;
      }
      totalSenderBytes = totalSenderBytes_;
    }
    double totalTime = durationSeconds(Clock::now() - startTime_);
    auto transferReport = folly::make_unique<TransferReport>(
        threadStats_, totalTime, totalSenderBytes);
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
  if (hasPendingTransfer()) {
    LOG(WARNING) << "There is an existing transfer in progress on this object";
  }
  areThreadsJoined_ = false;
  numActiveThreads_ = threadServerSockets_.size();
  LOG(INFO) << "Starting (receiving) server on ports [ " << getPorts()
            << "] Target dir : " << destDir_;
  markTransferFinished(false);
  const auto &options = WdtOptions::get();
  size_t bufferSize = options.buffer_size;
  if (bufferSize < Protocol::kMaxHeader) {
    // round up to even k
    bufferSize = 2 * 1024 * ((Protocol::kMaxHeader - 1) / (2 * 1024) + 1);
    LOG(INFO) << "Specified -buffer_size " << options.buffer_size
              << " smaller than " << Protocol::kMaxHeader << " using "
              << bufferSize << " instead";
  }
  fileCreator_.reset(new FileCreator(destDir_, threadServerSockets_.size()));
  perfReports_.resize(threadServerSockets_.size());
  for (int i = 0; i < threadServerSockets_.size(); i++) {
    threadStats_.emplace_back(true);
  }
  if (!throttler_) {
    configureThrottler();
  } else {
    LOG(INFO) << "Throttler set externally. Throttler : " << *throttler_;
  }

  for (int i = 0; i < threadServerSockets_.size(); i++) {
    receiverThreads_.emplace_back(&Receiver::receiveOne, this, i,
                                  std::ref(threadServerSockets_[i]), bufferSize,
                                  std::ref(threadStats_[i]));
  }
  if (isJoinable_) {
    if (progressReporter_) {
      progressReporter_->start();
    }
    std::thread trackerThread(&Receiver::progressTracker, this);
    progressTrackerThread_ = std::move(trackerThread);
  }
}

bool Receiver::areAllThreadsFinished(bool checkpointAdded) {
  bool finished = (failedThreadCount_ + waitingThreadCount_ +
                   waitingWithErrorThreadCount_) == threadServerSockets_.size();
  if (checkpointAdded) {
    // The thread has added a global checkpoint. So,
    // even if all the threads are waiting, the session does no end. However,
    // if all the threads are waiting with an error, then we must end the
    // session. because none of the waiting threads can send the global
    // checkpoint back to the sender
    finished &= (waitingThreadCount_ == 0);
  }
  return finished;
}

void Receiver::endCurGlobalSession() {
  WDT_CHECK(transferFinishedCount_ + 1 == transferStartedCount_);
  LOG(INFO) << "Received done for all threads. Transfer session "
            << transferStartedCount_ << " finished";
  if (throttler_) {
    throttler_->deRegisterTransfer();
  }
  transferFinishedCount_++;
  waitingThreadCount_ = 0;
  waitingWithErrorThreadCount_ = 0;
  checkpoints_.clear();
  fileCreator_->clearAllocationMap();
  conditionAllFinished_.notify_all();
}

void Receiver::incrFailedThreadCountAndCheckForSessionEnd(ThreadData &data) {
  std::unique_lock<std::mutex> lock(mutex_);
  failedThreadCount_++;
  // a new session may not have started when a thread failed
  if (hasNewSessionStarted(data) && areAllThreadsFinished(false)) {
    endCurGlobalSession();
  }
}

bool Receiver::hasNewSessionStarted(ThreadData &data) {
  bool started = transferStartedCount_ > data.transferStartedCount_;
  if (started) {
    WDT_CHECK(transferStartedCount_ == data.transferStartedCount_ + 1);
  }
  return started;
}

void Receiver::startNewGlobalSession() {
  WDT_CHECK(transferStartedCount_ == transferFinishedCount_);
  if (throttler_) {
    // If throttler is configured/set then register this session
    // in the throttler. This is guranteed to work in either of the
    // modes long running or not. We will de register from the throttler
    // when the current session ends
    throttler_->registerTransfer();
  }
  transferStartedCount_++;
  startTime_ = Clock::now();
  LOG(INFO) << "New transfer started " << transferStartedCount_;
}

bool Receiver::hasCurSessionFinished(ThreadData &data) {
  return transferFinishedCount_ > data.transferFinishedCount_;
}

void Receiver::startNewThreadSession(ThreadData &data) {
  WDT_CHECK(data.transferStartedCount_ == data.transferFinishedCount_);
  data.transferStartedCount_++;
}

void Receiver::endCurThreadSession(ThreadData &data) {
  WDT_CHECK(data.transferStartedCount_ == data.transferFinishedCount_ + 1);
  data.transferFinishedCount_++;
}

/***LISTEN STATE***/
Receiver::ReceiverState Receiver::listen(ThreadData &data) {
  VLOG(1) << "entered LISTEN state " << data.threadIndex_;
  const auto &options = WdtOptions::get();
  const bool doActualWrites = !options.skip_writes;
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;

  int32_t port = socket.getPort();
  VLOG(1) << "Server Thread for port " << port << " with backlog "
          << socket.getBackLog() << " on " << destDir_
          << " writes= " << doActualWrites;
  for (int i = 1; i < options.max_retries; ++i) {
    ErrorCode code = socket.listen();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      threadStats.setErrorCode(code);
      incrFailedThreadCountAndCheckForSessionEnd(data);
      return FAILED;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    /* sleep override */
    usleep(options.sleep_millis * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (socket.listen() != OK) {
    LOG(ERROR) << "Unable to listen/bind despite retries";
    threadStats.setErrorCode(CONN_ERROR);
    incrFailedThreadCountAndCheckForSessionEnd(data);
    return FAILED;
  }
  return ACCEPT_FIRST_CONNECTION;
}

/***ACCEPT_FIRST_CONNECTION***/
Receiver::ReceiverState Receiver::acceptFirstConnection(ThreadData &data) {
  VLOG(1) << "entered ACCEPT_FIRST_CONNECTION state " << data.threadIndex_;
  const auto &options = WdtOptions::get();
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;

  data.reset();
  socket.closeCurrentConnection();
  auto timeout = options.accept_timeout_millis;
  int acceptAttempts = 0;
  while (true) {
    {
      std::lock_guard<std::mutex> lock(mutex_);
      if (hasNewSessionStarted(data)) {
        startNewThreadSession(data);
        return ACCEPT_WITH_TIMEOUT;
      }
    }
    if (isJoinable_ && acceptAttempts == options.max_accept_retries) {
      LOG(ERROR) << "unable to accept after " << acceptAttempts << " attempts";
      threadStats.setErrorCode(CONN_ERROR);
      incrFailedThreadCountAndCheckForSessionEnd(data);
      return FAILED;
    }

    if (wasAbortRequested()) {
      LOG(ERROR) << "Thread marked to abort while trying to accept first"
                 << " connection. Num attempts " << acceptAttempts;
      // Even though there is a transition FAILED here
      // wasAbortRequested() is going to be checked again in the receiveOne.
      // So this is pretty much irrelavant
      return FAILED;
    }

    ErrorCode code = socket.acceptNextConnection(timeout);
    if (code == OK) {
      break;
    }
    acceptAttempts++;
  }

  std::lock_guard<std::mutex> lock(mutex_);
  if (!hasNewSessionStarted(data)) {
    // this thread has the first connection
    startNewGlobalSession();
  }
  startNewThreadSession(data);
  return READ_NEXT_CMD;
}

/***ACCEPT_WITH_TIMEOUT STATE***/
Receiver::ReceiverState Receiver::acceptWithTimeout(ThreadData &data) {
  LOG(INFO) << "entered ACCEPT_WITH_TIMEOUT state " << data.threadIndex_;
  const auto &options = WdtOptions::get();
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;
  auto &senderReadTimeout = data.senderReadTimeout_;
  auto &senderWriteTimeout = data.senderWriteTimeout_;
  auto &doneSendFailure = data.doneSendFailure_;
  socket.closeCurrentConnection();

  auto timeout = options.accept_window_millis;
  if (senderReadTimeout > 0) {
    // transfer is in progress and we have alreay got sender settings
    timeout =
        std::max(senderReadTimeout, senderWriteTimeout) + kTimeoutBufferMillis;
  }

  ErrorCode code = socket.acceptNextConnection(timeout);
  if (code != OK) {
    LOG(ERROR) << "accept() failed with timeout " << timeout;
    threadStats.setErrorCode(code);
    if (doneSendFailure) {
      // if SEND_DONE_CMD state had already been reached, we do not need to
      // wait for other threads to end
      return END;
    }
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  if (doneSendFailure) {
    // no need to reset any session variables in this case
    return SEND_LOCAL_CHECKPOINT;
  }

  data.numRead_ = data.off_ = 0;
  data.pendingCheckpointIndex_ = data.checkpointIndex_;
  ReceiverState nextState = READ_NEXT_CMD;
  if (threadStats.getErrorCode() != OK) {
    nextState = SEND_LOCAL_CHECKPOINT;
  }
  // reset thread status
  threadStats.setErrorCode(OK);
  return nextState;
}

/***SEND_LOCAL_CHECKPOINT STATE***/
Receiver::ReceiverState Receiver::sendLocalCheckpoint(ThreadData &data) {
  LOG(INFO) << "entered SEND_LOCAL_CHECKPOINT state " << data.threadIndex_;
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;
  auto &doneSendFailure = data.doneSendFailure_;
  char *buf = data.getBuf();

  // in case SEND_DONE failed, a special checkpoint(-1) is sent to signal this
  // condition
  auto checkpoint = doneSendFailure ? -1 : threadStats.getNumBlocks();
  std::vector<Checkpoint> checkpoints;
  checkpoints.emplace_back(threadServerSockets_[data.threadIndex_].getPort(),
                           checkpoint);
  size_t off = 0;
  bool success = Protocol::encodeCheckpoints(
      buf, off, Protocol::kMaxLocalCheckpoint, checkpoints);
  WDT_CHECK(success);
  auto written = socket.write(buf, Protocol::kMaxLocalCheckpoint);
  if (written != Protocol::kMaxLocalCheckpoint) {
    LOG(ERROR) << "unable to write local checkpoint. write mismatch "
               << Protocol::kMaxLocalCheckpoint << " " << written;
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  threadStats.addHeaderBytes(Protocol::kMaxLocalCheckpoint);
  if (doneSendFailure) {
    return SEND_DONE_CMD;
  }
  return READ_NEXT_CMD;
}

/***READ_NEXT_CMD***/
Receiver::ReceiverState Receiver::readNextCmd(ThreadData &data) {
  VLOG(1) << "entered READ_NEXT_CMD state " << data.threadIndex_;
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;
  char *buf = data.getBuf();
  auto &numRead = data.numRead_;
  auto &off = data.off_;
  auto &oldOffset = data.oldOffset_;
  auto bufferSize = data.bufferSize_;

  oldOffset = off;
  numRead = readAtLeast(socket, buf + off, bufferSize - off,
                        Protocol::kMinBufLength, numRead);
  if (numRead <= 0) {
    LOG(ERROR) << "socket read failure " << Protocol::kMinBufLength << " "
               << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[off++];
  if (cmd == Protocol::EXIT_CMD) {
    return PROCESS_EXIT_CMD;
  }
  if (cmd == Protocol::DONE_CMD) {
    return PROCESS_DONE_CMD;
  }
  if (cmd == Protocol::FILE_CMD) {
    return PROCESS_FILE_CMD;
  }
  if (cmd == Protocol::SETTINGS_CMD) {
    return PROCESS_SETTINGS_CMD;
  }
  if (cmd == Protocol::SIZE_CMD) {
    return PROCESS_SIZE_CMD;
  }
  LOG(ERROR) << "received an unknown cmd";
  threadStats.setErrorCode(PROTOCOL_ERROR);
  return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
}

/***PROCESS_EXIT_CMD STATE***/
Receiver::ReceiverState Receiver::processExitCmd(ThreadData &data) {
  LOG(INFO) << "entered PROCESS_EXIT_CMD state " << data.threadIndex_;
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;

  if (data.numRead_ != 1) {
    LOG(ERROR) << "Unexpected state for exit command. probably junk "
                  "content. ignoring...";
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  LOG(ERROR) << "Got exit command in port " << socket.getPort() << " - exiting";
  exit(0);
}

/***PROCESS_SETTINGS_CMD***/
Receiver::ReceiverState Receiver::processSettingsCmd(ThreadData &data) {
  VLOG(1) << "entered PROCESS_SETTINGS_CMD state " << data.threadIndex_;
  char *buf = data.getBuf();
  auto &off = data.off_;
  auto &oldOffset = data.oldOffset_;
  auto &numRead = data.numRead_;
  auto &senderReadTimeout = data.senderReadTimeout_;
  auto &senderWriteTimeout = data.senderWriteTimeout_;
  auto &threadStats = data.threadStats_;
  auto &enableChecksum = data.enableChecksum_;
  int32_t senderProtocolVersion;
  std::string senderId;
  bool success = Protocol::decodeSettings(
      protocolVersion_, buf, off, oldOffset + Protocol::kMaxSettings,
      senderProtocolVersion, senderReadTimeout, senderWriteTimeout, senderId,
      enableChecksum);
  if (!success) {
    LOG(ERROR) << "Unable to decode settings cmd";
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  if (senderProtocolVersion != protocolVersion_) {
    LOG(ERROR) << "Receiver and sender protocol version mismatch "
               << senderProtocolVersion << " " << protocolVersion_;
    threadStats.setErrorCode(VERSION_MISMATCH);
    return SEND_ABORT_CMD;
  }
  if (receiverId_ != senderId) {
    LOG(ERROR) << "Receiver and sender id mismatch " << senderId << " "
               << receiverId_;
    threadStats.setErrorCode(ID_MISMATCH);
    return SEND_ABORT_CMD;
  }
  auto msgLen = off - oldOffset;
  numRead -= msgLen;
  return READ_NEXT_CMD;
}

/***PROCESS_FILE_CMD***/
Receiver::ReceiverState Receiver::processFileCmd(ThreadData &data) {
  VLOG(1) << "entered PROCESS_FILE_CMD state " << data.threadIndex_;
  auto &socket = data.socket_;
  auto &threadIndex = data.threadIndex_;
  auto &threadStats = data.threadStats_;
  char *buf = data.getBuf();
  auto &numRead = data.numRead_;
  auto &off = data.off_;
  auto &oldOffset = data.oldOffset_;
  auto bufferSize = data.bufferSize_;
  auto &checkpointIndex = data.checkpointIndex_;
  auto &pendingCheckpointIndex = data.pendingCheckpointIndex_;
  auto &enableChecksum = data.enableChecksum_;
  std::string id;
  uint64_t seqId;
  int64_t dataSize;
  int64_t offset;
  int64_t fileSize;

  auto guard = folly::makeGuard([&socket, &threadStats] {
    if (threadStats.getErrorCode() != OK) {
      threadStats.incrFailedAttempts();
    }
  });

  ErrorCode transferStatus = (ErrorCode)buf[off++];
  if (transferStatus != OK) {
    // TODO: use this status information to implement fail fast mode
    VLOG(1) << "sender entered into error state "
            << errorCodeToStr(transferStatus);
  }
  uint16_t headerLen = folly::loadUnaligned<uint16_t>(buf + off);
  headerLen = folly::Endian::little(headerLen);
  VLOG(2) << "header len " << headerLen;

  if (headerLen > numRead) {
    size_t end = oldOffset + numRead;
    numRead =
        readAtLeast(socket, buf + end, bufferSize - end, headerLen, numRead);
  }
  if (numRead < headerLen) {
    LOG(ERROR) << "unable to read full header " << headerLen << " " << numRead;
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  off += sizeof(uint16_t);
  bool success = Protocol::decodeHeader(buf, off, numRead + oldOffset, id,
                                        seqId, dataSize, offset, fileSize);
  ssize_t headerBytes = off - oldOffset;
  // transferred header length must match decoded header length
  WDT_CHECK(headerLen == headerBytes);
  threadStats.addHeaderBytes(headerBytes);
  if (!success) {
    LOG(ERROR) << "Error decoding at"
               << " ooff:" << oldOffset << " off: " << off
               << " numRead: " << numRead;
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  // received a well formed file cmd, apply the pending checkpoint update
  checkpointIndex = pendingCheckpointIndex;
  VLOG(1) << "Read id:" << id << " size:" << dataSize << " ooff:" << oldOffset
          << " off: " << off << " numRead: " << numRead;

  FileWriter writer(threadIndex, id, seqId, fileSize, offset, dataSize,
                    fileCreator_.get());

  if (writer.open() != OK) {
    threadStats.setErrorCode(FILE_WRITE_ERROR);
    return SEND_ABORT_CMD;
  }
  uint32_t checksum = 0;
  ssize_t remainingData = numRead + oldOffset - off;
  ssize_t toWrite = remainingData;
  if (remainingData >= dataSize) {
    toWrite = dataSize;
  }
  threadStats.addDataBytes(toWrite);
  if (enableChecksum) {
    checksum = folly::crc32c((const uint8_t *)(buf + off), toWrite, checksum);
  }
  if (throttler_) {
    // We might be reading more than we require for this file but
    // throttling should make sense for any additional bytes received
    // on the network
    throttler_->limit(toWrite + headerBytes);
  }
  ErrorCode code = writer.write(buf + off, toWrite);
  if (code != OK) {
    threadStats.setErrorCode(code);
    return SEND_ABORT_CMD;
  }
  off += toWrite;
  remainingData -= toWrite;
  // also means no leftOver so it's ok we use buf from start
  while (writer.getTotalWritten() < dataSize) {
    if (wasAbortRequested()) {
      LOG(ERROR) << "Thread marked for abort while processing a file."
                 << " port : " << socket.getPort();
      return FAILED;
    }
    int64_t nres = readAtMost(socket, buf, bufferSize,
                              dataSize - writer.getTotalWritten());
    if (throttler_) {
      // We only know how much we have read after we are done calling
      // readAtMost. Call throttler with the bytes read off the wire.
      throttler_->limit(nres);
    }
    if (nres <= 0) {
      break;
    }
    threadStats.addDataBytes(nres);
    if (enableChecksum) {
      checksum = folly::crc32c((const uint8_t *)buf, nres, checksum);
    }
    code = writer.write(buf, nres);
    if (code != OK) {
      threadStats.setErrorCode(code);
      return SEND_ABORT_CMD;
    }
  }
  if (writer.getTotalWritten() != dataSize) {
    // This can only happen if there are transmission errors
    // Write errors to disk are already taken care of above
    LOG(ERROR) << "could not read entire content for " << id << " port "
               << socket.getPort();
    threadStats.setErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  VLOG(2) << "completed " << id << " off: " << off << " numRead: " << numRead;
  // Transfer of the file is complete here, mark the bytes effective
  WDT_CHECK(remainingData >= 0) << "Negative remainingData " << remainingData;
  if (remainingData > 0) {
    // if we need to read more anyway, let's move the data
    numRead = remainingData;
    if ((remainingData < Protocol::kMaxHeader) && (off > (bufferSize / 2))) {
      // rare so inneficient is ok
      VLOG(3) << "copying extra " << remainingData << " leftover bytes @ "
              << off;
      memmove(/* dst      */ buf,
              /* from     */ buf + off,
              /* how much */ remainingData);
      off = 0;
    } else {
      // otherwise just change the offset
      VLOG(3) << "will use remaining extra " << remainingData
              << " leftover bytes @ " << off;
    }
  } else {
    numRead = off = 0;
  }
  if (enableChecksum) {
    // have to read footer cmd
    oldOffset = off;
    numRead = readAtLeast(socket, buf + off, bufferSize - off,
                          Protocol::kMinBufLength, numRead);
    if (numRead < Protocol::kMinBufLength) {
      LOG(ERROR) << "socket read failure " << Protocol::kMinBufLength << " "
                 << numRead;
      threadStats.setErrorCode(SOCKET_READ_ERROR);
      return ACCEPT_WITH_TIMEOUT;
    }
    Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf[off++];
    if (cmd != Protocol::FOOTER_CMD) {
      LOG(ERROR) << "Expecting footer cmd, but received " << cmd;
      threadStats.setErrorCode(PROTOCOL_ERROR);
      return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
    }
    uint32_t receivedChecksum;
    bool success = Protocol::decodeFooter(
        buf, off, oldOffset + Protocol::kMaxFooter, receivedChecksum);
    if (!success) {
      LOG(ERROR) << "Unable to decode footer cmd";
      threadStats.setErrorCode(PROTOCOL_ERROR);
      return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
    }
    if (checksum != receivedChecksum) {
      LOG(ERROR) << "Checksum mismatch " << checksum << " " << receivedChecksum
                 << " port " << socket.getPort() << " file " << id;
      threadStats.setErrorCode(CHECKSUM_MISMATCH);
      return ACCEPT_WITH_TIMEOUT;
    }
    size_t msgLen = off - oldOffset;
    numRead -= msgLen;
  }
  threadStats.addEffectiveBytes(headerBytes, dataSize);
  threadStats.incrNumBlocks();
  return READ_NEXT_CMD;
}

Receiver::ReceiverState Receiver::processDoneCmd(ThreadData &data) {
  VLOG(1) << "entered PROCESS_DONE_CMD state " << data.threadIndex_;
  auto &numRead = data.numRead_;
  auto &threadStats = data.threadStats_;
  auto &checkpointIndex = data.checkpointIndex_;
  auto &pendingCheckpointIndex = data.pendingCheckpointIndex_;
  auto &off = data.off_;
  auto &oldOffset = data.oldOffset_;
  char *buf = data.getBuf();

  if (numRead != Protocol::kMinBufLength) {
    LOG(ERROR) << "Unexpected state for done command"
               << " off: " << off << " numRead: " << numRead;
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }

  ErrorCode senderStatus = (ErrorCode)buf[off++];
  int64_t numBlocksSend;
  bool success = Protocol::decodeDone(buf, off, oldOffset + Protocol::kMaxDone,
                                      numBlocksSend);
  if (!success) {
    LOG(ERROR) << "Unable to decode done cmd";
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  threadStats.setRemoteErrorCode(senderStatus);

  // received a valid command, applying pending checkpoint write update
  checkpointIndex = pendingCheckpointIndex;
  std::unique_lock<std::mutex> lock(mutex_);
  numBlocksSend_ = numBlocksSend;
  return WAIT_FOR_FINISH_OR_NEW_CHECKPOINT;
}

Receiver::ReceiverState Receiver::processSizeCmd(ThreadData &data) {
  VLOG(1) << "entered PROCESS_SIZE_CMD state " << data.threadIndex_;
  auto &threadStats = data.threadStats_;
  auto &numRead = data.numRead_;
  auto &off = data.off_;
  auto &oldOffset = data.oldOffset_;
  char *buf = data.getBuf();
  std::lock_guard<std::mutex> lock(mutex_);
  bool success = Protocol::decodeSize(buf, off, oldOffset + Protocol::kMaxSize,
                                      totalSenderBytes_);
  if (!success) {
    LOG(ERROR) << "Unable to decode size cmd";
    threadStats.setErrorCode(PROTOCOL_ERROR);
    return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
  }
  VLOG(1) << "Number of bytes to receive " << totalSenderBytes_;
  auto msgLen = off - oldOffset;
  numRead -= msgLen;
  return READ_NEXT_CMD;
}

Receiver::ReceiverState Receiver::sendGlobalCheckpoint(ThreadData &data) {
  LOG(INFO) << "entered SEND_GLOBAL_CHECKPOINTS state " << data.threadIndex_;
  char *buf = data.getBuf();
  auto &off = data.off_;
  auto &newCheckpoints = data.newCheckpoints_;
  auto &socket = data.socket_;
  auto &checkpointIndex = data.checkpointIndex_;
  auto &pendingCheckpointIndex = data.pendingCheckpointIndex_;
  auto &threadStats = data.threadStats_;
  auto &numRead = data.numRead_;
  auto bufferSize = data.bufferSize_;

  buf[0] = Protocol::ERR_CMD;
  off = 1;
  // leave space for length
  off += sizeof(uint16_t);
  auto oldOffset = off;
  bool retValue =
      Protocol::encodeCheckpoints(buf, off, bufferSize, newCheckpoints);
  WDT_CHECK(retValue);
  uint16_t length = off - oldOffset;
  folly::storeUnaligned<uint16_t>(buf + 1, folly::Endian::little(length));

  auto written = socket.write(buf, off);
  if (written != off) {
    LOG(ERROR) << "unable to write error checkpoints";
    threadStats.setErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  } else {
    threadStats.addHeaderBytes(off);
    pendingCheckpointIndex = checkpointIndex + newCheckpoints.size();
    numRead = off = 0;
    return READ_NEXT_CMD;
  }
}

Receiver::ReceiverState Receiver::sendAbortCmd(ThreadData &data) {
  LOG(INFO) << "Entered SEND_ABORT_CMD state " << data.threadIndex_;
  auto &threadStats = data.threadStats_;
  char *buf = data.getBuf();
  auto &socket = data.socket_;
  int32_t protocolVersion = protocolVersion_;
  int offset = 0;
  buf[offset++] = Protocol::ABORT_CMD;
  folly::storeUnaligned<int32_t>(buf + offset,
                                 folly::Endian::little(protocolVersion));
  offset += sizeof(int32_t);
  buf[offset++] = threadStats.getErrorCode();
  int64_t checkpoint = folly::Endian::little(threadStats.getNumBlocks());
  folly::storeUnaligned<int64_t>(buf + offset, checkpoint);
  offset += sizeof(int64_t);
  socket.write(buf, offset);
  // No need to check if we were successful in sending ABORT
  // This thread will simply disconnect and sender thread on the
  // other side will timeout
  socket.closeCurrentConnection();
  threadStats.addHeaderBytes(offset);
  return WAIT_FOR_FINISH_WITH_THREAD_ERROR;
}

Receiver::ReceiverState Receiver::sendDoneCmd(ThreadData &data) {
  VLOG(1) << "entered SEND_DONE_CMD state " << data.threadIndex_;
  char *buf = data.getBuf();
  auto &socket = data.socket_;
  auto &threadStats = data.threadStats_;
  auto &doneSendFailure = data.doneSendFailure_;

  buf[0] = Protocol::DONE_CMD;
  if (socket.write(buf, 1) != 1) {
    PLOG(ERROR) << "unable to send DONE " << data.threadIndex_;
    doneSendFailure = true;
    return ACCEPT_WITH_TIMEOUT;
  }

  threadStats.addHeaderBytes(1);

  auto read = socket.read(buf, 1);
  if (read != 1 || buf[0] != Protocol::DONE_CMD) {
    LOG(ERROR) << "did not receive ack for DONE";
    doneSendFailure = true;
    return ACCEPT_WITH_TIMEOUT;
  }

  read = socket.read(buf, Protocol::kMinBufLength);
  if (read != 0) {
    LOG(ERROR) << "EOF not found where expected";
    doneSendFailure = true;
    return ACCEPT_WITH_TIMEOUT;
  }
  socket.closeCurrentConnection();
  LOG(INFO) << "Got ack for DONE. Transfer finished for " << socket.getPort();
  return END;
}

Receiver::ReceiverState Receiver::waitForFinishWithThreadError(
    ThreadData &data) {
  LOG(INFO) << "entered WAIT_FOR_FINISH_WITH_THREAD_ERROR state "
            << data.threadIndex_;
  auto &threadStats = data.threadStats_;
  auto &socket = data.socket_;
  // should only be in this state if there is some error
  WDT_CHECK(threadStats.getErrorCode() != OK);

  // close the socket, so that sender receives an error during connect
  socket.closeAll();

  std::unique_lock<std::mutex> lock(mutex_);
  // post checkpoint in case of an error
  Checkpoint localCheckpoint =
      std::make_pair(threadServerSockets_[data.threadIndex_].getPort(),
                     threadStats.getNumBlocks());
  addCheckpoint(localCheckpoint);
  waitingWithErrorThreadCount_++;

  if (areAllThreadsFinished(true)) {
    endCurGlobalSession();
  } else {
    // wait for session end
    while (!hasCurSessionFinished(data)) {
      conditionAllFinished_.wait(lock);
    }
  }
  endCurThreadSession(data);
  return END;
}

Receiver::ReceiverState Receiver::waitForFinishOrNewCheckpoint(
    ThreadData &data) {
  VLOG(1) << "entered WAIT_FOR_FINISH_OR_NEW_CHECKPOINT state "
          << data.threadIndex_;
  auto &threadStats = data.threadStats_;
  auto &senderReadTimeout = data.senderReadTimeout_;
  auto &checkpointIndex = data.checkpointIndex_;
  auto &newCheckpoints = data.newCheckpoints_;
  char *buf = data.getBuf();
  auto &socket = data.socket_;
  // should only be called if there are no errors
  WDT_CHECK(threadStats.getErrorCode() == OK);

  std::unique_lock<std::mutex> lock(mutex_);
  // we have to check for checkpoints before checking to see if session ended or
  // not. because if some checkpoints have not been sent back to the sender,
  // session should not end
  newCheckpoints = getNewCheckpoints(checkpointIndex);
  if (!newCheckpoints.empty()) {
    return SEND_GLOBAL_CHECKPOINTS;
  }

  waitingThreadCount_++;
  if (areAllThreadsFinished(false)) {
    endCurGlobalSession();
    endCurThreadSession(data);
    return SEND_DONE_CMD;
  }

  // we must send periodic wait cmd to keep the sender thread alive
  while (true) {
    WDT_CHECK(senderReadTimeout > 0);  // must have received settings
    int timeoutMillis = senderReadTimeout / kWaitTinmeoutFactor;
    auto waitingTime = std::chrono::milliseconds(timeoutMillis);
    START_PERF_TIMER
    conditionAllFinished_.wait_for(lock, waitingTime);
    RECORD_PERF_RESULT(PerfStatReport::RECEIVER_WAIT_SLEEP)

    // check if transfer finished or not
    if (hasCurSessionFinished(data)) {
      endCurThreadSession(data);
      return SEND_DONE_CMD;
    }

    // check to see if any new checkpoints were added
    newCheckpoints = getNewCheckpoints(checkpointIndex);
    if (!newCheckpoints.empty()) {
      waitingThreadCount_--;
      return SEND_GLOBAL_CHECKPOINTS;
    }

    // must unlock because socket write could block for long time, as long as
    // the write timeout, which is 5sec by default
    lock.unlock();

    // send WAIT cmd to keep sender thread alive
    buf[0] = Protocol::WAIT_CMD;
    if (socket.write(buf, 1) != 1) {
      PLOG(ERROR) << "unable to write WAIT " << data.threadIndex_;
      threadStats.setErrorCode(SOCKET_WRITE_ERROR);
      lock.lock();
      // we again have to check if the session has finished or not. while
      // writing WAIT cmd, some other thread could have ended the session, so
      // going back to ACCEPT_WITH_TIMEOUT state would be wrong
      if (!hasCurSessionFinished(data)) {
        waitingThreadCount_--;
        return ACCEPT_WITH_TIMEOUT;
      }
      endCurThreadSession(data);
      return END;
    }
    threadStats.addHeaderBytes(1);
    lock.lock();
  }
}

void Receiver::receiveOne(int threadIndex, ServerSocket &socket,
                          size_t bufferSize, TransferStats &threadStats) {
  INIT_PERF_STAT_REPORT
  auto guard = folly::makeGuard([&] {
    perfReports_[threadIndex] = *perfStatReport;  // copy when done
    std::unique_lock<std::mutex> lock(mutex_);
    numActiveThreads_--;
    if (numActiveThreads_ == 0) {
      LOG(WARNING) << "Last thread finished "
                   << durationSeconds(Clock::now() - startTime_);
      transferFinished_ = true;
    }
  });
  ThreadData data(threadIndex, socket, threadStats, bufferSize);
  if (!data.getBuf()) {
    LOG(ERROR) << "error allocating " << bufferSize;
    threadStats.setErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }
  ReceiverState state = LISTEN;
  while (true) {
    if (wasAbortRequested()) {
      LOG(ERROR) << "Transfer aborted " << socket.getPort();
      threadStats.setErrorCode(ABORT);
      incrFailedThreadCountAndCheckForSessionEnd(data);
      break;
    }
    if (state == FAILED) {
      return;
    }
    if (state == END) {
      if (isJoinable_) {
        return;
      }
      state = ACCEPT_FIRST_CONNECTION;
    }
    state = (this->*stateMap_[state])(data);
  }
}
}
}  // namespace facebook::wdt
