/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/ReceiverThread.h>
#include <folly/Bits.h>
#include <folly/Checksum.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/ScopeGuard.h>
#include <folly/String.h>
#include <wdt/util/FileWriter.h>

namespace facebook {
namespace wdt {

const static int kTimeoutBufferMillis = 1000;
const static int kWaitTimeoutFactor = 5;
std::ostream &operator<<(std::ostream &os,
                         const ReceiverThread &receiverThread) {
  os << "Thread[" << receiverThread.threadIndex_
     << ", port: " << receiverThread.socket_->getPort() << "] ";
  return os;
}

// TODO: TLOG or such that does WLOG(level) << *this << " " << rest...

int64_t readAtLeast(ServerSocket &s, char *buf, int64_t max, int64_t atLeast,
                    int64_t len) {
  WVLOG(4) << "readAtLeast len " << len << " max " << max << " atLeast "
           << atLeast << " from " << s.getFd();
  WDT_CHECK_GE(len, 0);
  WDT_CHECK_GT(atLeast, 0);
  WDT_CHECK_LE(atLeast, max);
  int count = 0;
  while (len < atLeast) {
    // because we want to process data as soon as it arrives, tryFull option for
    // read is false
    int64_t n = s.read(buf + len, max - len, false);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << s.getPort() << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      WVLOG(2) << "Eof on " << s.getPort() << " after " << count << " reads "
               << "got " << len;
      return len;
    }
    len += n;
    count++;
  }
  WVLOG(3) << "Took " << count << " reads to get " << len
           << " from fd : " << s.getFd();
  return len;
}

int64_t readAtMost(ServerSocket &s, char *buf, int64_t max, int64_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  WVLOG(3) << "readAtMost target " << target;
  // because we want to process data as soon as it arrives, tryFull option for
  // read is false
  int64_t n = s.read(buf, target, false);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << s.getPort() << " with target " << target;
    return n;
  }
  if (n == 0) {
    WLOG(WARNING) << "Eof on " << s.getFd();
    return n;
  }
  WVLOG(3) << "readAtMost " << n << " / " << atMost << " from " << s.getFd();
  return n;
}

const ReceiverThread::StateFunction ReceiverThread::stateMap_[] = {
    &ReceiverThread::listen,
    &ReceiverThread::acceptFirstConnection,
    &ReceiverThread::acceptWithTimeout,
    &ReceiverThread::sendLocalCheckpoint,
    &ReceiverThread::readNextCmd,
    &ReceiverThread::processFileCmd,
    &ReceiverThread::processSettingsCmd,
    &ReceiverThread::processDoneCmd,
    &ReceiverThread::processSizeCmd,
    &ReceiverThread::sendFileChunks,
    &ReceiverThread::sendGlobalCheckpoint,
    &ReceiverThread::sendDoneCmd,
    &ReceiverThread::sendAbortCmd,
    &ReceiverThread::waitForFinishOrNewCheckpoint,
    &ReceiverThread::finishWithError};

ReceiverThread::ReceiverThread(Receiver *wdtParent, int threadIndex,
                               int32_t port, ThreadsController *controller)
    : WdtThread(wdtParent->options_, threadIndex, port,
                wdtParent->getProtocolVersion(), controller),
      wdtParent_(wdtParent) {
  controller_->registerThread(threadIndex_);
  threadCtx_->setAbortChecker(&wdtParent_->abortCheckerCallback_);
}

/**LISTEN STATE***/
ReceiverState ReceiverThread::listen() {
  WVLOG(1) << *this << " entered LISTEN state";
  const bool doActualWrites = !options_.skip_writes;
  int32_t port = socket_->getPort();
  WVLOG(1) << "Server Thread for port " << port << " with backlog "
           << socket_->getBackLog() << " on " << wdtParent_->getDir()
           << " writes = " << doActualWrites;

  for (int retry = 1; retry < options_.max_retries; ++retry) {
    ErrorCode code = socket_->listen();
    if (code == OK) {
      break;
    } else if (code == CONN_ERROR) {
      threadStats_.setLocalErrorCode(code);
      return FAILED;
    }
    WLOG(INFO) << *this << " Sleeping after failed attempt " << retry;
    /* sleep override */
    usleep(options_.sleep_millis * 1000);
  }
  // one more/last try (stays true if it worked above)
  if (socket_->listen() != OK) {
    WLOG(ERROR) << *this << " Unable to listen/bind despite retries";
    threadStats_.setLocalErrorCode(CONN_ERROR);
    return FAILED;
  }
  return ACCEPT_FIRST_CONNECTION;
}

/***ACCEPT_FIRST_CONNECTION***/
ReceiverState ReceiverThread::acceptFirstConnection() {
  WVLOG(1) << *this << " entered ACCEPT_FIRST_CONNECTION state";

  reset();
  socket_->closeNoCheck();
  auto timeout = options_.accept_timeout_millis;
  int acceptAttempts = 0;
  while (true) {
    // Move to timeout state if some other thread was successful
    // in getting a connection
    if (wdtParent_->hasNewTransferStarted()) {
      return ACCEPT_WITH_TIMEOUT;
    }
    if (acceptAttempts == options_.max_accept_retries) {
      WLOG(ERROR) << *this << " Unable to accept after " << acceptAttempts
                  << " attempts";
      threadStats_.setLocalErrorCode(CONN_ERROR);
      return FAILED;
    }
    if (wdtParent_->getCurAbortCode() != OK) {
      WLOG(ERROR) << *this << " Thread marked to abort while trying to accept "
                  << "first connection. Num attempts " << acceptAttempts;
      // Even though there is a transition FAILED here
      // getCurAbortCode() is going to be checked again in the receiveOne.
      // So this is pretty much irrelevant
      return FAILED;
    }
    ErrorCode code =
        socket_->acceptNextConnection(timeout, curConnectionVerified_);
    if (code == OK) {
      break;
    }
    ++acceptAttempts;
  }
  // Make the parent start new global session. This is executed
  // only by the first thread that calls this function
  controller_->executeAtStart(
      [&]() { wdtParent_->startNewGlobalSession(socket_->getPeerIp()); });
  return READ_NEXT_CMD;
}

/***ACCEPT_WITH_TIMEOUT STATE***/
ReceiverState ReceiverThread::acceptWithTimeout() {
  WLOG(INFO) << *this << " entered ACCEPT_WITH_TIMEOUT state";

  // check socket status
  ErrorCode socketErrCode = socket_->getNonRetryableErrCode();
  if (socketErrCode != OK) {
    WLOG(ERROR) << *this << "Socket has non-retryable error "
                << errorCodeToStr(socketErrCode);
    threadStats_.setLocalErrorCode(socketErrCode);
    return END;
  }
  socket_->closeNoCheck();
  blocksWaitingVerification_.clear();

  auto timeout = options_.accept_window_millis;
  if (senderReadTimeout_ > 0) {
    // transfer is in progress and we have already got sender settings
    timeout = std::max(senderReadTimeout_, senderWriteTimeout_) +
              kTimeoutBufferMillis;
  }
  ErrorCode code =
      socket_->acceptNextConnection(timeout, curConnectionVerified_);
  curConnectionVerified_ = false;
  if (code != OK) {
    WLOG(ERROR) << *this << " accept() failed with timeout " << timeout;
    threadStats_.setLocalErrorCode(code);
    return FINISH_WITH_ERROR;
  }

  numRead_ = off_ = 0;
  pendingCheckpointIndex_ = checkpointIndex_;
  ReceiverState nextState = READ_NEXT_CMD;
  if (threadStats_.getLocalErrorCode() != OK) {
    nextState = SEND_LOCAL_CHECKPOINT;
  }
  // reset thread status
  threadStats_.setLocalErrorCode(OK);
  return nextState;
}

/***SEND_LOCAL_CHECKPOINT STATE***/
ReceiverState ReceiverThread::sendLocalCheckpoint() {
  WLOG(INFO) << *this << " entered SEND_LOCAL_CHECKPOINT state";
  std::vector<Checkpoint> checkpoints;
  WVLOG(1) << *this << " sending local checkpoint " << checkpoint_;
  checkpoints.emplace_back(checkpoint_);

  int64_t off = 0;
  const int checkpointLen =
      Protocol::getMaxLocalCheckpointLength(threadProtocolVersion_);
  Protocol::encodeCheckpoints(threadProtocolVersion_, buf_, off, checkpointLen,
                              checkpoints);
  int written = socket_->write(buf_, checkpointLen);
  if (written != checkpointLen) {
    WLOG(ERROR) << *this << " unable to write local checkpoint. write mismatch "
                << checkpointLen << " " << written;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  threadStats_.addHeaderBytes(checkpointLen);
  return READ_NEXT_CMD;
}

/***READ_NEXT_CMD***/
ReceiverState ReceiverThread::readNextCmd() {
  WVLOG(1) << *this << " entered READ_NEXT_CMD state";
  oldOffset_ = off_;
  // TODO: we shouldn't have off_ here and buffer/size inside buffer.
  numRead_ = readAtLeast(*socket_, buf_ + off_, bufSize_ - off_,
                         Protocol::kMinBufLength, numRead_);
  if (numRead_ < Protocol::kMinBufLength) {
    WLOG(ERROR) << *this << " socket read failure " << Protocol::kMinBufLength
                << " " << numRead_;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[off_++];
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
  WLOG(ERROR) << *this << " received an unknown cmd " << cmd;
  threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
  return FINISH_WITH_ERROR;
}

/***PROCESS_SETTINGS_CMD***/
ReceiverState ReceiverThread::processSettingsCmd() {
  WVLOG(1) << *this << " entered PROCESS_SETTINGS_CMD state";
  Settings settings;
  int senderProtocolVersion;

  bool success = Protocol::decodeVersion(
      buf_, off_, oldOffset_ + Protocol::kMaxVersion, senderProtocolVersion);
  if (!success) {
    WLOG(ERROR) << *this << " Unable to decode version " << threadIndex_;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }
  if (senderProtocolVersion != threadProtocolVersion_) {
    WLOG(ERROR) << *this << " Receiver and sender protocol version mismatch "
                << senderProtocolVersion << " " << threadProtocolVersion_;
    int negotiatedProtocol = Protocol::negotiateProtocol(
        senderProtocolVersion, threadProtocolVersion_);
    if (negotiatedProtocol == 0) {
      WLOG(WARNING) << *this << " Can not support sender with version "
                    << senderProtocolVersion << ", aborting!";
      threadStats_.setLocalErrorCode(VERSION_INCOMPATIBLE);
      return SEND_ABORT_CMD;
    } else {
      WLOG_IF(INFO, threadProtocolVersion_ != negotiatedProtocol)
          << *this << "Changing receiver protocol version to "
          << negotiatedProtocol;
      threadProtocolVersion_ = negotiatedProtocol;
      if (negotiatedProtocol != senderProtocolVersion) {
        threadStats_.setLocalErrorCode(VERSION_MISMATCH);
        return SEND_ABORT_CMD;
      }
    }
  }

  success = Protocol::decodeSettings(
      threadProtocolVersion_, buf_, off_,
      oldOffset_ + Protocol::kMaxVersion + Protocol::kMaxSettings, settings);
  if (!success) {
    WLOG(ERROR) << *this << "Unable to decode settings cmd ";
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }
  auto senderId = settings.transferId;
  auto transferId = wdtParent_->getTransferId();
  if (transferId != senderId) {
    WLOG(ERROR) << *this << "Receiver and sender id mismatch " << senderId
                << " " << transferId;
    threadStats_.setLocalErrorCode(ID_MISMATCH);
    return SEND_ABORT_CMD;
  }
  senderReadTimeout_ = settings.readTimeoutMillis;
  senderWriteTimeout_ = settings.writeTimeoutMillis;
  isBlockMode_ = !settings.blockModeDisabled;
  curConnectionVerified_ = true;

  // determine footer type
  EncryptionType encryptionType = socket_->getEncryptionType();
  if (threadProtocolVersion_ >=
          Protocol::INCREMENTAL_TAG_VERIFICATION_VERSION &&
      encryptionTypeToTagLen(encryptionType)) {
    if (settings.enableChecksum) {
      WLOG(ERROR) << *this << "Checksum can not be enabled with gcm encryption";
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return FINISH_WITH_ERROR;
    }
    footerType_ = ENC_TAG_FOOTER;
  } else if (settings.enableChecksum) {
    footerType_ = CHECKSUM_FOOTER;
  } else {
    footerType_ = NO_FOOTER;
  }

  if (settings.sendFileChunks) {
    // We only move to SEND_FILE_CHUNKS state, if download resumption is enabled
    // in the sender side
    numRead_ = off_ = 0;
    return SEND_FILE_CHUNKS;
  }
  auto msgLen = off_ - oldOffset_;
  numRead_ -= msgLen;
  return READ_NEXT_CMD;
}

/***PROCESS_FILE_CMD***/
ReceiverState ReceiverThread::processFileCmd() {
  WVLOG(1) << *this << " entered PROCESS_FILE_CMD state";
  // following block needs to be executed for the first file cmd. There is no
  // harm in executing it more than once. number of blocks equal to 0 is a good
  // approximation for first file cmd. Did not want to introduce another boolean
  if (options_.enable_download_resumption && threadStats_.getNumBlocks() == 0) {
    auto sendChunksFunnel = controller_->getFunnel(SEND_FILE_CHUNKS_FUNNEL);
    auto state = sendChunksFunnel->getStatus();
    if (state == FUNNEL_START) {
      // sender is not in resumption mode
      wdtParent_->addTransferLogHeader(isBlockMode_,
                                       /* sender not resuming */ false);
      sendChunksFunnel->notifySuccess();
    }
  }
  checkpoint_.resetLastBlockDetails();
  BlockDetails blockDetails;
  auto guard = folly::makeGuard([&] {
    if (threadStats_.getLocalErrorCode() != OK) {
      threadStats_.incrFailedAttempts();
    }
  });

  ErrorCode transferStatus = (ErrorCode)buf_[off_++];
  if (transferStatus != OK) {
    // TODO: use this status information to implement fail fast mode
    WVLOG(1) << *this << " sender entered into error state "
             << errorCodeToStr(transferStatus);
  }
  int16_t headerLen = folly::loadUnaligned<int16_t>(buf_ + off_);
  headerLen = folly::Endian::little(headerLen);
  WVLOG(2) << "Processing FILE_CMD, header len " << headerLen;

  if (headerLen > numRead_) {
    int64_t end = oldOffset_ + numRead_;
    numRead_ =
        readAtLeast(*socket_, buf_ + end, bufSize_ - end, headerLen, numRead_);
  }
  if (numRead_ < headerLen) {
    WLOG(ERROR) << *this << " Unable to read full header " << headerLen << " "
                << numRead_;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  off_ += sizeof(int16_t);
  bool success = Protocol::decodeHeader(threadProtocolVersion_, buf_, off_,
                                        numRead_ + oldOffset_, blockDetails);
  int64_t headerBytes = off_ - oldOffset_;
  // transferred header length must match decoded header length
  WDT_CHECK_EQ(headerLen, headerBytes) << " " << blockDetails.fileName << " "
                                       << blockDetails.seqId << " "
                                       << threadProtocolVersion_;
  threadStats_.addHeaderBytes(headerBytes);
  threadStats_.addEffectiveBytes(headerBytes, 0);
  if (!success) {
    WLOG(ERROR) << *this << " Error decoding at"
                << " ooff:" << oldOffset_ << " off_: " << off_
                << " numRead_: " << numRead_;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }
  if (blockDetails.allocationStatus == TO_BE_DELETED &&
      (blockDetails.fileSize != 0 || blockDetails.dataSize != 0)) {
    WLOG(ERROR) << *this << " Invalid file header, file to be deleted, but "
                            "file-size/block-size not zero "
                << blockDetails.fileName << " file-size "
                << blockDetails.fileSize << " block-size "
                << blockDetails.dataSize;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }

  // received a well formed file cmd, apply the pending checkpoint update
  checkpointIndex_ = pendingCheckpointIndex_;
  WVLOG(1) << *this << " Read id:" << blockDetails.fileName
           << " size:" << blockDetails.dataSize << " ooff:" << oldOffset_
           << " off_: " << off_ << " numRead_: " << numRead_;
  auto &fileCreator = wdtParent_->getFileCreator();
  FileWriter writer(*threadCtx_, &blockDetails, fileCreator.get());
  auto writtenGuard = folly::makeGuard([&] {
    if (threadProtocolVersion_ >= Protocol::CHECKPOINT_OFFSET_VERSION &&
        footerType_ == NO_FOOTER) {
      checkpoint_.setLastBlockDetails(blockDetails.seqId, blockDetails.offset,
                                      writer.getTotalWritten());
      threadStats_.addEffectiveBytes(headerBytes, writer.getTotalWritten());
    }
  });
  if (writer.open() != OK) {
    threadStats_.setLocalErrorCode(FILE_WRITE_ERROR);
    return SEND_ABORT_CMD;
  }
  int32_t checksum = 0;
  int64_t remainingData = numRead_ + oldOffset_ - off_;
  int64_t toWrite = remainingData;
  WDT_CHECK(remainingData >= 0);
  if (remainingData >= blockDetails.dataSize) {
    toWrite = blockDetails.dataSize;
  }
  bool decryptorCtxSaved = false;
  if (footerType_ == ENC_TAG_FOOTER && remainingData <= blockDetails.dataSize) {
    decryptorCtxSaved = true;
    socket_->saveDecryptorCtx(blockDetails.dataSize - remainingData);
  }
  threadStats_.addDataBytes(toWrite);
  if (footerType_ == CHECKSUM_FOOTER) {
    checksum = folly::crc32c((const uint8_t *)(buf_ + off_), toWrite, checksum);
  }
  auto throttler = wdtParent_->getThrottler();
  if (throttler) {
    // We might be reading more than we require for this file but
    // throttling should make sense for any additional bytes received
    // on the network
    throttler->limit(*threadCtx_, toWrite + headerBytes);
  }
  ErrorCode code = ERROR;
  if (toWrite > 0) {
    code = writer.write(buf_ + off_, toWrite);
    if (code != OK) {
      threadStats_.setLocalErrorCode(code);
      return SEND_ABORT_CMD;
    }
  }
  off_ += toWrite;
  remainingData -= toWrite;
  // also means no leftOver so it's ok we use buf_ from start
  while (writer.getTotalWritten() < blockDetails.dataSize) {
    if (wdtParent_->getCurAbortCode() != OK) {
      WLOG(ERROR) << *this << "Thread marked for abort while processing "
                  << blockDetails.fileName << " " << blockDetails.seqId
                  << " port : " << socket_->getPort();
      return FAILED;
    }
    int64_t nres = readAtMost(*socket_, buf_, bufSize_,
                              blockDetails.dataSize - writer.getTotalWritten());
    if (nres <= 0) {
      break;
    }
    if (throttler) {
      // We only know how much we have read after we are done calling
      // readAtMost. Call throttler with the bytes read off_ the wire.
      throttler->limit(*threadCtx_, nres);
    }
    threadStats_.addDataBytes(nres);
    if (footerType_ == CHECKSUM_FOOTER) {
      checksum = folly::crc32c((const uint8_t *)buf_, nres, checksum);
    }
    code = writer.write(buf_, nres);
    if (code != OK) {
      threadStats_.setLocalErrorCode(code);
      return SEND_ABORT_CMD;
    }
  }
  if (writer.getTotalWritten() != blockDetails.dataSize) {
    // This can only happen if there are transmission errors
    // Write errors to disk are already taken care of above
    WLOG(ERROR) << *this << " could not read entire content for "
                << blockDetails.fileName << " port " << socket_->getPort();
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  writtenGuard.dismiss();
  WVLOG(2) << "completed " << blockDetails.fileName << " off: " << off_
           << " numRead: " << numRead_;
  // Transfer of the file is complete here, mark the bytes effective
  WDT_CHECK(remainingData >= 0) << "Negative remainingData " << remainingData;
  if (remainingData > 0) {
    // if we need to read more anyway, let's move the data
    numRead_ = remainingData;
    if ((remainingData < Protocol::kMaxHeader) && (off_ > (bufSize_ / 2))) {
      // rare so inefficient is ok
      WVLOG(3) << "copying extra " << remainingData << " leftover bytes @ "
               << off_;
      memmove(/* dst      */ buf_,
              /* from     */ buf_ + off_,
              /* how much */ remainingData);
      off_ = 0;
    } else {
      // otherwise just continue from the offset
      WVLOG(3) << "Using remaining extra " << remainingData
               << " leftover bytes starting @ " << off_;
    }
  } else {
    numRead_ = off_ = 0;
  }
  if (footerType_ != NO_FOOTER) {
    // have to read footer cmd
    oldOffset_ = off_;
    numRead_ = readAtLeast(*socket_, buf_ + off_, bufSize_ - off_,
                           Protocol::kMinBufLength, numRead_);
    if (numRead_ < Protocol::kMinBufLength) {
      WLOG(ERROR) << *this << " socket read failure " << Protocol::kMinBufLength
                  << " " << numRead_;
      threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
      return ACCEPT_WITH_TIMEOUT;
    }
    Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[off_++];
    if (cmd != Protocol::FOOTER_CMD) {
      WLOG(ERROR) << *this << " Expecting footer cmd, but received " << cmd;
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return FINISH_WITH_ERROR;
    }
    int32_t receivedChecksum;
    std::string receivedTag;
    bool success = Protocol::decodeFooter(
        buf_, off_, oldOffset_ + Protocol::kMaxFooter, receivedChecksum,
        receivedTag, (footerType_ == ENC_TAG_FOOTER));
    if (!success) {
      WLOG(ERROR) << *this << " Unable to decode footer cmd";
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return FINISH_WITH_ERROR;
    }
    if (footerType_ == CHECKSUM_FOOTER) {
      if (checksum != receivedChecksum) {
        WLOG(ERROR) << *this << " Checksum mismatch " << checksum << " "
                    << receivedChecksum << " port " << socket_->getPort()
                    << " file " << blockDetails.fileName;
        threadStats_.setLocalErrorCode(CHECKSUM_MISMATCH);
        return ACCEPT_WITH_TIMEOUT;
      }
      markBlockVerified(blockDetails);
    }
    if (footerType_ == ENC_TAG_FOOTER) {
      blocksWaitingVerification_.emplace_back(blockDetails);
      if (decryptorCtxSaved) {
        if (!socket_->verifyTag(receivedTag)) {
          WLOG(ERROR) << *this << " GCM encryption tag mismatch "
                      << folly::humanify(receivedTag) << " file "
                      << blockDetails.fileName;
          threadStats_.setLocalErrorCode(ENCRYPTION_ERROR);
          return ACCEPT_WITH_TIMEOUT;
        }
        markReceivedBlocksVerified();
      }
    }
    int64_t msgLen = off_ - oldOffset_;
    numRead_ -= msgLen;
  } else {
    markBlockVerified(blockDetails);
  }
  return READ_NEXT_CMD;
}

void ReceiverThread::markBlockVerified(const BlockDetails &blockDetails) {
  threadStats_.addEffectiveBytes(0, blockDetails.dataSize);
  threadStats_.incrNumBlocks();
  checkpoint_.incrNumBlocks();
  if (!options_.isLogBasedResumption()) {
    return;
  }
  TransferLogManager &transferLogManager = wdtParent_->getTransferLogManager();
  if (blockDetails.allocationStatus == TO_BE_DELETED) {
    transferLogManager.addFileInvalidationEntry(blockDetails.seqId);
    return;
  }
  transferLogManager.addBlockWriteEntry(blockDetails.seqId, blockDetails.offset,
                                        blockDetails.dataSize);
}

void ReceiverThread::markReceivedBlocksVerified() {
  for (const BlockDetails &blockDetails : blocksWaitingVerification_) {
    markBlockVerified(blockDetails);
  }
  blocksWaitingVerification_.clear();
}

ReceiverState ReceiverThread::processDoneCmd() {
  WVLOG(1) << *this << " entered PROCESS_DONE_CMD state";
  if (numRead_ != Protocol::kMinBufLength) {
    WLOG(ERROR) << *this << " Unexpected state for done command"
                << " off_: " << off_ << " numRead_: " << numRead_;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }

  ErrorCode senderStatus = (ErrorCode)buf_[off_++];
  int64_t numBlocksSend = -1;
  int64_t totalSenderBytes = -1;
  bool success = Protocol::decodeDone(threadProtocolVersion_, buf_, off_,
                                      oldOffset_ + Protocol::kMaxDone,
                                      numBlocksSend, totalSenderBytes);
  if (!success) {
    WLOG(ERROR) << *this << " Unable to decode done cmd";
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }
  threadStats_.setNumBlocksSend(numBlocksSend);
  threadStats_.setTotalSenderBytes(totalSenderBytes);
  threadStats_.setRemoteErrorCode(senderStatus);

  // received a valid command, applying pending checkpoint write update
  checkpointIndex_ = pendingCheckpointIndex_;
  return WAIT_FOR_FINISH_OR_NEW_CHECKPOINT;
}

ReceiverState ReceiverThread::processSizeCmd() {
  WVLOG(1) << *this << " entered PROCESS_SIZE_CMD state";
  int64_t totalSenderBytes;
  bool success = Protocol::decodeSize(
      buf_, off_, oldOffset_ + Protocol::kMaxSize, totalSenderBytes);
  if (!success) {
    WLOG(ERROR) << *this << " Unable to decode size cmd";
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return FINISH_WITH_ERROR;
  }
  WVLOG(1) << "Number of bytes to receive " << totalSenderBytes;
  threadStats_.setTotalSenderBytes(totalSenderBytes);
  auto msgLen = off_ - oldOffset_;
  numRead_ -= msgLen;
  return READ_NEXT_CMD;
}

ReceiverState ReceiverThread::sendFileChunks() {
  WLOG(INFO) << *this << " entered SEND_FILE_CHUNKS state";
  WDT_CHECK(senderReadTimeout_ > 0);  // must have received settings
  int waitingTimeMillis = senderReadTimeout_ / kWaitTimeoutFactor;
  auto execFunnel = controller_->getFunnel(SEND_FILE_CHUNKS_FUNNEL);
  while (true) {
    auto status = execFunnel->getStatus();
    switch (status) {
      case FUNNEL_END: {
        buf_[0] = Protocol::ACK_CMD;
        int toWrite = 1;
        int written = socket_->write(buf_, toWrite);
        if (written != toWrite) {
          WLOG(ERROR) << *this << " socket write error " << toWrite << " "
                      << written;
          threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
          return ACCEPT_WITH_TIMEOUT;
        }
        threadStats_.addHeaderBytes(toWrite);
        return READ_NEXT_CMD;
      }
      case FUNNEL_PROGRESS: {
        buf_[0] = Protocol::WAIT_CMD;
        int toWrite = 1;
        int written = socket_->write(buf_, toWrite);
        if (written != toWrite) {
          WLOG(ERROR) << *this << " socket write error " << toWrite << " "
                      << written;
          threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
          return ACCEPT_WITH_TIMEOUT;
        }
        threadStats_.addHeaderBytes(toWrite);
        execFunnel->wait(waitingTimeMillis);
        break;
      }
      case FUNNEL_START: {
        int64_t off = 0;
        buf_[off++] = Protocol::CHUNKS_CMD;
        const auto &fileChunksInfo = wdtParent_->getFileChunksInfo();
        const int64_t numParsedChunksInfo = fileChunksInfo.size();
        Protocol::encodeChunksCmd(buf_, off, bufSize_, numParsedChunksInfo);
        int written = socket_->write(buf_, off);
        if (written > 0) {
          threadStats_.addHeaderBytes(written);
        }
        if (written != off) {
          WLOG(ERROR) << *this << " socket write err " << off << " " << written;
          threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
          execFunnel->notifyFail();
          return ACCEPT_WITH_TIMEOUT;
        }
        int64_t numEntriesWritten = 0;
        // we try to encode as many chunks as possible in the buffer. If a
        // single
        // chunk can not fit in the buffer, it is ignored. Format of encoding :
        // <data-size><chunk1><chunk2>...
        while (numEntriesWritten < numParsedChunksInfo) {
          off = sizeof(int32_t);
          int64_t numEntriesEncoded = Protocol::encodeFileChunksInfoList(
              buf_, off, bufSize_, numEntriesWritten, fileChunksInfo);
          int32_t dataSize = folly::Endian::little(off - sizeof(int32_t));
          folly::storeUnaligned<int32_t>(buf_, dataSize);
          written = socket_->write(buf_, off);
          if (written > 0) {
            threadStats_.addHeaderBytes(written);
          }
          if (written != off) {
            break;
          }
          numEntriesWritten += numEntriesEncoded;
        }
        if (numEntriesWritten != numParsedChunksInfo) {
          WLOG(ERROR) << *this << " Could not write all the file chunks "
                      << numParsedChunksInfo << " " << numEntriesWritten;
          threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
          execFunnel->notifyFail();
          return ACCEPT_WITH_TIMEOUT;
        }
        // try to read ack
        int64_t toRead = 1;
        int64_t numRead = socket_->read(buf_, toRead);
        if (numRead != toRead) {
          WLOG(ERROR) << *this << " Socket read error " << toRead << " "
                      << numRead;
          threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
          execFunnel->notifyFail();
          return ACCEPT_WITH_TIMEOUT;
        }
        wdtParent_->addTransferLogHeader(isBlockMode_,
                                         /* sender resuming */ true);
        execFunnel->notifySuccess();
        return READ_NEXT_CMD;
      }
    }
  }
}

ReceiverState ReceiverThread::sendGlobalCheckpoint() {
  WLOG(INFO) << *this << " entered SEND_GLOBAL_CHECKPOINTS state";
  buf_[0] = Protocol::ERR_CMD;
  off_ = 1;
  // leave space for length
  off_ += sizeof(int16_t);
  auto oldOffset = off_;
  Protocol::encodeCheckpoints(threadProtocolVersion_, buf_, off_, bufSize_,
                              newCheckpoints_);
  int16_t length = off_ - oldOffset;
  folly::storeUnaligned<int16_t>(buf_ + 1, folly::Endian::little(length));

  int written = socket_->write(buf_, off_);
  if (written != off_) {
    WLOG(ERROR) << *this << " unable to write error checkpoints";
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  } else {
    threadStats_.addHeaderBytes(off_);
    pendingCheckpointIndex_ = checkpointIndex_ + newCheckpoints_.size();
    numRead_ = off_ = 0;
    return READ_NEXT_CMD;
  }
}

ReceiverState ReceiverThread::sendAbortCmd() {
  WLOG(INFO) << *this << " entered SEND_ABORT_CMD state";
  int64_t offset = 0;
  buf_[offset++] = Protocol::ABORT_CMD;
  Protocol::encodeAbort(buf_, offset, threadProtocolVersion_,
                        threadStats_.getLocalErrorCode(),
                        threadStats_.getNumFiles());
  socket_->write(buf_, offset);
  // No need to check if we were successful in sending ABORT
  // This thread will simply disconnect and sender thread on the
  // other side will timeout
  socket_->closeConnection();
  threadStats_.addHeaderBytes(offset);
  if (threadStats_.getLocalErrorCode() == VERSION_MISMATCH) {
    // Receiver should try again expecting sender to have changed its version
    return ACCEPT_WITH_TIMEOUT;
  }
  return FINISH_WITH_ERROR;
}

ReceiverState ReceiverThread::sendDoneCmd() {
  WVLOG(1) << *this << " entered SEND_DONE_CMD state";
  buf_[0] = Protocol::DONE_CMD;
  if (socket_->write(buf_, 1) != 1) {
    PLOG(ERROR) << *this << " unable to send DONE " << threadIndex_;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }

  threadStats_.addHeaderBytes(1);

  auto read = socket_->read(buf_, 1);
  if (read != 1 || buf_[0] != Protocol::DONE_CMD) {
    WLOG(ERROR) << *this << " did not receive ack for DONE";
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return ACCEPT_WITH_TIMEOUT;
  }
  ErrorCode code = socket_->expectEndOfStream();
  if (code != OK) {
    WLOG(ERROR) << *this << " error while processing logical end of stream "
                << errorCodeToStr(code);
    threadStats_.setLocalErrorCode(code);
    return ACCEPT_WITH_TIMEOUT;
  } else if (footerType_ == ENC_TAG_FOOTER) {
    markReceivedBlocksVerified();
  }
  threadStats_.setLocalErrorCode(socket_->closeConnection());
  WLOG(INFO) << *this << " got ack for DONE and logical eof. Transfer finished";
  return END;
}

ReceiverState ReceiverThread::finishWithError() {
  WLOG(INFO) << *this << " entered FINISH_WITH_ERROR state";
  // should only be in this state if there is some error
  WDT_CHECK(threadStats_.getLocalErrorCode() != OK);

  // close the socket, so that sender receives an error during connect
  socket_->closeAllNoCheck();
  auto cv = controller_->getCondition(WAIT_FOR_FINISH_OR_CHECKPOINT_CV);
  auto guard = cv->acquire();
  wdtParent_->addCheckpoint(checkpoint_);
  controller_->markState(threadIndex_, FINISHED);
  guard.notifyOne();
  return END;
}

ReceiverState ReceiverThread::checkForFinishOrNewCheckpoints() {
  auto checkpoints = wdtParent_->getNewCheckpoints(checkpointIndex_);
  if (!checkpoints.empty()) {
    newCheckpoints_ = std::move(checkpoints);
    controller_->markState(threadIndex_, RUNNING);
    return SEND_GLOBAL_CHECKPOINTS;
  }
  bool existActiveThreads = controller_->hasThreads(threadIndex_, RUNNING);
  if (!existActiveThreads) {
    controller_->markState(threadIndex_, FINISHED);
    return SEND_DONE_CMD;
  }
  return WAIT_FOR_FINISH_OR_NEW_CHECKPOINT;
}

ReceiverState ReceiverThread::waitForFinishOrNewCheckpoint() {
  WLOG(INFO) << *this << " entered WAIT_FOR_FINISH_OR_NEW_CHECKPOINT state";
  // should only be called if the are no errors
  WDT_CHECK(threadStats_.getLocalErrorCode() == OK);
  auto cv = controller_->getCondition(WAIT_FOR_FINISH_OR_CHECKPOINT_CV);
  int timeoutMillis = senderReadTimeout_ / kWaitTimeoutFactor;
  controller_->markState(threadIndex_, WAITING);
  while (true) {
    WDT_CHECK(senderReadTimeout_ > 0);  // must have received settings
    {
      auto guard = cv->acquire();
      auto state = checkForFinishOrNewCheckpoints();
      if (state != WAIT_FOR_FINISH_OR_NEW_CHECKPOINT) {
        guard.notifyOne();
        return state;
      }
      {
        PerfStatCollector statCollector(*threadCtx_,
                                        PerfStatReport::RECEIVER_WAIT_SLEEP);
        guard.wait(timeoutMillis);
      }
      state = checkForFinishOrNewCheckpoints();
      if (state != WAIT_FOR_FINISH_OR_NEW_CHECKPOINT) {
        guard.notifyOne();
        return state;
      }
    }
    // send WAIT cmd to keep sender thread alive
    buf_[0] = Protocol::WAIT_CMD;
    if (socket_->write(buf_, 1) != 1) {
      PLOG(ERROR) << *this << " unable to write WAIT ";
      threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
      controller_->markState(threadIndex_, RUNNING);
      return ACCEPT_WITH_TIMEOUT;
    }
    threadStats_.addHeaderBytes(1);
  }
}

void ReceiverThread::start() {
  if (buf_ == nullptr) {
    WLOG(ERROR) << *this << " Unable to allocate buffer";
    threadStats_.setLocalErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }
  ReceiverState state = LISTEN;
  while (true) {
    ErrorCode abortCode = wdtParent_->getCurAbortCode();
    if (abortCode != OK) {
      WLOG(ERROR) << *this << " Transfer aborted " << socket_->getPort() << " "
                  << errorCodeToStr(abortCode);
      threadStats_.setLocalErrorCode(ABORT);
      break;
    }
    if (state == FAILED || state == END) {
      break;
    }
    state = (this->*stateMap_[state])();
  }
  controller_->deRegisterThread(threadIndex_);
  controller_->executeAtEnd([&]() { wdtParent_->endCurGlobalSession(); });
  WDT_CHECK(socket_.get());
  threadStats_.setEncryptionType(socket_->getEncryptionType());
  WLOG(INFO) << *this << threadStats_;
}

int32_t ReceiverThread::getPort() const {
  return socket_->getPort();
}

ErrorCode ReceiverThread::init() {
  const EncryptionParams &encryptionData =
      wdtParent_->transferRequest_.encryptionData;
  socket_ = folly::make_unique<ServerSocket>(
      *threadCtx_, port_, wdtParent_->backlog_, encryptionData);
  int max_retries = options_.max_retries;
  for (int retries = 0; retries < max_retries; retries++) {
    if (socket_->listen() == OK) {
      break;
    }
  }
  if (socket_->listen() != OK) {
    WLOG(ERROR) << *this << "Couldn't listen on port " << socket_->getPort();
    return ERROR;
  }
  checkpoint_.port = socket_->getPort();
  WLOG(INFO) << *this << " Listening on port " << socket_->getPort();
  return OK;
}

void ReceiverThread::reset() {
  numRead_ = off_ = 0;
  checkpointIndex_ = pendingCheckpointIndex_ = 0;
  senderReadTimeout_ = senderWriteTimeout_ = -1;
  curConnectionVerified_ = false;
  threadStats_.reset();
  checkpoints_.clear();
  newCheckpoints_.clear();
  checkpoint_ = Checkpoint(socket_->getPort());
}

ReceiverThread::~ReceiverThread() {
}
}
}
