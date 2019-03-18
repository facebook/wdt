/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/SenderThread.h>
#include <folly/lang/Bits.h>
#include <folly/hash/Checksum.h>
#include <folly/Conv.h>
#include <folly/Memory.h>
#include <folly/String.h>
#include <sys/stat.h>
#include <wdt/Sender.h>

namespace facebook {
namespace wdt {

std::ostream &operator<<(std::ostream &os, const SenderThread &senderThread) {
  os << "Thread[" << senderThread.threadIndex_
     << ", port: " << senderThread.port_ << "] ";
  return os;
}

const SenderThread::StateFunction SenderThread::stateMap_[] = {
    &SenderThread::connect,         &SenderThread::readLocalCheckPoint,
    &SenderThread::sendSettings,    &SenderThread::sendBlocks,
    &SenderThread::sendDoneCmd,     &SenderThread::sendSizeCmd,
    &SenderThread::checkForAbort,   &SenderThread::readFileChunks,
    &SenderThread::readReceiverCmd, &SenderThread::processDoneCmd,
    &SenderThread::processWaitCmd,  &SenderThread::processErrCmd,
    &SenderThread::processAbortCmd, &SenderThread::processVersionMismatch};

std::unique_ptr<IClientSocket> SenderThread::connectToReceiver(
    const int port, IAbortChecker const * /*abortChecker*/,
    ErrorCode &errCode) {
  auto startTime = Clock::now();
  int connectAttempts = 0;
  std::unique_ptr<IClientSocket> socket;
  const EncryptionParams &encryptionData =
      wdtParent_->transferRequest_.encryptionData;
  int64_t ivChangeInterval = wdtParent_->transferRequest_.ivChangeInterval;
  if (threadProtocolVersion_ <
      Protocol::PERIODIC_ENCRYPTION_IV_CHANGE_VERSION) {
    WTLOG(WARNING) << "Disabling periodic iv change for sender with version "
                   << threadProtocolVersion_;
    ivChangeInterval = 0;
  }

  if (wdtParent_->socketCreator_) {
    VLOG(3) << "Creating sender socket";
    socket = wdtParent_->socketCreator_->makeClientSocket(
        *threadCtx_, wdtParent_->getDestination(), port, encryptionData,
        ivChangeInterval, wdtParent_->transferRequest_.tls);
  } else {
    // socket creator not set, creating ClientSocket
    VLOG(3) << "Creating sender socket";
    socket = std::make_unique<ClientSocket>(*threadCtx_,
                                            wdtParent_->getDestination(), port,
                                            encryptionData, ivChangeInterval);
  }

  if (!socket) {
    errCode = ERROR;
    return nullptr;
  }

  double retryInterval = options_.sleep_millis;
  int maxRetries = options_.max_retries;
  if (maxRetries < 1) {
    WTLOG(ERROR) << "Invalid max_retries " << maxRetries << " using 1 instead";
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
    if (getThreadAbortCode() != OK) {
      errCode = ABORT;
      return nullptr;
    }
    if (i != maxRetries) {
      // sleep between attempts but not after the last
      WTVLOG(1) << "Sleeping after failed attempt " << i;
      /* sleep override */ usleep(retryInterval * 1000);
    }
  }
  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  if (errCode != OK) {
    WTLOG(ERROR) << "Unable to connect to " << wdtParent_->getDestination()
                 << " " << port << " despite " << connectAttempts
                 << " retries in " << elapsedSecsConn << " seconds.";
    errCode = CONN_ERROR;
    return nullptr;
  }

  (connectAttempts > 1) ? WTLOG(WARNING) : WTLOG(INFO)
      << "Connection took " << connectAttempts << " attempt(s) and "
      << elapsedSecsConn << " seconds. port " << port;
  return socket;
}

SenderState SenderThread::connect() {
  WTVLOG(1) << "entered CONNECT state";
  if (socket_) {
    ErrorCode socketErrCode = socket_->getNonRetryableErrCode();
    if (socketErrCode != OK) {
      WTLOG(ERROR) << "Socket has non-retryable error "
                   << errorCodeToStr(socketErrCode);
      threadStats_.setLocalErrorCode(socketErrCode);
      return END;
    }
    socket_->closeNoCheck();
  }
  if (numReconnectWithoutProgress_ >= options_.max_transfer_retries) {
    WTLOG(ERROR) << "Sender thread reconnected " << numReconnectWithoutProgress_
                 << " times without making any progress, giving up. port: "
                 << socket_->getPort();
    threadStats_.setLocalErrorCode(NO_PROGRESS);
    return END;
  }
  ErrorCode code;
  // TODO cleanup more but for now avoid having 2 socket object live per port
  socket_ = nullptr;
  socket_ = connectToReceiver(port_, threadCtx_->getAbortChecker(), code);
  if (code == ABORT) {
    threadStats_.setLocalErrorCode(ABORT);
    if (getThreadAbortCode() == VERSION_MISMATCH) {
      return PROCESS_VERSION_MISMATCH;
    }
    return END;
  }
  if (code != OK) {
    threadStats_.setLocalErrorCode(code);
    return END;
  }
  auto nextState = SEND_SETTINGS;
  if (threadStats_.getLocalErrorCode() != OK) {
    nextState = READ_LOCAL_CHECKPOINT;
  }
  // resetting the status of thread
  reset();
  return nextState;
}

SenderState SenderThread::readLocalCheckPoint() {
  WTLOG(INFO) << "entered READ_LOCAL_CHECKPOINT state";
  ThreadTransferHistory &transferHistory = getTransferHistory();
  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  int checkpointLen =
      Protocol::getMaxLocalCheckpointLength(threadProtocolVersion_);
  int64_t numRead = socket_->read(buf_, checkpointLen);
  if (numRead != checkpointLen) {
    WTLOG(ERROR) << "read mismatch during reading local checkpoint "
                 << checkpointLen << " " << numRead << " port " << port_;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    numReconnectWithoutProgress_++;
    return CONNECT;
  }
  bool isValidCheckpoint = true;
  if (!Protocol::decodeCheckpoints(threadProtocolVersion_, buf_, decodeOffset,
                                   checkpointLen, checkpoints)) {
    WTLOG(ERROR) << "checkpoint decode failure "
                 << folly::humanify(std::string(buf_, numRead));
    isValidCheckpoint = false;
  } else if (checkpoints.size() != 1) {
    WTLOG(ERROR) << "Illegal local checkpoint, unexpected num checkpoints "
                 << checkpoints.size() << " "
                 << folly::humanify(std::string(buf_, numRead));
    isValidCheckpoint = false;
  } else if (checkpoints[0].port != port_) {
    WTLOG(ERROR) << "illegal checkpoint, checkpoint " << checkpoints[0]
                 << " doesn't match the port " << port_;
    isValidCheckpoint = false;
  }
  if (!isValidCheckpoint) {
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return END;
  }
  const Checkpoint &checkpoint = checkpoints[0];
  auto numBlocks = checkpoint.numBlocks;
  WTVLOG(1) << "received local checkpoint " << checkpoint;

  if (numBlocks == -1) {
    // Receiver failed while sending DONE cmd
    return READ_RECEIVER_CMD;
  }

  ErrorCode errCode = transferHistory.setLocalCheckpoint(checkpoint);
  if (errCode == INVALID_CHECKPOINT) {
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return END;
  }
  if (errCode == NO_PROGRESS) {
    ++numReconnectWithoutProgress_;
  } else {
    numReconnectWithoutProgress_ = 0;
  }
  return SEND_SETTINGS;
}

SenderState SenderThread::sendSettings() {
  WTVLOG(1) << "entered SEND_SETTINGS state";
  int64_t readTimeoutMillis = options_.read_timeout_millis;
  int64_t writeTimeoutMillis = options_.write_timeout_millis;
  int64_t off = 0;
  buf_[off++] = Protocol::SETTINGS_CMD;
  bool sendFileChunks = wdtParent_->isSendFileChunks();
  enableHeartBeat_ = false;
  if (options_.enable_heart_beat) {
    if (threadProtocolVersion_ < Protocol::HEART_BEAT_VERSION) {
      WTLOG(INFO) << "Disabling heart beat because of the receiver version is "
                  << threadProtocolVersion_;
    } else {
      enableHeartBeat_ = true;
    }
  }
  enableHeartBeat_ = (threadProtocolVersion_ >= Protocol::HEART_BEAT_VERSION &&
                      options_.enable_heart_beat);
  Settings settings;
  settings.readTimeoutMillis = readTimeoutMillis;
  settings.writeTimeoutMillis = writeTimeoutMillis;
  settings.transferId = wdtParent_->getTransferId();
  settings.enableChecksum = (footerType_ == CHECKSUM_FOOTER);
  settings.sendFileChunks = sendFileChunks;
  settings.blockModeDisabled = (options_.block_size_mbytes <= 0);
  settings.enableHeartBeat = enableHeartBeat_;
  Protocol::encodeSettings(threadProtocolVersion_, buf_, off,
                           Protocol::kMaxSettings, settings);
  int64_t toWrite = sendFileChunks ? Protocol::kMinBufLength : off;
  int64_t written = socket_->write(buf_, toWrite);
  if (written != toWrite) {
    WTLOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return CONNECT;
  }
  threadStats_.addHeaderBytes(toWrite);
  return (sendFileChunks ? READ_FILE_CHUNKS : SEND_BLOCKS);
}

const int kHeartBeatReadTimeFactor = 10;

ErrorCode SenderThread::readHeartBeats() {
  if (!enableHeartBeat_) {
    return OK;
  }
  const auto now = Clock::now();
  const int timeSinceLastHeartBeatMs = durationMillis(now - lastHeartBeatTime_);
  const int heartBeatIntervalMs =
      (options_.read_timeout_millis * kHeartBeatReadTimeFactor);
  if (timeSinceLastHeartBeatMs <= heartBeatIntervalMs) {
    return OK;
  }
  lastHeartBeatTime_ = now;
  // time to read heart-beats
  const int numRead = socket_->read(buf_, bufSize_,
                                    /* don't try to read all the data */ false);
  if (numRead <= 0) {
    WTLOG(ERROR) << "Failed to read heart-beat " << numRead;
    return SOCKET_READ_ERROR;
  }
  for (int i = 0; i < numRead; i++) {
    const char receivedCmd = buf_[i];
    if (receivedCmd != Protocol::HEART_BEAT_CMD) {
      WTLOG(ERROR) << "Received " << receivedCmd
                   << " instead of heart-beat cmd";
      return PROTOCOL_ERROR;
    }
  }
  if (!isTty_) {
    WTLOG(INFO) << "Received " << numRead << " heart-beats";
  }
  return OK;
}

SenderState SenderThread::sendBlocks() {
  WTVLOG(1) << "entered SEND_BLOCKS state";
  ThreadTransferHistory &transferHistory = getTransferHistory();
  if (threadProtocolVersion_ >= Protocol::RECEIVER_PROGRESS_REPORT_VERSION &&
      !totalSizeSent_ && dirQueue_->fileDiscoveryFinished()) {
    return SEND_SIZE_CMD;
  }
  ErrorCode transferStatus;
  std::unique_ptr<ByteSource> source =
      dirQueue_->getNextSource(threadCtx_.get(), transferStatus);
  if (!source) {
    // try to read any buffered heart-beats
    readHeartBeats();

    return SEND_DONE_CMD;
  }
  WDT_CHECK(!source->hasError());
  TransferStats transferStats = sendOneByteSource(source, transferStatus);
  threadStats_ += transferStats;
  source->addTransferStats(transferStats);
  source->close();
  if (!transferHistory.addSource(source)) {
    // global checkpoint received for this thread. no point in
    // continuing
    WTLOG(ERROR) << "global checkpoint received. Stopping";
    threadStats_.setLocalErrorCode(CONN_ERROR);
    return END;
  }
  if (transferStats.getLocalErrorCode() != OK) {
    return CHECK_FOR_ABORT;
  }
  return SEND_BLOCKS;
}

TransferStats SenderThread::sendOneByteSource(
    const std::unique_ptr<ByteSource> &source, ErrorCode transferStatus) {
  TransferStats stats;
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
  Protocol::encodeHeader(wdtParent_->getProtocolVersion(), headerBuf, off,
                         Protocol::kMaxHeader, blockDetails);
  int16_t littleEndianOff = folly::Endian::little((int16_t)off);
  folly::storeUnaligned<int16_t>(headerLenPtr, littleEndianOff);
  int64_t written = socket_->write(headerBuf, off);
  if (written != off) {
    WTPLOG(ERROR) << "Write error/mismatch " << written << " " << off
                  << ". fd = " << socket_->getFd()
                  << ". file = " << metadata.relPath
                  << ". port = " << socket_->getPort();
    stats.setLocalErrorCode(SOCKET_WRITE_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }

  stats.addHeaderBytes(written);
  int64_t byteSourceHeaderBytes = written;
  int64_t throttlerInstanceBytes = byteSourceHeaderBytes;
  int64_t totalThrottlerBytes = 0;
  WTVLOG(3) << "Sent " << written << " on " << socket_->getFd() << " : "
            << folly::humanify(std::string(headerBuf, off));
  int32_t checksum = 0;
  while (!source->finished()) {
    // TODO: handle protocol errors from readHeartBeats
    readHeartBeats();

    int64_t size;
    char *buffer = source->read(size);
    if (source->hasError()) {
      WTLOG(ERROR) << "Failed reading file " << source->getIdentifier()
                   << " for fd " << socket_->getFd();
      break;
    }
    WDT_CHECK(buffer && size > 0);
    if (footerType_ == CHECKSUM_FOOTER) {
      checksum = folly::crc32c((const uint8_t *)buffer, size, checksum);
    }
    if (wdtParent_->getThrottler()) {
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
      wdtParent_->getThrottler()->limit(*threadCtx_, throttlerInstanceBytes);
      totalThrottlerBytes += throttlerInstanceBytes;
      throttlerInstanceBytes = 0;
    }
    written = socket_->write(buffer, size, /* retry writes */ true);
    if (getThreadAbortCode() != OK) {
      WTLOG(ERROR) << "Transfer aborted during block transfer "
                   << socket_->getPort() << " " << source->getIdentifier();
      stats.setLocalErrorCode(ABORT);
      stats.incrFailedAttempts();
      return stats;
    }
    if (written != size) {
      WTLOG(ERROR) << "Write error " << written << " (" << size << ")"
                   << ". fd = " << socket_->getFd()
                   << ". file = " << metadata.relPath
                   << ". port = " << socket_->getPort();
      stats.setLocalErrorCode(SOCKET_WRITE_ERROR);
      stats.incrFailedAttempts();
      return stats;
    }
    stats.addDataBytes(written);
    actualSize += written;
  }
  if (actualSize != expectedSize) {
    // Can only happen if sender thread can not read complete source byte
    // stream
    WTLOG(ERROR) << "UGH " << source->getIdentifier() << " " << expectedSize
                 << " " << actualSize;
    struct stat fileStat;
    if (stat(metadata.fullPath.c_str(), &fileStat) != 0) {
      WTPLOG(ERROR) << "stat failed on path " << metadata.fullPath;
    } else {
      WTLOG(WARNING) << "file " << source->getIdentifier() << " previous size "
                     << metadata.size << " current size " << fileStat.st_size;
    }
    stats.setLocalErrorCode(BYTE_SOURCE_READ_ERROR);
    stats.incrFailedAttempts();
    return stats;
  }
  if (wdtParent_->getThrottler() && actualSize > 0) {
    WDT_CHECK(totalThrottlerBytes == actualSize + byteSourceHeaderBytes)
        << totalThrottlerBytes << " " << (actualSize + totalThrottlerBytes);
  }
  if (footerType_ != NO_FOOTER) {
    off = 0;
    headerBuf[off++] = Protocol::FOOTER_CMD;
    Protocol::encodeFooter(headerBuf, off, Protocol::kMaxFooter, checksum);
    int toWrite = off;
    written = socket_->write(headerBuf, toWrite);
    if (written != toWrite) {
      WTLOG(ERROR) << "Write mismatch " << written << " " << toWrite;
      stats.setLocalErrorCode(SOCKET_WRITE_ERROR);
      stats.incrFailedAttempts();
      return stats;
    }
    stats.addHeaderBytes(toWrite);
  }
  stats.setLocalErrorCode(OK);
  stats.incrNumBlocks();
  stats.addEffectiveBytes(stats.getHeaderBytes(), stats.getDataBytes());
  return stats;
}

SenderState SenderThread::sendSizeCmd() {
  WTVLOG(1) << "entered SEND_SIZE_CMD state";
  int64_t off = 0;
  buf_[off++] = Protocol::SIZE_CMD;

  Protocol::encodeSize(buf_, off, Protocol::kMaxSize,
                       dirQueue_->getTotalSize());
  int64_t written = socket_->write(buf_, off);
  if (written != off) {
    WTLOG(ERROR) << "Socket write error " << off << " " << written;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(off);
  totalSizeSent_ = true;
  return SEND_BLOCKS;
}

SenderState SenderThread::sendDoneCmd() {
  WTVLOG(1) << "entered SEND_DONE_CMD state";

  int64_t off = 0;
  buf_[off++] = Protocol::DONE_CMD;
  auto pair = dirQueue_->getNumBlocksAndStatus();
  int64_t numBlocksDiscovered = pair.first;
  ErrorCode transferStatus = pair.second;
  buf_[off++] = transferStatus;
  Protocol::encodeDone(threadProtocolVersion_, buf_, off, Protocol::kMaxDone,
                       numBlocksDiscovered, dirQueue_->getTotalSize());
  int toWrite = Protocol::kMinBufLength;
  int64_t written = socket_->write(buf_, toWrite);
  if (written != toWrite) {
    WTLOG(ERROR) << "Socket write failure " << written << " " << toWrite;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(toWrite);
  WTVLOG(1) << "Wrote done cmd on " << socket_->getFd()
            << " waiting for reply...";
  return READ_RECEIVER_CMD;
}

SenderState SenderThread::checkForAbort() {
  WTLOG(INFO) << "entered CHECK_FOR_ABORT state";
  auto numRead = socket_->read(buf_, 1);
  if (numRead != 1) {
    WTVLOG(1) << "No abort cmd found";
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
  if (cmd != Protocol::ABORT_CMD) {
    WTVLOG(1) << "Unexpected result found while reading for abort " << buf_[0];
    return CONNECT;
  }
  threadStats_.addHeaderBytes(1);
  return PROCESS_ABORT_CMD;
}

SenderState SenderThread::readFileChunks() {
  WTLOG(INFO) << "entered READ_FILE_CHUNKS state ";
  int64_t numRead = socket_->read(buf_, 1);
  if (numRead != 1) {
    WTLOG(ERROR) << "Socket read error 1 " << numRead;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(numRead);
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
  if (cmd == Protocol::ABORT_CMD) {
    return PROCESS_ABORT_CMD;
  }
  if (cmd == Protocol::WAIT_CMD) {
    return READ_FILE_CHUNKS;
  }
  if (cmd == Protocol::ACK_CMD) {
    if (!wdtParent_->isFileChunksReceived()) {
      WTLOG(ERROR) << "Sender has not yet received file chunks, but receiver "
                   << "thinks it has already sent it";
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return END;
    }
    return SEND_BLOCKS;
  }
  if (cmd == Protocol::LOCAL_CHECKPOINT_CMD) {
    ErrorCode errCode = readAndVerifySpuriousCheckpoint();
    if (errCode == SOCKET_READ_ERROR) {
      return CONNECT;
    }
    if (errCode == PROTOCOL_ERROR) {
      return END;
    }
    WDT_CHECK_EQ(OK, errCode);
    return READ_FILE_CHUNKS;
  }
  if (cmd != Protocol::CHUNKS_CMD) {
    WTLOG(ERROR) << "Unexpected cmd " << cmd;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return END;
  }
  int64_t toRead = Protocol::kChunksCmdLen;
  numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    WTLOG(ERROR) << "Socket read error " << toRead << " " << numRead;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(numRead);
  int64_t off = 0;
  int64_t bufSize, numFiles;
  Protocol::decodeChunksCmd(buf_, off, bufSize_, bufSize, numFiles);
  WTLOG(INFO) << "File chunk list has " << numFiles
              << " entries and is broken in buffers of length " << bufSize;
  if (bufSize < 0 || numFiles < 0) {
    WLOG(ERROR) << "Decoded bogus size for file chunks list bufSize = "
                << bufSize << " num files " << numFiles;
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return END;
  }
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
      WTLOG(ERROR) << "Number of file chunks received is more than the number "
                      "mentioned in CHUNKS_CMD "
                   << numFileChunks << " " << numFiles;
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return END;
    }
    if (numFileChunks == numFiles) {
      break;
    }
    toRead = sizeof(int32_t);
    numRead = socket_->read(buf_, toRead);
    if (numRead != toRead) {
      WTLOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    toRead = folly::loadUnaligned<int32_t>(buf_);
    toRead = folly::Endian::little(toRead);
    numRead = socket_->read(chunkBuffer.get(), toRead);
    if (numRead != toRead) {
      WTLOG(ERROR) << "Socket read error " << toRead << " " << numRead;
      threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
      return CHECK_FOR_ABORT;
    }
    threadStats_.addHeaderBytes(numRead);
    off = 0;
    // decode function below adds decoded file chunks to fileChunksInfoList
    bool success = Protocol::decodeFileChunksInfoList(
        chunkBuffer.get(), off, toRead, fileChunksInfoList);
    if (!success) {
      WTLOG(ERROR) << "Unable to decode file chunks list";
      threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
      return END;
    }
  }
  wdtParent_->setFileChunksInfo(fileChunksInfoList);
  // send ack for file chunks list
  buf_[0] = Protocol::ACK_CMD;
  int64_t toWrite = 1;
  int64_t written = socket_->write(buf_, toWrite);
  if (toWrite != written) {
    WTLOG(ERROR) << "Socket write error " << toWrite << " " << written;
    threadStats_.setLocalErrorCode(SOCKET_WRITE_ERROR);
    return CHECK_FOR_ABORT;
  }
  threadStats_.addHeaderBytes(written);
  return SEND_BLOCKS;
}

ErrorCode SenderThread::readNextReceiverCmd() {
  int numUnackedBytes = socket_->getUnackedBytes();
  int timeToClearSendBuffer = 0;
  Clock::time_point startTime = Clock::now();
  while (true) {
    int numRead = socket_->read(buf_, 1);
    if (numRead == 1) {
      return OK;
    }
    if (getThreadAbortCode() != OK) {
      return ABORT;
    }
    if (numRead == 0) {
      WTPLOG(ERROR) << "Got unexpected EOF, reconnecting";
      return SOCKET_READ_ERROR;
    }
    WDT_CHECK_LT(numRead, 0);
    ErrorCode errCode = socket_->getReadErrCode();
    WTLOG(ERROR) << "Failed to read receiver cmd " << numRead << " "
                 << errorCodeToStr(errCode);
    if (errCode != WDT_TIMEOUT) {
      // not timed out
      return SOCKET_READ_ERROR;
    }
    int curUnackedBytes = socket_->getUnackedBytes();
    if (numUnackedBytes < 0 || curUnackedBytes < 0) {
      WTLOG(ERROR) << "Failed to read number of unacked bytes, reconnecting";
      return SOCKET_READ_ERROR;
    }
    WDT_CHECK_GE(numUnackedBytes, curUnackedBytes);
    if (curUnackedBytes == 0) {
      timeToClearSendBuffer = durationMillis(Clock::now() - startTime);
      break;
    }
    if (curUnackedBytes == numUnackedBytes) {
      WTLOG(ERROR) << "Number of unacked bytes did not change, reconnecting "
                   << curUnackedBytes;
      return SOCKET_READ_ERROR;
    }
    WTLOG(INFO) << "Read receiver command failed, but number of unacked "
                   "bytes decreased, retrying socket read "
                << numUnackedBytes << " " << curUnackedBytes;
    numUnackedBytes = curUnackedBytes;
  }
  // we are assuming that sender and receiver tcp buffer sizes are same. So, we
  // expect another timeToClearSendBuffer milliseconds for receiver to clear its
  // buffer
  int readTimeout = timeToClearSendBuffer + options_.drain_extra_ms;
  WTLOG(INFO) << "Send buffer cleared in " << timeToClearSendBuffer
              << "ms, waiting for " << readTimeout
              << "ms for receiver buffer to clear";
  // readWithTimeout internally checks for abort periodically
  int numRead = socket_->readWithTimeout(buf_, 1, readTimeout);
  if (numRead != 1) {
    WTLOG(ERROR) << "Failed to read receiver cmd " << numRead;
    return SOCKET_READ_ERROR;
  }
  return OK;
}

SenderState SenderThread::readReceiverCmd() {
  WTVLOG(1) << "entered READ_RECEIVER_CMD state";

  ErrorCode errCode = readNextReceiverCmd();
  if (errCode != OK) {
    threadStats_.setLocalErrorCode(errCode);
    return CONNECT;
  }
  Protocol::CMD_MAGIC cmd = (Protocol::CMD_MAGIC)buf_[0];
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
  if (cmd == Protocol::LOCAL_CHECKPOINT_CMD) {
    errCode = readAndVerifySpuriousCheckpoint();
    if (errCode == SOCKET_READ_ERROR) {
      return CONNECT;
    }
    if (errCode == PROTOCOL_ERROR) {
      return END;
    }
    WDT_CHECK_EQ(OK, errCode);
    return READ_RECEIVER_CMD;
  }
  if (cmd == Protocol::HEART_BEAT_CMD) {
    return READ_RECEIVER_CMD;
  }
  WTLOG(ERROR) << "Read unexpected receiver cmd " << cmd << " port " << port_;
  threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
  return END;
}

ErrorCode SenderThread::readAndVerifySpuriousCheckpoint() {
  int checkpointLen =
      Protocol::getMaxLocalCheckpointLength(threadProtocolVersion_);
  int64_t toRead = checkpointLen - 1;
  int numRead = socket_->read(buf_ + 1, toRead);
  if (numRead != toRead) {
    WTLOG(ERROR) << "Could not read possible local checkpoint " << toRead << " "
                 << numRead << " " << port_;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return SOCKET_READ_ERROR;
  }
  int64_t offset = 0;
  std::vector<Checkpoint> checkpoints;
  if (Protocol::decodeCheckpoints(threadProtocolVersion_, buf_, offset,
                                  checkpointLen, checkpoints)) {
    if (checkpoints.size() == 1 && checkpoints[0].port == port_ &&
        checkpoints[0].numBlocks == 0 &&
        checkpoints[0].lastBlockReceivedBytes == 0) {
      // In a spurious local checkpoint, number of blocks and offset must both
      // be zero
      // Ignore the checkpoint
      WTLOG(WARNING)
          << "Received valid but unexpected local checkpoint, ignoring "
          << port_ << " checkpoint " << checkpoints[0];
      return OK;
    }
  }
  WTLOG(ERROR) << "Failed to verify spurious local checkpoint, port " << port_
               << " numRead " << numRead << " chkptsz " << checkpoints.size()
               << " chkplen " << checkpointLen;
  threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
  return PROTOCOL_ERROR;
}

SenderState SenderThread::processDoneCmd() {
  WTVLOG(1) << "entered PROCESS_DONE_CMD state";
  // DONE cmd implies that all the blocks sent till now is acked
  ThreadTransferHistory &transferHistory = getTransferHistory();
  transferHistory.markAllAcknowledged();

  // send ack for DONE
  buf_[0] = Protocol::DONE_CMD;
  socket_->write(buf_, 1);

  socket_->shutdownWrites();
  ErrorCode retCode = socket_->expectEndOfStream();
  if (retCode != OK) {
    WTLOG(WARNING) << "Logical EOF not found when expected "
                   << errorCodeToStr(retCode);
    threadStats_.setLocalErrorCode(retCode);
    return CONNECT;
  }
  WTVLOG(1) << "done with transfer, port " << port_;
  return END;
}

SenderState SenderThread::processWaitCmd() {
  WTLOG(INFO) << "entered PROCESS_WAIT_CMD state ";
  // similar to DONE, WAIT also verifies all the blocks
  ThreadTransferHistory &transferHistory = getTransferHistory();
  transferHistory.markAllAcknowledged();
  WTVLOG(1) << "received WAIT_CMD, port " << port_;
  return READ_RECEIVER_CMD;
}

SenderState SenderThread::processErrCmd() {
  WTLOG(INFO) << "entered PROCESS_ERR_CMD state";
  // similar to DONE, global checkpoint cmd also verifies all the blocks
  ThreadTransferHistory &transferHistory = getTransferHistory();
  transferHistory.markAllAcknowledged();
  int64_t toRead = sizeof(int16_t);
  int64_t numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    WTLOG(ERROR) << "read unexpected " << toRead << " " << numRead;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  int16_t checkpointsLen = folly::loadUnaligned<int16_t>(buf_);
  checkpointsLen = folly::Endian::little(checkpointsLen);
  char checkpointBuf[checkpointsLen];
  numRead = socket_->read(checkpointBuf, checkpointsLen);
  if (numRead != checkpointsLen) {
    WTLOG(ERROR) << "read unexpected " << checkpointsLen << " " << numRead;
    threadStats_.setLocalErrorCode(SOCKET_READ_ERROR);
    return CONNECT;
  }

  std::vector<Checkpoint> checkpoints;
  int64_t decodeOffset = 0;
  if (!Protocol::decodeCheckpoints(threadProtocolVersion_, checkpointBuf,
                                   decodeOffset, checkpointsLen, checkpoints)) {
    WTLOG(ERROR) << "checkpoint decode failure "
                 << folly::humanify(std::string(checkpointBuf, checkpointsLen));
    threadStats_.setLocalErrorCode(PROTOCOL_ERROR);
    return END;
  }
  for (auto &checkpoint : checkpoints) {
    WTLOG(INFO) << "Received global checkpoint " << checkpoint;
    transferHistoryController_->handleGlobalCheckpoint(checkpoint);
  }
  return SEND_BLOCKS;
}

SenderState SenderThread::processAbortCmd() {
  WTLOG(INFO) << "entered PROCESS_ABORT_CMD state ";
  ThreadTransferHistory &transferHistory = getTransferHistory();
  threadStats_.setLocalErrorCode(ABORT);
  int toRead = Protocol::kAbortLength;
  auto numRead = socket_->read(buf_, toRead);
  if (numRead != toRead) {
    // can not read checkpoint, but still must exit because of ABORT
    WTLOG(ERROR) << "Error while trying to read ABORT cmd " << numRead << " "
                 << toRead;
    return END;
  }
  int64_t offset = 0;
  int32_t negotiatedProtocol;
  ErrorCode remoteError;
  int64_t checkpoint;
  Protocol::decodeAbort(buf_, offset, bufSize_, negotiatedProtocol, remoteError,
                        checkpoint);
  threadStats_.setRemoteErrorCode(remoteError);
  std::string failedFileName = transferHistory.getSourceId(checkpoint);
  WTLOG(WARNING) << "Received abort on "
                 << " remote protocol version " << negotiatedProtocol
                 << " remote error code " << errorCodeToStr(remoteError)
                 << " file " << failedFileName << " checkpoint " << checkpoint;
  wdtParent_->abort(remoteError);
  if (remoteError == VERSION_MISMATCH) {
    if (Protocol::negotiateProtocol(
            negotiatedProtocol, threadProtocolVersion_) == negotiatedProtocol) {
      // sender can support this negotiated version
      negotiatedProtocol_ = negotiatedProtocol;
      return PROCESS_VERSION_MISMATCH;
    } else {
      WTLOG(ERROR) << "Sender can not support receiver version "
                   << negotiatedProtocol;
      threadStats_.setRemoteErrorCode(VERSION_INCOMPATIBLE);
    }
  }
  return END;
}

SenderState SenderThread::processVersionMismatch() {
  WTLOG(INFO) << "entered PROCESS_VERSION_MISMATCH state ";
  WDT_CHECK(threadStats_.getLocalErrorCode() == ABORT);
  auto negotiationStatus = wdtParent_->getNegotiationStatus();
  WDT_CHECK_NE(negotiationStatus, V_MISMATCH_FAILED)
      << "Thread should have ended in case of version mismatch";
  if (negotiationStatus == V_MISMATCH_RESOLVED) {
    WTLOG(WARNING) << "Protocol version already negotiated, but "
                      "transfer still aborted due to version mismatch";
    return END;
  }
  WDT_CHECK_EQ(negotiationStatus, V_MISMATCH_WAIT);
  // Need a barrier here to make sure all the negotiated protocol versions
  // have been collected
  auto barrier = controller_->getBarrier(VERSION_MISMATCH_BARRIER);
  barrier->execute();
  WTVLOG(1) << "cleared the protocol version barrier";
  auto execFunnel = controller_->getFunnel(VERSION_MISMATCH_FUNNEL);
  while (true) {
    auto status = execFunnel->getStatus();
    switch (status) {
      case FUNNEL_START: {
        WTLOG(INFO) << "started the funnel for version mismatch";
        wdtParent_->setProtoNegotiationStatus(V_MISMATCH_FAILED);
        if (transferHistoryController_->handleVersionMismatch() != OK) {
          execFunnel->notifySuccess();
          return END;
        }
        int negotiatedProtocol = 0;
        for (int threadProtocolVersion : wdtParent_->getNegotiatedProtocols()) {
          if (threadProtocolVersion > 0) {
            if (negotiatedProtocol > 0 &&
                negotiatedProtocol != threadProtocolVersion) {
              WTLOG(ERROR)
                  << "Different threads negotiated different protocols "
                  << negotiatedProtocol << " " << threadProtocolVersion;
              execFunnel->notifySuccess();
              return END;
            }
            negotiatedProtocol = threadProtocolVersion;
          }
        }
        WDT_CHECK_GT(negotiatedProtocol, 0);
        WLOG_IF(INFO, negotiatedProtocol != threadProtocolVersion_)
            << *this << " Changing protocol version to " << negotiatedProtocol
            << ", previous version " << threadProtocolVersion_;
        wdtParent_->setProtocolVersion(negotiatedProtocol);
        threadProtocolVersion_ = wdtParent_->getProtocolVersion();
        setFooterType();
        threadStats_.setRemoteErrorCode(OK);
        wdtParent_->setProtoNegotiationStatus(V_MISMATCH_RESOLVED);
        wdtParent_->clearAbort();
        execFunnel->notifySuccess();
        return CONNECT;
      }
      case FUNNEL_PROGRESS: {
        execFunnel->wait();
        break;
      }
      case FUNNEL_END: {
        negotiationStatus = wdtParent_->getNegotiationStatus();
        WDT_CHECK_NE(negotiationStatus, V_MISMATCH_WAIT);
        if (negotiationStatus == V_MISMATCH_FAILED) {
          return END;
        }
        if (negotiationStatus == V_MISMATCH_RESOLVED) {
          threadProtocolVersion_ = wdtParent_->getProtocolVersion();
          threadStats_.setRemoteErrorCode(OK);
          return CONNECT;
        }
      }
    }
  }
}

void SenderThread::setFooterType() {
  const int protocolVersion = wdtParent_->getProtocolVersion();
  if (protocolVersion >= Protocol::CHECKSUM_VERSION &&
      options_.enable_checksum) {
    footerType_ = CHECKSUM_FOOTER;
  } else {
    footerType_ = NO_FOOTER;
  }
}

void SenderThread::start() {
  Clock::time_point startTime = Clock::now();

  if (buf_ == nullptr) {
    WTLOG(ERROR) << "Unable to allocate buffer";
    threadStats_.setLocalErrorCode(MEMORY_ALLOCATION_ERROR);
    return;
  }

  setFooterType();

  controller_->executeAtStart([&]() { wdtParent_->startNewTransfer(); });
  SenderState state = CONNECT;

  while (state != END) {
    ErrorCode abortCode = getThreadAbortCode();
    if (abortCode != OK) {
      WTLOG(ERROR) << "Transfer aborted " << errorCodeToStr(abortCode);
      threadStats_.setLocalErrorCode(ABORT);
      if (abortCode == VERSION_MISMATCH) {
        state = PROCESS_VERSION_MISMATCH;
      } else {
        break;
      }
    }
    state = (this->*stateMap_[state])();
  }

  threadStats_.setTls(wdtParent_->transferRequest_.tls);
  EncryptionType encryptionType =
      (socket_ ? socket_->getEncryptionType() : ENC_NONE);
  threadStats_.setEncryptionType(encryptionType);
  double totalTime = durationSeconds(Clock::now() - startTime);
  WTLOG(INFO) << "Port " << port_ << " done. " << threadStats_
              << " Total throughput = "
              << threadStats_.getEffectiveTotalBytes() / totalTime / kMbToB
              << " Mbytes/sec";

  ThreadTransferHistory &transferHistory = getTransferHistory();
  transferHistory.markNotInUse();
  controller_->deRegisterThread(threadIndex_);
  controller_->executeAtEnd([&]() { wdtParent_->endCurTransfer(); });
  // Important to delete the socket before the thread dies for sub class
  // of clientsocket which have thread local data
  socket_ = nullptr;

  return;
}

int SenderThread::getPort() const {
  return port_;
}

int SenderThread::getNegotiatedProtocol() const {
  return negotiatedProtocol_;
}

ErrorCode SenderThread::init() {
  return OK;
}

void SenderThread::reset() {
  totalSizeSent_ = false;
  threadStats_.setLocalErrorCode(OK);
}

ErrorCode SenderThread::getThreadAbortCode() {
  ErrorCode globalAbortCode = wdtParent_->getCurAbortCode();
  if (globalAbortCode != OK) {
    return globalAbortCode;
  }
  if (getTransferHistory().isGlobalCheckpointReceived()) {
    return GLOBAL_CHECKPOINT_ABORT;
  }
  return OK;
}
}
}
