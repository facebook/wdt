/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <thread>
#include "WdtThread.h"
#include "Sender.h"
#include "ClientSocket.h"
#include "ThreadTransferHistory.h"
namespace facebook {
namespace wdt {
class DirectorySourceQueue;
/// state machine states
enum SenderState {
  CONNECT,
  READ_LOCAL_CHECKPOINT,
  SEND_SETTINGS,
  SEND_BLOCKS,
  SEND_DONE_CMD,
  SEND_SIZE_CMD,
  CHECK_FOR_ABORT,
  READ_FILE_CHUNKS,
  READ_RECEIVER_CMD,
  PROCESS_DONE_CMD,
  PROCESS_WAIT_CMD,
  PROCESS_ERR_CMD,
  PROCESS_ABORT_CMD,
  PROCESS_VERSION_MISMATCH,
  END
};

/**
 * This class represents one sender thread. It contains all the
 * functionalities that a thread would need to send data over
 * a connection to the receiver.
 * All the sender threads share bunch of modules like directory queue,
 * throttler, threads controller etc
 */
class SenderThread : public WdtThread {
 public:
  /// Identifers for the barriers used in the thread
  enum SENDER_BARRIERS { VERSION_MISMATCH_BARRIER, NUM_BARRIERS };

  /// Identifiers for the funnels used in the thread
  enum SENDER_FUNNELS { VERSION_MISMATCH_FUNNEL, NUM_FUNNELS };

  /// Identifier for the condition wrappers used in the thread
  enum SENDER_CONDITIONS { NUM_CONDITIONS };

  /// abort checker passed to client sockets. This checks both global sender
  /// abort and whether global checkpoint has been received or not
  class SocketAbortChecker : public WdtBase::AbortChecker {
   public:
    explicit SocketAbortChecker(WdtBase *wdtBase,
                                ThreadTransferHistory &transferHistory)
        : AbortChecker(wdtBase), transferHistory_(transferHistory) {
    }

    bool shouldAbort() const {
      return AbortChecker::shouldAbort() ||
             transferHistory_.isGlobalCheckpointReceived();
    }

   private:
    ThreadTransferHistory &transferHistory_;
  };

  /// Constructor for the sender thread
  SenderThread(Sender *sender, int threadIndex, int32_t port,
               ThreadsController *threadsController)
      : WdtThread(threadIndex, sender->getProtocolVersion(), threadsController),
        wdtParent_(sender),
        port_(port),
        dirQueue_(sender->dirQueue_.get()),
        transferHistoryController_(sender->transferHistoryController_.get()) {
    controller_->registerThread(threadIndex_);
    transferHistoryController_->addThreadHistory(port_, threadStats_);
    socketAbortChecker_ =
        folly::make_unique<SocketAbortChecker>(sender, getTransferHistory());
    threadStats_.setId(folly::to<std::string>(threadIndex_));
  }

  typedef SenderState (SenderThread::*StateFunction)();

  /// Returns the neogtiated protocol
  int getNegotiatedProtocol() const override;

  /// Steps to do ebfore calling start
  ErrorCode init() override;

  /// Reset the sender thread
  void reset() override;

  /// Get the port sender thread is connecting to
  int getPort() const override;

  /// Destructor of the sender thread
  ~SenderThread() {
  }

 private:
  /// Overloaded operator for printing thread info
  friend std::ostream &operator<<(std::ostream &os,
                                  const SenderThread &senderThread);

  /// Parent shared among all the threads for meta information
  Sender *wdtParent_;

  /// Special abort checker for the client socket
  std::unique_ptr<SocketAbortChecker> socketAbortChecker_{nullptr};

  /// The main entry point of the thread
  void start() override;

  /// Get the local transfer history
  ThreadTransferHistory &getTransferHistory() {
    return transferHistoryController_->getTransferHistory(port_);
  }

  /**
   * tries to connect to the receiver
   * Previous states : Almost all states(in case of network errors, all states
   *                   move to this state)
   * Next states : SEND_SETTINGS(if there is no previous error)
   *               READ_LOCAL_CHECKPOINT(if there is previous error)
   *               END(failed)
   */
  SenderState connect();
  /**
   * tries to read local checkpoint and return unacked sources to queue. If the
   * checkpoint value is -1, then we know previous attempt to send DONE had
   * failed. So, we move to READ_RECEIVER_CMD state.
   * Previous states : CONNECT
   * Next states : CONNECT(read failure),
   *               END(protocol error or global checkpoint found),
   *               READ_RECEIVER_CMD(if checkpoint is -1),
   *               SEND_SETTINGS(success)
   */
  SenderState readLocalCheckPoint();
  /**
   * sends sender settings to the receiver
   * Previous states : READ_LOCAL_CHECKPOINT,
   *                   CONNECT
   * Next states : SEND_BLOCKS(success),
   *               CONNECT(failure)
   */
  SenderState sendSettings();
  /**
   * sends blocks to receiver till the queue is not empty. After transferring a
   * block, we add it to the history. While adding to history, if it is found
   * that global checkpoint has been received for this thread, we move to END
   * state.
   * Previous states : SEND_SETTINGS,
   *                   PROCESS_ERR_CMD
   * Next states : SEND_BLOCKS(success),
   *               END(global checkpoint received),
   *               CHECK_FOR_ABORT(socket write failure),
   *               SEND_DONE_CMD(no more blocks left to transfer)
   */
  SenderState sendBlocks();
  /**
   * sends DONE cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CONNECT(failure),
   *               READ_RECEIVER_CMD(success)
   */
  SenderState sendDoneCmd();
  /**
   * sends size cmd to the receiver
   * Previous states : SEND_BLOCKS
   * Next states : CHECK_FOR_ABORT(failure),
   *               SEND_BLOCKS(success)
   */
  SenderState sendSizeCmd();
  /**
   * checks to see if the receiver has sent ABORT or not
   * Previous states : SEND_BLOCKS,
   *                   SEND_DONE_CMD
   * Next states : CONNECT(no ABORT cmd),
   *               END(protocol error),
   *               PROCESS_ABORT_CMD(read ABORT cmd)
   */
  SenderState checkForAbort();
  /**
   * reads previously transferred file chunks list. If it receives an ACK cmd,
   * then it moves on. If wait cmd is received, it waits. Otherwise reads the
   * file chunks and when done starts directory queue thread.
   * Previous states : SEND_SETTINGS,
   * Next states: READ_FILE_CHUNKS(if wait cmd is received),
   *              CHECK_FOR_ABORT(network error),
   *              END(protocol error),
   *              SEND_BLOCKS(success)
   *
   */
  SenderState readFileChunks();
  /**
   * reads receiver cmd
   * Previous states : SEND_DONE_CMD
   * Next states : PROCESS_DONE_CMD,
   *               PROCESS_WAIT_CMD,
   *               PROCESS_ERR_CMD,
   *               END(protocol error),
   *               CONNECT(failure)
   */
  SenderState readReceiverCmd();
  /**
   * handles DONE cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processDoneCmd();
  /**
   * handles WAIT cmd
   * Previous states : READ_RECEIVER_CMD
   * Next states : READ_RECEIVER_CMD
   */
  SenderState processWaitCmd();
  /**
   * reads list of global checkpoints and returns unacked sources to queue.
   * Previous states : READ_RECEIVER_CMD
   * Next states : CONNECT(socket read failure)
   *               END(checkpoint list decode failure),
   *               SEND_BLOCKS(success)
   */
  SenderState processErrCmd();
  /**
   * processes ABORT cmd
   * Previous states : CHECK_FOR_ABORT,
   *                   READ_RECEIVER_CMD
   * Next states : END
   */
  SenderState processAbortCmd();

  /**
   * waits for all active threads to be aborted, checks to see if the abort was
   * due to version mismatch. Also performs various sanity checks.
   * Previous states : Almost all threads, abort flags is checked between every
   *                   state transition
   * Next states : CONNECT(Abort was due to version kismatch),
   *               END(if abort was not due to version mismatch or some sanity
   *               check failed)
   */
  SenderState processVersionMismatch();

  /// mapping from sender states to state functions
  static const StateFunction stateMap_[];

  /// Port number of this sender thread
  const int32_t port_;

  /// Negotiated protocol of the sender thread
  int negotiatedProtocol_{-1};

  /// Pointer to client socket to maintain connection to the receiver
  std::unique_ptr<ClientSocket> socket_;

  /// Buffer used by the sender thread to read/write data
  char buf_[Protocol::kMinBufLength];

  /// whether total file size has been sent to the receiver
  bool totalSizeSent_{false};

  /// number of consecutive reconnects without any progress
  int numReconnectWithoutProgress_{0};

  /// Point to the directory queue of parent sender
  DirectorySourceQueue *dirQueue_;

  /// Thread history controller shared across all threads
  TransferHistoryController *transferHistoryController_;
};
}
}
