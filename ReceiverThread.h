/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <wdt/Receiver.h>
#include <wdt/WdtBase.h>
#include <wdt/WdtThread.h>
#include <wdt/util/ServerSocket.h>

namespace facebook {
namespace wdt {

class Receiver;
/**
 * Wdt receiver has logic to maintain the consistency of the
 * transfers through connection errors. All threads are run by the logic
 * defined as a state machine. These are the all the states in that
 * state machine
 */
enum ReceiverState {
  LISTEN,
  ACCEPT_FIRST_CONNECTION,
  ACCEPT_WITH_TIMEOUT,
  SEND_LOCAL_CHECKPOINT,
  READ_NEXT_CMD,
  PROCESS_FILE_CMD,
  PROCESS_SETTINGS_CMD,
  PROCESS_DONE_CMD,
  PROCESS_SIZE_CMD,
  SEND_FILE_CHUNKS,
  SEND_GLOBAL_CHECKPOINTS,
  SEND_DONE_CMD,
  SEND_ABORT_CMD,
  WAIT_FOR_FINISH_OR_NEW_CHECKPOINT,
  FINISH_WITH_ERROR,
  END
};

/**
 * This class represents a receiver thread. It contains
 * all the logic for a thread to bind on a port and
 * receive data from the wdt sender. All the receiver threads
 * share modules like threads controller, throttler etc
 */
class ReceiverThread : public WdtThread {
 public:
  /// Identifiers for the funnels that this thread will use
  enum RECEIVER_FUNNELS { SEND_FILE_CHUNKS_FUNNEL, NUM_FUNNELS };

  /// Identifiers for the condition variable wrappers used in the thread
  enum RECEIVER_CONDITIONS { WAIT_FOR_FINISH_OR_CHECKPOINT_CV, NUM_CONDITIONS };

  /// Identifiers for the barriers used in the thread
  enum RECEIVER_BARRIERS { NUM_BARRIERS };
  /**
   * Constructor for receiver thread.
   * @param wdtParent       Pointer back to the parent receiver for meta
   *                        information
   * @param threadIndex     Every thread is identified by unique index
   * @param port            Port this thread will listen on
   * @param controller      Thread controller for all the instances of the
   *                        receiver threads. All the receiver thread objects
   *                        need to share the same instance of the controller
   */
  ReceiverThread(Receiver *wdtParent, int threadIndex, int port,
                 ThreadsController *controller);

  /// Initializes the receiver thread before starting
  ErrorCode init() override;

  /**
   * In long running mode, we need to reset thread variables after each
   * session. Before starting each session, reset() has to called to do that.
   */
  void reset() override;

  /// Destructor of Receiver thread
  ~ReceiverThread() override;

  /// Get the port this receiver thread is listening on
  int32_t getPort() const override;

 private:
  /// Overloaded operator for printing thread info
  friend std::ostream &operator<<(std::ostream &os,
                                  const ReceiverThread &receiverThread);
  typedef ReceiverState (ReceiverThread::*StateFunction)();

  /// Parent shared among all the threads for meta information
  Receiver *wdtParent_;

  /**
   * Tries to listen/bind to port. If this fails, thread is considered failed.
   * Previous states : n/a (start state)
   * Next states : ACCEPT_FIRST_CONNECTION(success),
   *               FINISH_WITH_ERROR(failure)
   */
  ReceiverState listen();

  /**
   * Tries to accept first connection of a new session. Periodically checks
   * whether a new session has started or not. If a new session has started then
   * goes to ACCEPT_WITH_TIMEOUT state. Also does session initialization. In
   * joinable mode, tries to accept for a limited number of user specified
   * retries.
   * Previous states : LISTEN,
   *                   END(if in long running mode)
   * Next states : ACCEPT_WITH_TIMEOUT(if a new transfer has started and this
   *               thread has not received a connection),
   *               FINISH_WITH_ERROR(if did not receive a
   *               connection in specified number of retries),
   *               READ_NEXT_CMD(if a connection was received)
   */
  ReceiverState acceptFirstConnection();

  /**
   * Tries to accept a connection with timeout. There are 2 kinds of timeout. At
   * the beginning of the session, it uses accept window as the timeout. Later
   * when sender settings are known it uses max(readTimeOut, writeTimeout)) +
   * buffer(500) as the timeout.
   * Previous states : Almost all states(for any network errors during transfer,
   *                   we transition to this state),
   * Next states : READ_NEXT_CMD(if there are no previous errors and accept
   *               was successful),
   *               SEND_LOCAL_CHECKPOINT(if there were previous errors and
   *               accept was successful),
   *               FINISH_WITH_ERROR(if accept failed and
   *               transfer previously failed during SEND_DONE_CMD state. Thus
   *               case needs special handling to ensure that we do not mess up
   *               session variables),
   *               END(if accept fails otherwise)
   */
  ReceiverState acceptWithTimeout();
  /**
   * Sends local checkpoint to the sender. In case of previous error during
   * SEND_LOCAL_CHECKPOINT state, we send -1 as the checkpoint.
   * Previous states : ACCEPT_WITH_TIMEOUT
   * Next states : ACCEPT_WITH_TIMEOUT(if sending fails),
   *               SEND_DONE_CMD(if send is successful and we have previous
   *               SEND_DONE_CMD error),
   *               READ_NEXT_CMD(if send is successful otherwise)
   */
  ReceiverState sendLocalCheckpoint();
  /**
   * Reads next cmd and transitions to the state accordingly.
   * Previous states : SEND_LOCAL_CHECKPOINT,
   *                   ACCEPT_FIRST_CONNECTION,
   *                   ACCEPT_WITH_TIMEOUT,
   *                   PROCESS_SETTINGS_CMD,
   *                   PROCESS_FILE_CMD,
   *                   SEND_GLOBAL_CHECKPOINTS,
   * Next states : PROCESS_FILE_CMD,
   *               PROCESS_DONE_CMD,
   *               PROCESS_SETTINGS_CMD,
   *               PROCESS_SIZE_CMD,
   *               ACCEPT_WITH_TIMEOUT(in case of read failure),
   *               FINISH_WITH_ERROR(in case of protocol errors)
   */
  ReceiverState readNextCmd();
  /**
   * Processes file cmd. Logic of how we write the file to the destination
   * directory is defined here.
   * Previous states : READ_NEXT_CMD
   * Next states : READ_NEXT_CMD(success),
   *               FINISH_WITH_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure)
   */
  ReceiverState processFileCmd();
  /**
   * Processes settings cmd. Settings has a connection settings,
   * protocol version, transfer id, etc. For more info check Protocol.h
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               FINISH_WITH_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure),
   *               SEND_FILE_CHUNKS(If the sender wants to resume transfer)
   */
  ReceiverState processSettingsCmd();
  /**
   * Processes done cmd. Also checks to see if there are any new global
   * checkpoints or not
   * Previous states : READ_NEXT_CMD,
   * Next states : FINISH_WITH_ERROR(protocol error),
   *               WAIT_FOR_FINISH_OR_NEW_CHECKPOINT(success),
   *               SEND_GLOBAL_CHECKPOINTS(if there are global errors)
   */
  ReceiverState processDoneCmd();
  /**
   * Processes size cmd. Sets the value of totalSenderBytes_
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               FINISH_WITH_ERROR(protocol error)
   */
  ReceiverState processSizeCmd();
  /**
   * Sends file chunks that were received successfully in any previous transfer,
   * this is the first step in download resumption.
   * Checks to see if they have already been transferred or not.
   * If yes, send ACK. If some other thread is sending it, sends wait cmd
   * and checks again later. Otherwise, breaks the entire data into bufferSIze_
   * chunks and sends it.
   * Previous states: PROCESS_SETTINGS_CMD,
   * Next states : ACCEPT_WITH_TIMEOUT(network error),
   *               READ_NEXT_CMD(success)
   */
  ReceiverState sendFileChunks();
  /**
   * Sends global checkpoints to sender
   * Previous states : PROCESS_DONE_CMD,
   *                   FINISH_WITH_ERROR
   * Next states : READ_NEXT_CMD(success),
   *               ACCEPT_WITH_TIMEOUT(socket write failure)
   */
  ReceiverState sendGlobalCheckpoint();
  /**
   * Sends DONE to sender, also tries to read back ack. If anything fails during
   * this state, doneSendFailure_ thread variable is set. This flag makes the
   * state machine behave differently, effectively bypassing all session related
   * things.
   * Previous states : SEND_LOCAL_CHECKPOINT,
   *                   FINISH_WITH_ERROR
   * Next states : END(success),
   *               ACCEPT_WITH_TIMEOUT(failure)
   */
  ReceiverState sendDoneCmd();

  /**
   * Sends ABORT cmd back to the sender
   * Previous states : PROCESS_FILE_CMD
   * Next states : FINISH_WITH_ERROR
   */
  ReceiverState sendAbortCmd();

  /**
   * Internal implementation of waitForFinishOrNewCheckpoint
   * Returns :
   *  SEND_GLOBAL_CHECKPOINTS if there are checkpoints
   *  SEND_DONE_CMD if there are no checkpoints and
   *  there are no active threads
   *  WAIT_FOR_FINISH_OR_NEW_CHECKPOINT in all other cases
   */
  ReceiverState checkForFinishOrNewCheckpoints();

  /**
   * Waits for transfer to finish or new checkpoints. This state first
   * increments waitingThreadCount_. Then, it
   * waits till all the threads have finished. It sends periodic WAIT signal to
   * prevent sender from timing out. If a new checkpoint is found, we move to
   * SEND_GLOBAL_CHECKPOINTS state.
   * Previous states : PROCESS_DONE_CMD
   * Next states : SEND_DONE_CMD(all threads finished),
   *               SEND_GLOBAL_CHECKPOINTS(if new checkpoints are found),
   *               ACCEPT_WITH_TIMEOUT(if socket write fails)
   */
  ReceiverState waitForFinishOrNewCheckpoint();

  /**
   * Waits for transfer to finish. Only called when there is an error for the
   * thread. It adds a checkpoint to the global list of checkpoints if a
   * connection was received. It increments waitingWithErrorThreadCount_ and
   * waits till the session ends.
   * Previous states : Almost all states
   * Next states : END
   */
  ReceiverState finishWithError();

  /// marks a block a verified
  void markBlockVerified(const BlockDetails &blockDetails);

  /// verifies received blocks which are not already verified
  void markReceivedBlocksVerified();

  /// checks whether heart-beat is enabled, and whether it is time to send
  /// another heart-beat, and if yes, sends a heart-beat
  void sendHeartBeat();

  /// Mapping from receiver states to state functions
  static const StateFunction stateMap_[];

  /// Main entry point for the thread, starts the state machine
  void start() override;

  /**
   * Server socket object that provides functionality such as listen()
   * accept, read, write on the socket
   */
  std::unique_ptr<IServerSocket> socket_{nullptr};

  /// Marks the number of bytes already read in the buffer
  int64_t numRead_{0};

  /// Following two are markers to mark how much data has been read/parsed
  int64_t off_{0};
  int64_t oldOffset_{0};

  /// Number of checkpoints already transferred
  int checkpointIndex_{0};

  /// Checkpoints saved for this thread
  std::vector<Checkpoint> checkpoints_;

  /**
   * Pending value of checkpoint count. since write call success does not
   * gurantee actual transfer, we do not apply checkpoint count update after
   * the write. Only after receiving next cmd from sender, we apply the
   * update
   */
  int pendingCheckpointIndex_{0};

  /// read timeout for sender
  int64_t senderReadTimeout_{-1};

  /// write timeout for sender
  int64_t senderWriteTimeout_{-1};

  /// whether the transfer is in block mode or not
  bool isBlockMode_{true};

  /// Checkpoint local to the thread, updated regularly
  Checkpoint checkpoint_;

  /// whether settings have been received and verified for the current
  /// connection. This is used to determine round robin order for polling in
  /// the server socket
  bool curConnectionVerified_{false};

  /// Checkpoints that have not been sent back to the sender
  std::vector<Checkpoint> newCheckpoints_;

  /// list of received blocks which have not yet been verified
  std::vector<BlockDetails> blocksWaitingVerification_;
};
}
}
