/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "WdtBase.h"
#include "FileCreator.h"
#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "ServerSocket.h"
#include "Protocol.h"
#include "Writer.h"
#include "Throttler.h"
#include <memory>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace facebook {
namespace wdt {

class Receiver : public WdtBase {
 public:
  /// Constructor that only needs start port and number of ports
  Receiver(int startPort, int numPorts);

  /// Constructor which also takes the directory where receiver
  /// will be writing files
  Receiver(int port, int numSockets, const std::string &destDir);

  /// Starts listening on as many ports as possible from the ports
  /// provided in the constructor
  int32_t registerPorts(bool stopOnFailure = false);

  /**
   * Joins on the threads spawned by start. This method
   * is called by default when the wdt receiver is expected
   * to run as forever running process. However this has to
   * be explicitly called when the caller expects to conclude
   * a transfer. TODO: move to base?
   */
  std::unique_ptr<TransferReport> finish();

  /**
   * Call this method when you don't want the wdt receiver
   * to stop after a transfer.
   */
  ErrorCode runForever();

  /**
   * Starts the threads, and returns. Expects the caller to call
   * finish() after this method has been called.
   */
  ErrorCode transferAsync();

  /**
   * Setter for the directory where the files received are to
   * be written in
   */
  void setDir(const std::string &destDir);

  /// Get the dir where receiver is transferring
  const std::string &getDir();

  /// Get the unique id of this receiver
  const std::string &getReceiverId();

  /// @param receiverId   unique id of this receiver
  void setReceiverId(const std::string &receiverId);

  /// @param protocolVersion    protocol to use
  void setProtocolVersion(int protocolVersion);

  /**
   * Destructor for the receiver should try to join threads.
   * Since the threads are part of the object. We can't destroy the
   * object, since deleting the threads which are running will raise
   * an exception
   */
  virtual ~Receiver();

  /**
   * Take a lock on the instance mutex and return the value of
   * whether the existing transfer has been finished
   */
  bool hasPendingTransfer();

  /**
   * @param isFinished         Mark transfer active/inactive
   *
   */
  void markTransferFinished(bool isFinished);

  /**
   * Use the method to get a list of ports
   */
  std::vector<int32_t> getPorts();

 protected:
  /// receiver state
  enum ReceiverState {
    LISTEN,
    ACCEPT_FIRST_CONNECTION,
    ACCEPT_WITH_TIMEOUT,
    SEND_LOCAL_CHECKPOINT,
    READ_NEXT_CMD,
    PROCESS_FILE_CMD,
    PROCESS_EXIT_CMD,
    PROCESS_SETTINGS_CMD,
    PROCESS_DONE_CMD,
    PROCESS_SIZE_CMD,
    SEND_GLOBAL_CHECKPOINTS,
    SEND_DONE_CMD,
    SEND_ABORT_CMD,
    WAIT_FOR_FINISH_OR_NEW_CHECKPOINT,
    WAIT_FOR_FINISH_WITH_THREAD_ERROR,
    FAILED,
    END
  };

  /// structure to pass data to the state machine and also to share data between
  /// different state
  struct ThreadData {
    const int threadIndex_;
    ServerSocket &socket_;
    TransferStats &threadStats_;

    std::unique_ptr<char[]> buf_;
    const size_t bufferSize_;
    ssize_t numRead_{0};
    size_t off_{0};
    size_t oldOffset_{0};

    /// number of checkpoints already transferred
    int checkpointIndex_{0};
    /// pending value of checkpoint count. since write call success does not
    /// gurantee actual transfer, we do not apply checkpoint count update after
    /// the write. Only after receiving next cmd from sender, we apply the
    /// update
    int pendingCheckpointIndex_{0};

    /// a counter incremented each time a new session starts for this thread
    uint64_t transferStartedCount_{0};

    /// a counter incremented each time a new session ends for this thread
    uint64_t transferFinishedCount_{0};

    /// read timeout for sender
    int64_t senderReadTimeout_{-1};

    /// write timeout for sender
    int64_t senderWriteTimeout_{-1};

    /// whether checksum verification is enabled or not
    bool enableChecksum_{false};

    /// whether SEND_DONE_CMD state has already failed for this session or not.
    /// This has to be separately handled, because session barrier is
    /// implemented before sending done cmd
    bool doneSendFailure_{false};

    std::vector<Checkpoint> newCheckpoints_;

    ThreadData(int threadIndex, ServerSocket &socket,
               TransferStats &threadStats, size_t bufferSize)
        : threadIndex_(threadIndex),
          socket_(socket),
          threadStats_(threadStats),
          bufferSize_(bufferSize) {
      buf_.reset(new char[bufferSize_]);
    }

    /// In long running mode, we need to reset thread variables after each
    /// session. Before starting each session, reset() has to called to do that.
    void reset() {
      numRead_ = off_ = 0;
      checkpointIndex_ = pendingCheckpointIndex_ = 0;
      doneSendFailure_ = false;
      senderReadTimeout_ = senderWriteTimeout_ = -1;
      threadStats_.reset();
    }

    char *getBuf() {
      return buf_.get();
    }
  };

  typedef ReceiverState (Receiver::*StateFunction)(ThreadData &data);

  /**
   * Tries to listen/bind to port. If this fails, thread is considered failed.
   * Previous states : n/a (start state)
   * Next states : ACCEPT_FIRST_CONNECTION(success),
   *               FAILED(failure)
   */
  ReceiverState listen(ThreadData &data);
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
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(if did not receive a
   *               connection in specified number of retries),
   *               READ_NEXT_CMD(if a connection was received)
   */
  ReceiverState acceptFirstConnection(ThreadData &data);
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
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(if accept failed and
   *               transfer previously failed during SEND_DONE_CMD state. Thus
   *               case needs special handling to ensure that we do not mess up
   *               session variables),
   *               END(if accept fails otherwise)
   */
  ReceiverState acceptWithTimeout(ThreadData &data);
  /**
   * sends local checkpoint to the sender. In case of previous error during
   * SEND_LOCAL_CHECKPOINT state, we send -1 as the checkpoint.
   * Previous states : ACCEPT_WITH_TIMEOUT
   * Next states : ACCEPT_WITH_TIMEOUT(if sending fails),
   *               SEND_DONE_CMD(if send is successful and we have previous
   *               SEND_DONE_CMD error),
   *               READ_NEXT_CMD(if send is successful otherwise)
   */
  ReceiverState sendLocalCheckpoint(ThreadData &data);
  /**
   * Reads next cmd.
   * Previous states : SEND_LOCAL_CHECKPOINT,
   *                   ACCEPT_FIRST_CONNECTION,
   *                   ACCEPT_WITH_TIMEOUT,
   *                   PROCESS_SETTINGS_CMD,
   *                   PROCESS_FILE_CMD,
   *                   SEND_GLOBAL_CHECKPOINTS,
   * Next states : PROCESS_FILE_CMD,
   *               PROCESS_EXIT_CMD,
   *               PROCESS_DONE_CMD,
   *               PROCESS_SETTINGS_CMD,
   *               PROCESS_SIZE_CMD,
   *               ACCEPT_WITH_TIMEOUT(in case of read failure),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(in case of protocol errors)
   */
  ReceiverState readNextCmd(ThreadData &data);
  /**
   * processes exit cmd
   * Previous states : READ_NEXT_CMD
   * Next states :
   */
  ReceiverState processExitCmd(ThreadData &data);
  /**
   * processes file cmd
   * Previous states : READ_NEXT_CMD
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure)
   */
  ReceiverState processFileCmd(ThreadData &data);
  /**
   * processes settings cmd
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure)
   */
  ReceiverState processSettingsCmd(ThreadData &data);
  /**
   * processes done cmd. Also checks to see if there are any new global
   * checkpoints or not
   * Previous states : READ_NEXT_CMD,
   * Next states : WAIT_FOR_FINISH_OR_ERROR(protocol error),
   *               WAIT_FOR_FINISH_OR_NEW_CHECKPOINT(success),
   *               SEND_GLOBAL_CHECKPOINTS(if there are global errors)
   */
  ReceiverState processDoneCmd(ThreadData &data);
  /**
   * processes size cmd. Sets the value of totalSenderBytes_
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error)
   */
  ReceiverState processSizeCmd(ThreadData &data);
  /**
   * sends global checkpoints to sender
   * Previous states : PROCESS_DONE_CMD,
   *                   WAIT_FOR_FINISH_OR_ERROR
   * Next states : READ_NEXT_CMD(success),
   *               ACCEPT_WITH_TIMEOUT(socket write failure)
   */
  ReceiverState sendGlobalCheckpoint(ThreadData &data);
  /**
   * sends DONE to sender, also tries to read back ack. If anything fails during
   * this state, doneSendFailure_ thread variable is set. This flag makes the
   * state machine behave differently, effectively bypassing all session related
   * things.
   * Previous states : SEND_LOCAL_CHECKPOINT,
   *                   WAIT_FOR_FINISH_OR_ERROR
   * Next states : END(success),
   *               ACCEPT_WITH_TIMEOUT(failure)
   */
  ReceiverState sendDoneCmd(ThreadData &data);

  /**
   * Sends ABORT cmd back to the sender
   * Previous states : PROCESS_FILE_CMD
   * Next states : WAIT_FOR_FINISH_WITH_THREAD_ERROR
   */
  ReceiverState sendAbortCmd(ThreadData &data);

  /**
   * waits for transfer to finish or new checkpoints. This state first
   * increments waitingThreadCount_. Then, it
   * waits till all the threads have finished. It sends periodic WAIT signal to
   * prevent sender from timing out. If a new checkpoint is found, we move to
   * SEND_GLOBAL_CHECKPOINTS state.
   * Previous states : PROCESS_DONE_CMD
   * Next states : SEND_DONE_CMD(all threads finished),
   *               SEND_GLOBAL_CHECKPOINTS(if new checkpoints are found),
   *               ACCEPT_WITH_TIMEOUT(if socket write fails)
   */
  ReceiverState waitForFinishOrNewCheckpoint(ThreadData &data);

  /**
   * waits for transfer to finish. Only called when there is an error for the
   * thread. It adds a checkpoint to the global list of checkpoints if a
   * connection was received. It increases waitingWithErrorThreadCount_ and
   * waits till session end.
   * Previous states : Almost all states
   * Next states : END
   */
  ReceiverState waitForFinishWithThreadError(ThreadData &data);

  /// mapping from receiver states to state functions
  static const StateFunction stateMap_[];

  /**
   * Responsible for basic setup and starting threads
   */
  void start();

  void receiveOne(int threadIndex, ServerSocket &s, size_t bufferSize,
                  TransferStats &threadStats);

  /**
   * Periodically calculates current transfer report and send it to progress
   * reporter. This only works in the single transfer mode.
   */
  void progressTracker();

  /**
   * adds a checkpoint to the global checkpoint list
   * @param checkpoint    checkpoint to be added
   */
  void addCheckpoint(Checkpoint checkpoint);

  /**
   * @param startIndex    number of checkpoints already transferred by the
   *                      calling thread
   * @return              list of new checkpoints
   */
  std::vector<Checkpoint> getNewCheckpoints(int startIndex);

  /// returns true if all threads finished for this session
  bool areAllThreadsFinished(bool checkpointAdded);

  /// ends current global session
  void endCurGlobalSession();

  /// whether a new session has started and the thread is not aware of it
  /// must hold lock on mutex_ before calling this
  bool hasNewSessionStarted(ThreadData &data);

  /// start new transfer by incrementing transferStartedCount_
  /// must hold lock on mutex_ before calling this
  void startNewGlobalSession();

  /// whether the current session has finished or not.
  /// must hold lock on mutex_ before calling this
  bool hasCurSessionFinished(ThreadData &data);

  /// starts a new session for the thread
  void startNewThreadSession(ThreadData &data);

  /// ends current thread session
  void endCurThreadSession(ThreadData &data);

  /// Auto configure the throttler if none set externally
  void configureThrottler();

  /**
   * increments failed thread count, does not wait for transfer to finish
   */
  void incrFailedThreadCountAndCheckForSessionEnd(ThreadData &data);

  /// Get transfer report, meant to be called after threads have been finished
  /// Method is not thread safe
  std::unique_ptr<TransferReport> getTransferReport();

  /// The thread that is responsible for calling running the progress tracker
  std::thread progressTrackerThread_;
  /**
   * Flag set by the finish() method when the receiver threads are joined.
   * No transfer can be started as long as this flag is false.
   */
  bool transferFinished_{true};
  /// Flag based on which threads finish processing on receiving a done
  bool isJoinable_;
  /// Destination directory where the received files will be written
  std::string destDir_;
  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_;

  /// Unique id of this receiver object, this must match sender-id sent as part
  /// of settings
  std::string receiverId_;
  /// protocol version to use, this is determined by negotiating protocol
  /// version with the other side
  int protocolVersion_{Protocol::protocol_version};

  /**
   * Progress tracker thread is a thread which has to be joined when the
   * transfer is finished. The root thread in finish() and the progress
   * tracker coordinate with each other through the boolean and this
   * condition variable.
   */
  std::condition_variable conditionRecvFinished_;
  /**
   * The instance of the receiver threads are stored in this vector.
   * This will not be destroyed until this object is destroyed, hence
   * it has to be made sure that these threads are joined at least before
   * the destruction of this object.
   */
  std::vector<std::thread> receiverThreads_;
  /**
   * start() gives each thread the instance of the serversocket, these
   * sockets can be closed and changed completely by the progress tracker
   * thread thus ending any hope of receiver threads doing any further
   * successful transfer
   */
  std::vector<ServerSocket> threadServerSockets_;
  /// Bunch of stats objects given to each thread by the root thread
  /// so that finish() can summarize the result at the end of joining.
  std::vector<TransferStats> threadStats_;

  /// per thread perf report
  std::vector<PerfStatReport> perfReports_;

  /// number of blocks send by the sender
  int64_t numBlocksSend_{-1};

  /// global list of checkpoints
  std::vector<Checkpoint> checkpoints_;

  /// number of threads which finished transfer
  int failedThreadCount_{0};

  /// number of threads which are waiting for finish or new checkpoint
  int waitingThreadCount_{0};

  /// number of threads which are waiting with an error
  int waitingWithErrorThreadCount_{0};

  /// a counter incremented each time a new session starts
  uint64_t transferStartedCount_{0};

  /// a counter incremented each time a new session ends
  uint64_t transferFinishedCount_{0};

  /// total number of data bytes sender wants to transfer
  int64_t totalSenderBytes_{-1};

  /// start time of the session
  std::chrono::time_point<Clock> startTime_;

  /// mutex to guard all the shared variables
  mutable std::mutex mutex_;

  /// condition variable to coordinate transfer finish
  mutable std::condition_variable conditionAllFinished_;

  /// Have threads been joined
  bool areThreadsJoined_{false};

  /// Number of active threads, decremented every time a thread is finished
  uint32_t numActiveThreads_{0};

  /// Mutex for the management of this instance, specifically to keep the
  /// instance sane for multi threaded public API calls
  std::mutex instanceManagementMutex_;
};
}
}  // namespace facebook::wdt
