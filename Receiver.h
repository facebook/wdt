/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
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
#include "TransferLogManager.h"
#include <memory>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <chrono>

namespace facebook {
namespace wdt {
/**
 * Receiver is the receiving side of the transfer. Receiver listens on ports
 * accepts connections, receives the files and writes to the destination
 * directory. Receiver has two modes of operation : You can spawn a receiver
 * for one transfer or alternatively it can also be used in a long running
 * mode where it accepts subsequent transfers and runs in an infinte loop.
 */
class Receiver : public WdtBase {
 public:
  /// Constructor using wdt transfer request (@see in WdtBase.h)
  explicit Receiver(const WdtTransferRequest &transferRequest);

  /**
   * Constructor with start port, number of ports and directory to write to.
   * If the start port is specified as zero, it auto configures the ports
   */
  Receiver(int port, int numSockets, const std::string &destDir);

  /// Setup before starting (@see WdtBase.h)
  WdtTransferRequest init() override;

  /**
   * Joins on the threads spawned by start. This method
   * is called by default when the wdt receiver is expected
   * to run as forever running process. However this has to
   * be explicitly called when the caller expects to conclude
   * a transfer.
   */
  std::unique_ptr<TransferReport> finish() override;

  /**
   * Call this method instead of transferAsync() when you don't
   * want the wdt receiver to stop after one transfer.
   */
  ErrorCode runForever();

  /**
   * Starts the threads, and returns. Caller should call finish() after
   * calling this method to get the statistics of the transfer.
   */
  ErrorCode transferAsync() override;

  /// Setter for the directory where the files received are written to
  void setDir(const std::string &destDir);

  /// Get the dir where receiver is transferring
  const std::string &getDir();

  /// @param recoveryId   unique-id used to verify transfer log
  void setRecoveryId(const std::string &recoveryId);

  /**
   * Destructor for the receiver. The destructor automatically cancels
   * any incomplete transfers that are going on. 'Incomplete transfer' is a
   * transfer where there is no receiver thread that has received
   * confirmation from wdt sender that the transfer is 'DONE'. Destructor also
   * internally calls finish() for every transfer if finish() wasn't called
   */
  virtual ~Receiver();

  /**
   * Take a lock on the instance mutex and return the value of
   * whether the existing transfer has been finished
   */
  bool hasPendingTransfer();

  /**
   * Use the method to get the list of ports receiver is listening on
   */
  std::vector<int32_t> getPorts() const;

 protected:
  /**
   * @param isFinished         Mark transfer active/inactive
   */
  void markTransferFinished(bool isFinished);

  /**
   * Wdt receiver has logic to maintain the consistency of the
   * the transfers through connection errors. All threads are run by the logic
   * defined as a state machine. These are the all the stataes in that
   * state machine
   */
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
    SEND_FILE_CHUNKS,
    SEND_GLOBAL_CHECKPOINTS,
    SEND_DONE_CMD,
    SEND_ABORT_CMD,
    WAIT_FOR_FINISH_OR_NEW_CHECKPOINT,
    WAIT_FOR_FINISH_WITH_THREAD_ERROR,
    FAILED,
    END
  };

  /**
   * Structure to pass data to the state machine and also to share data between
   * different state
   */
  struct ThreadData {
    /// Index of the thread that this data belongs to
    const int threadIndex_;

    /**
     * Server socket object that provides functionality such as listen()
     * accept, read, write on the socket
     */
    ServerSocket &socket_;

    /// Statistics of the transfer for this thread
    TransferStats &threadStats_;

    /// protocol version for this thread. This per thread protocol version is
    /// kept separately from the global one to avoid locking
    int threadProtocolVersion_;

    /// Buffer that receivers reads data into from the network
    std::unique_ptr<char[]> buf_;
    /// Maximum size of the buffer
    const int64_t bufferSize_;

    /// Marks the number of bytes already read in the buffer
    int64_t numRead_{0};

    /// Following two are markers to mark how much data has been read/parsed
    int64_t off_{0};
    int64_t oldOffset_{0};

    /// number of checkpoints already transferred
    int checkpointIndex_{0};

    /**
     * Pending value of checkpoint count. since write call success does not
     * gurantee actual transfer, we do not apply checkpoint count update after
     * the write. Only after receiving next cmd from sender, we apply the
     * update
     */
    int pendingCheckpointIndex_{0};

    /// a counter incremented each time a new session starts for this thread
    int64_t transferStartedCount_{0};

    /// a counter incremented each time a new session ends for this thread
    int64_t transferFinishedCount_{0};

    /// read timeout for sender
    int64_t senderReadTimeout_{-1};

    /// write timeout for sender
    int64_t senderWriteTimeout_{-1};

    /// whether checksum verification is enabled or not
    bool enableChecksum_{false};

    /**
     * Whether SEND_DONE_CMD state has already failed for this session or not.
     * This has to be separately handled, because session barrier is
     * implemented before sending done cmd
     */
    bool doneSendFailure_{false};

    /// Checkpoints that have not been sent back to the sender
    std::vector<Checkpoint> newCheckpoints_;

    /// Constructor for thread data
    ThreadData(int threadIndex, ServerSocket &socket,
               TransferStats &threadStats, int protocolVersion,
               int64_t bufferSize)
        : threadIndex_(threadIndex),
          socket_(socket),
          threadStats_(threadStats),
          threadProtocolVersion_(protocolVersion),
          bufferSize_(bufferSize) {
      buf_.reset(new char[bufferSize_]);
    }

    /**
     * In long running mode, we need to reset thread variables after each
     * session. Before starting each session, reset() has to called to do that.
     */
    void reset() {
      numRead_ = off_ = 0;
      checkpointIndex_ = pendingCheckpointIndex_ = 0;
      doneSendFailure_ = false;
      senderReadTimeout_ = senderWriteTimeout_ = -1;
      threadStats_.reset();
    }

    /// Get the raw pointer to the buffer
    char *getBuf() {
      return buf_.get();
    }
  };

  /// Overloaded operator for printing thread info
  friend std::ostream &operator<<(std::ostream &os, const ThreadData &data);

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
   * Sends local checkpoint to the sender. In case of previous error during
   * SEND_LOCAL_CHECKPOINT state, we send -1 as the checkpoint.
   * Previous states : ACCEPT_WITH_TIMEOUT
   * Next states : ACCEPT_WITH_TIMEOUT(if sending fails),
   *               SEND_DONE_CMD(if send is successful and we have previous
   *               SEND_DONE_CMD error),
   *               READ_NEXT_CMD(if send is successful otherwise)
   */
  ReceiverState sendLocalCheckpoint(ThreadData &data);
  /**
   * Reads next cmd and transistions to the state accordingly.
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
   * Processes exit cmd. Wdt standalone or any application will exit after
   * reaching this state
   * Previous states : READ_NEXT_CMD
   * Next states :
   */
  ReceiverState processExitCmd(ThreadData &data);
  /**
   * Processes file cmd. Logic of how we write the file to the destination
   * directory is defined here.
   * Previous states : READ_NEXT_CMD
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure)
   */
  ReceiverState processFileCmd(ThreadData &data);
  /**
   * Processes settings cmd. Settings has a connection settings,
   * protocol version, transfer id, etc. For more info check Protocol.h
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error),
   *               ACCEPT_WITH_TIMEOUT(socket read failure),
   *               SEND_FILE_CHUNKS(If the sender wants to resume transfer)
   */
  ReceiverState processSettingsCmd(ThreadData &data);
  /**
   * Processes done cmd. Also checks to see if there are any new global
   * checkpoints or not
   * Previous states : READ_NEXT_CMD,
   * Next states : WAIT_FOR_FINISH_OR_ERROR(protocol error),
   *               WAIT_FOR_FINISH_OR_NEW_CHECKPOINT(success),
   *               SEND_GLOBAL_CHECKPOINTS(if there are global errors)
   */
  ReceiverState processDoneCmd(ThreadData &data);
  /**
   * Processes size cmd. Sets the value of totalSenderBytes_
   * Previous states : READ_NEXT_CMD,
   * Next states : READ_NEXT_CMD(success),
   *               WAIT_FOR_FINISH_WITH_THREAD_ERROR(protocol error)
   */
  ReceiverState processSizeCmd(ThreadData &data);
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
  ReceiverState sendFileChunks(ThreadData &data);
  /**
   * Sends global checkpoints to sender
   * Previous states : PROCESS_DONE_CMD,
   *                   WAIT_FOR_FINISH_OR_ERROR
   * Next states : READ_NEXT_CMD(success),
   *               ACCEPT_WITH_TIMEOUT(socket write failure)
   */
  ReceiverState sendGlobalCheckpoint(ThreadData &data);
  /**
   * Sends DONE to sender, also tries to read back ack. If anything fails during
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
  ReceiverState waitForFinishOrNewCheckpoint(ThreadData &data);

  /**
   * Waits for transfer to finish. Only called when there is an error for the
   * thread. It adds a checkpoint to the global list of checkpoints if a
   * connection was received. It increments waitingWithErrorThreadCount_ and
   * waits till the session ends.
   * Previous states : Almost all states
   * Next states : END
   */
  ReceiverState waitForFinishWithThreadError(ThreadData &data);

  /// Mapping from receiver states to state functions
  static const StateFunction stateMap_[];

  /// Responsible for basic setup and starting threads
  void start();

  /// This method is the entry point for each thread.
  void receiveOne(int threadIndex, ServerSocket &s, int64_t bufferSize,
                  TransferStats &threadStats);

  /**
   * Periodically calculates current transfer report and send it to progress
   * reporter. This only works in the single transfer mode.
   */
  void progressTracker();

  /**
   * Adds a checkpoint to the global checkpoint list
   * @param checkpoint    checkpoint to be added
   */
  void addCheckpoint(Checkpoint checkpoint);

  /**
   * @param startIndex    number of checkpoints already transferred by the
   *                      calling thread
   * @return              list of new checkpoints
   */
  std::vector<Checkpoint> getNewCheckpoints(int startIndex);

  /// Returns true if all threads finished for this session
  bool areAllThreadsFinished(bool checkpointAdded);

  /// Ends current global session
  void endCurGlobalSession();

  /**
   * Returns if a new session has started and the thread is not aware of it
   * A thread must hold lock on mutex_ before calling this
   */
  bool hasNewSessionStarted(ThreadData &data);

  /**
   * Start new transfer by incrementing transferStartedCount_
   * A thread must hold lock on mutex_ before calling this
   */
  void startNewGlobalSession();

  /**
   * Returns whether the current session has finished or not.
   * A thread must hold lock on mutex_ before calling this
   */
  bool hasCurSessionFinished(ThreadData &data);

  /// Starts a new session for the thread
  void startNewThreadSession(ThreadData &data);

  /// Ends current thread session
  void endCurThreadSession(ThreadData &data);

  /// Increments failed thread count, does not wait for transfer to finish
  void incrFailedThreadCountAndCheckForSessionEnd(ThreadData &data);

  /**
   * Get transfer report, meant to be called after threads have been finished
   * This method is not thread safe
   */
  std::unique_ptr<TransferReport> getTransferReport();

  /// The thread that is responsible for calling running the progress tracker
  std::thread progressTrackerThread_;
  /**
   * Flags that represents if a transfer has finished. Threads on completion
   * set this flag. This is always accurate even if you don't call finish()
   * No transfer can be started as long as this flag is false.
   */
  bool transferFinished_{true};

  /// Flag based on which threads finish processing on receiving a done
  bool isJoinable_{false};

  /// Destination directory where the received files will be written
  std::string destDir_;

  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_;

  /**
   * Unique-id used to verify transfer log. This value must be same for
   * transfers across resumption
   */
  std::string recoveryId_;

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

  /**
   * Bunch of stats objects given to each thread by the root thread
   * so that finish() can summarize the result at the end of joining.
   */
  std::vector<TransferStats> threadStats_;

  /// Per thread perf report
  std::vector<PerfStatReport> perfReports_;

  /// Transfer log manager
  TransferLogManager transferLogManager_;

  /// Parsed log entries
  std::vector<FileChunksInfo> parsedFileChunksInfo_;

  /// Enum representing status of file chunks transfer
  enum SendChunkStatus { NOT_STARTED, IN_PROGRESS, SENT };

  /// State of the receiver when sending file chunks in sendFileChunksCmd
  SendChunkStatus sendChunksStatus_{NOT_STARTED};

  /**
   * All threads coordinate with each other to send previously received file
   * chunks using this condition variable.
   */
  mutable std::condition_variable conditionFileChunksSent_;

  /// Number of blocks sent by the sender
  int64_t numBlocksSend_{-1};

  /// Global list of checkpoints
  std::vector<Checkpoint> checkpoints_;

  /// Number of threads which failed in the transfer
  int failedThreadCount_{0};

  /// Number of threads which are waiting for finish or new checkpoint
  int waitingThreadCount_{0};

  /// Number of threads which are waiting with an error
  int waitingWithErrorThreadCount_{0};

  /// Counter that is incremented each time a new session starts
  int64_t transferStartedCount_{0};

  /// Counter that is incremented each time a new session ends
  int64_t transferFinishedCount_{0};

  /// Total number of data bytes sender wants to transfer
  int64_t totalSenderBytes_{-1};

  /// Start time of the session
  std::chrono::time_point<Clock> startTime_;

  /// Mutex to guard all the shared variables
  mutable std::mutex mutex_;

  /// Condition variable to coordinate transfer finish
  mutable std::condition_variable conditionAllFinished_;

  /**
   * Returns true if threads have been joined (done in finish())
   * This is how destructor determines whether it should join threads
   */
  bool areThreadsJoined_{false};

  /// Number of active threads, decremented every time a thread is finished
  int32_t numActiveThreads_{0};

  /**
   * Mutex for the management of this instance, specifically to keep the
   * instance sane for multi threaded public API calls
   */
  std::mutex instanceManagementMutex_;
};
}
}  // namespace facebook::wdt
