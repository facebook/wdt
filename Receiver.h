/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ReceiverThread.h>
#include <wdt/WdtBase.h>
#include <wdt/util/FileCreator.h>
#include <wdt/util/IServerSocket.h>
#include <wdt/util/TransferLogManager.h>
#include <chrono>
#include <memory>
#include <string>
#include <thread>

namespace facebook {
namespace wdt {
/**
 * Receiver is the receiving side of the transfer. Receiver listens on ports
 * accepts connections, receives the files and writes to the destination
 * directory. Receiver has two modes of operation : You can spawn a receiver
 * for one transfer or alternatively it can also be used in a long running
 * mode where it accepts subsequent transfers and runs in an infinite loop.
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
  const WdtTransferRequest &init() override;

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

  /// @param recoveryId   unique-id used to verify transfer log
  void setRecoveryId(const std::string &recoveryId);

  /// Returns true if at least one thread has accepted connection
  bool hasNewTransferStarted() const;

  // Different accept modes for the Receiver
  enum AcceptMode {
    ACCEPT_WITH_RETRIES,  // Receiver gives up after max_accept_retries
    ACCEPT_FOREVER,       // Receiver never gives up
    STOP_ACCEPTING,       // Receiver stops accepting
  };

  /// @param acceptMode   acceptMode to use
  void setAcceptMode(AcceptMode acceptMode);

  /// Interface to make socket
  class ISocketCreator {
   public:
    virtual std::unique_ptr<IServerSocket> makeServerSocket(
        ThreadCtx &threadCtx, int port, int backlog,
        const EncryptionParams &encryptionParams, int64_t ivChangeInterval,
        Func &&tagVerificationSuccessCallback, bool tls) = 0;

    virtual ~ISocketCreator() = default;
  };

  /**
   * Sets socket creator
   *
   * @param socketCreator   socket-creator to be used
   */
  void setSocketCreator(ISocketCreator *socketCreator);

  /**
   * Destructor for the receiver. The destructor automatically cancels
   * any incomplete transfers that are going on. 'Incomplete transfer' is a
   * transfer where there is no receiver thread that has received
   * confirmation from wdt sender that the transfer is 'DONE'. Destructor also
   * internally calls finish() for every transfer if finish() wasn't called
   */
  ~Receiver() override;

 protected:
  friend class ReceiverThread;

  /**
   * Traverses root directory and returns discovered file information
   *
   * @param fileChunksInfo     discovered file info
   */
  void traverseDestinationDir(std::vector<FileChunksInfo> &fileChunksInfo);

  /// Get the transferred file chunks info
  const std::vector<FileChunksInfo> &getFileChunksInfo() const;

  /// Get file creator, used by receiver threads
  std::unique_ptr<FileCreator> &getFileCreator();

  /// Socket creator used to optionally create different kind of server socket
  ISocketCreator *socketCreator_{nullptr};

  /// Get the ref to transfer log manager
  TransferLogManager &getTransferLogManager();

  /// Responsible for basic setup and starting threads
  ErrorCode start();

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

  /// Does the steps needed before a new transfer is started
  void startNewGlobalSession(const std::string &peerIp);

  /// Has steps to do when the current transfer is ended
  void endCurGlobalSession();

  /// adds log header and also a directory invalidation entry if needed
  void addTransferLogHeader(bool isBlockMode, bool isSenderResuming);

  /// fix, compact (if enabled) and close transfer log
  void fixAndCloseTransferLog(bool transferSuccess);

  /**
   * Get transfer report, meant to be called after threads have been finished
   * This method is not thread safe
   */
  std::unique_ptr<TransferReport> getTransferReport();

  void logPerfStats() const override;

  /// @return     transfer config encoded as int
  int64_t getTransferConfig() const;

  AcceptMode getAcceptMode();

  /// The thread that is responsible for calling running the progress tracker
  std::thread progressTrackerThread_;

  /// Flag based on which threads finish processing on receiving a done
  bool isJoinable_{false};

  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_{nullptr};

  /**
   * Unique-id used to verify transfer log. This value must be same for
   * transfers across resumption
   */
  std::string recoveryId_;

  /**
   * The instance of the receiver threads are stored in this vector.
   * This will not be destroyed until this object is destroyed, hence
   * it has to be made sure that these threads are joined at least before
   * the destruction of this object.
   */
  std::vector<std::unique_ptr<WdtThread>> receiverThreads_;

  /// Transfer log manager
  std::unique_ptr<TransferLogManager> transferLogManager_;

  /// Global list of checkpoints
  std::vector<Checkpoint> checkpoints_;

  /// Start time of the session
  std::chrono::time_point<Clock> startTime_;

  /// already transferred file chunks
  std::vector<FileChunksInfo> fileChunksInfo_;

  /// Marks when a new transfer has started
  std::atomic<bool> hasNewTransferStarted_{false};

  /// Backlog used by the sockets
  int backlog_;

  AcceptMode acceptMode_{ACCEPT_WITH_RETRIES};
};
}
}  // namespace facebook::wdt
