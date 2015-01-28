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

#include "FileCreator.h"
#include "ErrorCodes.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "ServerSocket.h"
#include <memory>
#include <string>
#include <condition_variable>
#include <mutex>

namespace facebook {
namespace wdt {
class Receiver {
 public:
  Receiver(int port, int numSockets);

  Receiver(int port, int numSockets, std::string destDir);

  /**
   * Joins on the threads spawned by start. This method
   * is called by default when the wdt receiver is expected
   * to run as forever running process. However this has to
   * be explicitly called when the caller expects to conclude
   * a transfer.
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
 protected:

  /**
   * Responsible for basic setup and starting threads
   */
  void start();

 private:
  void receiveOne(ServerSocket &s,
                  const std::string &destDir,
                  size_t bufferSize, TransferStats &threadStats,
                  std::vector<TransferStats> &receivedFilesStats);

  /**
   * The background process that will keep on running and monitor the
   * the progress made by all the threads. If there is no progress for
   * a significant period of time (specified by parameters), then the
   * the connection is broken off and threads finish
   */
  void progressTracker();
  /// The thread that is responsible for calling running the progress tracker
  std::thread progressTrackerThread_;
  /**
   * Flag set by the finish() method when the receiver threads are joined.
   * No transfer can be started as long as this flag is false.
   */
  bool transferFinished_{true};
  /// Flag based on which threads finish processing on receiving a done
  bool isJoinable_;
  /// The starting port number for the receiver threads
  int port_;
  /// The number of threads receiving files
  int numSockets_;
  /// Destination directory where the received files will be written
  std::string destDir_;
  /// Responsible for writing files on the disk
  std::unique_ptr<FileCreator> fileCreator_;
  /**
   * Progress tracker thread is a thread which has to be joined when the
   * transfer is finished. The root thread in finish() and the progress
   * tracker coordinate with each other through the boolean and this
   * condition variable.
   */
  std::condition_variable conditionRecvFinished_;
  /// The global thread for one instance of transfer. Only applicable
  /// when the process is joinable
  std::mutex transferInstanceMutex_;
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
  std::vector<std::vector<TransferStats>> receivedFilesStats_;
};
}
}  // namespace facebook::wdt
