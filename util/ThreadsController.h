/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <wdt/ErrorCodes.h>
#include <wdt/WdtThread.h>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace wdt {

class WdtThread;
/**
 * Thread states that represent what kind of functionality
 * are they executing on a higher level.
 * INIT - State before running at the time of construction
 * RUNNING - Thread is running without any errors
 * WAITING - Thread is not doing anything meaninful but
 *           rather waiting on other threads for something
 * FINISHED - Threads have finished with/without error
 */
enum ThreadStatus { INIT, RUNNING, WAITING, FINISHED };

/**
 * Primitive that takes a function and executes
 * it only once either on the first thread
 * or the last thread entrance
 */
class ExecuteOnceFunc {
 public:
  /// Constructor for the once only executor
  ExecuteOnceFunc(int numThreads, bool execFirst) {
    execFirst_ = execFirst;
    numThreads_ = numThreads;
  }

  /// Deleted copy constructor
  ExecuteOnceFunc(const ExecuteOnceFunc &that) = delete;

  /// Deleted assignment operator
  ExecuteOnceFunc &operator=(const ExecuteOnceFunc &that) = delete;

  /// Implements the main functionality of the executor
  template <typename Func>
  void execute(Func &&execFunc) {
    std::unique_lock<std::mutex> lock(mutex_);
    ++numHits_;
    WDT_CHECK(numHits_ <= numThreads_);
    int64_t numExpected = (execFirst_) ? 1 : numThreads_;
    if (numHits_ == numExpected) {
      execFunc();
    }
  }

  /// Reset the number of hits
  void reset() {
    numHits_ = 0;
  }

 private:
  /// Mutex for thread synchronization
  std::mutex mutex_;
  /// Number of times execute has been called
  int numHits_{0};
  /// Function can be executed on the first
  /// thread or the last thread
  bool execFirst_{true};
  /// Number of total threads
  int numThreads_;
};

/**
 * A scoped locking primitive. When you get this object
 * it means that you already have the lock. You can also
 * wait, notify etc using this primitive
 */
class ConditionGuardImpl {
 public:
  /// Release the lock and wait for the timeout
  /// After the wait is over, lock is reacquired
  void wait(int timeoutMillis, const ThreadCtx &threadCtx);
  /// Notify all the threads waiting on the lock
  void notifyAll();
  /// Notify one thread waiting on the lock
  void notifyOne();
  /// Delete the copy constructor
  ConditionGuardImpl(const ConditionGuardImpl &that) = delete;
  /// Delete the copy assignment operator
  ConditionGuardImpl &operator=(const ConditionGuardImpl &that) = delete;
  /// Move constructor for the guard
  ConditionGuardImpl(ConditionGuardImpl &&that) noexcept;
  /// Move assignment operator deleted
  ConditionGuardImpl &operator=(ConditionGuardImpl &&that) = delete;
  /// Destructor that releases the lock, you would explicitly
  /// need to notify any other threads waiting in the wait()
  ~ConditionGuardImpl();

 protected:
  friend class ConditionGuard;
  friend class Funnel;
  /// Constructor that takes the shared mutex and condition
  /// variable
  ConditionGuardImpl(std::mutex &mutex, std::condition_variable &cv);
  /// Instance of lock is made on construction with the specified mutex
  std::unique_lock<std::mutex> *lock_{nullptr};
  /// Shared condition variable
  std::condition_variable &cv_;
};

/**
 * Class for simplifying the primitive to take a lock
 * in conjunction with the ability to do things
 * on a condition variable based on the lock.
 * Use the condition guard like this
 *  ConditionGuard condition;
 *  auto guard = condition.acquire();
 *  guard.wait();
 */
class ConditionGuard {
 public:
  /// Caller has to call acquire before doing anything
  ConditionGuardImpl acquire();

  /// Default constructor
  ConditionGuard() {
  }

  /// Deleted copy constructor
  ConditionGuard(const ConditionGuard &that) = delete;

  /// Deleted assignment operator
  ConditionGuard &operator=(const ConditionGuard &that) = delete;

 private:
  /// Mutex for the condition variable
  std::mutex mutex_;
  /// std condition variable to support the functionality
  std::condition_variable cv_;
};

/**
 * A barrier primitive. When called for executing
 * will block the threads till all the threads registered
 * call execute()
 */
class Barrier {
 public:
  /// Deleted copy constructor
  Barrier(const Barrier &that) = delete;

  /// Deleted assignment operator
  Barrier &operator=(const Barrier &that) = delete;

  /// Constructor which takes total number of threads
  /// to be hit in order for the barrier to clear
  explicit Barrier(int numThreads) {
    numThreads_ = numThreads;
    WVLOG(1) << "making barrier with " << numThreads;
  }

  /// Executes the main functionality of the barrier
  void execute();

  /**
   * Thread controller should call this method when one thread
   * has been finished, since that thread will no longer be
   * participating in the barrier
   */
  void deRegister();

 private:
  /// Checks for finish, need to hold a lock to call this method
  bool checkForFinish();
  /// Condition variable that threads wait on
  std::condition_variable cv_;

  /// Number of threads entered the execute
  int64_t numHits_{0};

  /// Total number of threads that are supposed
  /// to hit the barrier
  int numThreads_{0};

  /// Thread synchronization mutex
  std::mutex mutex_;

  /// Represents the completion of barrier
  bool isComplete_{false};
};

/**
 * Different stages of the simple funnel
 * FUNNEL_START     the state of funnel at the beginning
 * FUNNEL_PROGRESS  is set by the first thread to enter the funnel
 *                  and it means that funnel functionality is in progress
 * FUNNEL_END means that funnel functionality has been executed
 */
enum FunnelStatus { FUNNEL_START, FUNNEL_PROGRESS, FUNNEL_END };

/**
 * Primitive that makes the threads execute in a funnel
 * manner. Only one thread gets to execute the main functionality
 * while other entering threads wait (while executing a function)
 */
class Funnel {
 public:
  /// Deleted copy constructor
  Funnel(const Funnel &that) = delete;

  /// Default constructor for funnel
  Funnel() {
    status_ = FUNNEL_START;
  }

  /// Deleted assignment operator
  Funnel &operator=(const Funnel &that) = delete;

  /**
   * Get the current status of funnel.
   * If the status is FUNNEL_START it gets set
   * to FUNNEL_PROGRESS else it is just a get
   */
  FunnelStatus getStatus();

  /// Threads in progress can wait indefinitely
  void wait();

  /// Threads that get status as progress execute this function
  void wait(int32_t waitingTime, const ThreadCtx &threadCtx);

  /**
   * The first thread that was able to start the funnel
   * calls this method on successful execution
   */
  void notifySuccess();

  /// The first thread that was able to start the funnel
  /// calls this method on failure in execution
  void notifyFail();

 private:
  /// Status of the funnel
  FunnelStatus status_;
  /// Mutex for the simple funnel executor
  std::mutex mutex_;
  /// Condition variable on which progressing threads wait
  std::condition_variable cv_;
};

/**
 * Controller class responsible for the receiver
 * and sender threads. Manages the states of threads and
 * session information
 */
class ThreadsController {
 public:
  /// Constructor that takes in the total number of threads
  /// to be run
  explicit ThreadsController(int totalThreads);

  /// Make threads of a type Sender/Receiver
  template <typename WdtBaseType, typename WdtThreadType>
  std::vector<std::unique_ptr<WdtThread>> makeThreads(
      WdtBaseType *wdtParent, int numThreads,
      const std::vector<int32_t> &ports) {
    std::vector<std::unique_ptr<WdtThread>> threads;
    for (int threadIndex = 0; threadIndex < numThreads; ++threadIndex) {
      threads.emplace_back(std::make_unique<WdtThreadType>(
          wdtParent, threadIndex, ports[threadIndex], this));
    }
    return threads;
  }
  ///  Mark the state of a thread
  void markState(int threadIndex, ThreadStatus state);

  /// Get the status of the thread by index
  ThreadStatus getState(int threadIndex);

  /// Execute a function func once, by the first thread
  template <typename FunctionType>
  void executeAtStart(FunctionType &&fn) const {
    execAtStart_->execute(fn);
  }

  /// Execute a function once by the last thread
  template <typename FunctionType>
  void executeAtEnd(FunctionType &&fn) const {
    execAtEnd_->execute(fn);
  }

  /// Returns a funnel executor shared between the threads
  /// If the executor does not exist then it creates one
  std::shared_ptr<Funnel> getFunnel(uint64_t funnelIndex);

  /// Returns a barrier shared between the threads
  /// If the executor does not exist then it creates one
  std::shared_ptr<Barrier> getBarrier(uint64_t barrierIndex);

  /// Get the condition variable wrapper
  std::shared_ptr<ConditionGuard> getCondition(uint64_t conditionIndex);

  /*
   * Returns back states of all the threads
   */
  std::unordered_map<int, ThreadStatus> getThreadStates() const;

  /// Register a thread, a thread registers with the state RUNNING
  void registerThread(int threadIndex);

  /// De-register a thread, marks it ended
  void deRegisterThread(int threadIndex);

  /// Returns true if any thread apart from the calling is in the state
  bool hasThreads(int threadIndex, ThreadStatus threadState);

  /// @return     true if any registered thread is in the state
  bool hasThreads(ThreadStatus threadState);

  /// Get the nunber of registered threads
  int getTotalThreads();

  /// Reset the thread controller so that same instance can be used again
  void reset();

  /// Set the total number of barriers
  void setNumBarriers(int numBarriers);

  /// Set the number of condition wrappers
  void setNumConditions(int numConditions);

  /// Set total number of funnel executors
  void setNumFunnels(int numFunnels);

  /// Destructor for the threads controller
  ~ThreadsController() {
  }

 private:
  /// Total number of threads managed by the thread controller
  int totalThreads_;

  typedef std::unique_lock<std::mutex> GuardLock;

  /// Mutex used in all of the thread controller methods
  mutable std::mutex controllerMutex_;

  /// States of the threads
  std::unordered_map<int, ThreadStatus> threadStateMap_;

  /// Executor to execute things at the start of transfer
  std::unique_ptr<ExecuteOnceFunc> execAtStart_;

  /// Executor to execute things at the end of transfer
  std::unique_ptr<ExecuteOnceFunc> execAtEnd_;

  /// Vector of funnel executors, read/modified by get/set funnel methods
  std::vector<std::shared_ptr<Funnel>> funnelExecutors_;

  /// Vector of condition wrappers, read/modified by get/set condition methods
  std::vector<std::shared_ptr<ConditionGuard>> conditionGuards_;

  /// Vector of barriers, can be read/modified by get/set barrier methods
  std::vector<std::shared_ptr<Barrier>> barriers_;
};
}
}
