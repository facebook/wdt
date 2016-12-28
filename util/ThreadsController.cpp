/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/ThreadsController.h>
#include <wdt/WdtOptions.h>

using namespace std;

namespace facebook {
namespace wdt {

void ConditionGuardImpl::wait(int timeoutMillis, const ThreadCtx &threadCtx) {
  const WdtOptions &options = threadCtx.getOptions();
  const bool checkAbort = (options.abort_check_interval_millis > 0);
  int remainingTime = timeoutMillis;

  while (remainingTime > 0) {
    int waitTime = remainingTime;
    if (checkAbort) {
      waitTime = std::min(waitTime, options.abort_check_interval_millis);
    }
    auto waitingTime = chrono::milliseconds(waitTime);
    auto status = cv_.wait_for(*lock_, waitingTime);
    if (status == std::cv_status::no_timeout) {
      return;
    }
    // check for abort
    if (threadCtx.getAbortChecker()->shouldAbort()) {
      WLOG(ERROR) << "Transfer aborted during condition guard wait "
                  << threadCtx.getThreadIndex();
      return;
    }
    remainingTime -= waitTime;
  }
}

void ConditionGuardImpl::notifyAll() {
  cv_.notify_all();
}

void ConditionGuardImpl::notifyOne() {
  cv_.notify_one();
}

ConditionGuardImpl::~ConditionGuardImpl() {
  if (lock_ != nullptr) {
    delete lock_;
  }
}

ConditionGuardImpl::ConditionGuardImpl(mutex &guardMutex,
                                       condition_variable &cv)
    : cv_(cv) {
  lock_ = new unique_lock<mutex>(guardMutex);
}

ConditionGuardImpl::ConditionGuardImpl(ConditionGuardImpl &&that) noexcept
    : cv_(that.cv_) {
  swap(lock_, that.lock_);
}

ConditionGuardImpl ConditionGuard::acquire() {
  return ConditionGuardImpl(mutex_, cv_);
}

FunnelStatus Funnel::getStatus() {
  unique_lock<mutex> lock(mutex_);
  if (status_ == FUNNEL_START) {
    status_ = FUNNEL_PROGRESS;
    return FUNNEL_START;
  }
  return status_;
}

void Funnel::wait() {
  unique_lock<mutex> lock(mutex_);
  if (status_ != FUNNEL_PROGRESS) {
    return;
  }
  cv_.wait(lock);
}

void Funnel::wait(int32_t waitingTime, const ThreadCtx &threadCtx) {
  ConditionGuardImpl guard(mutex_, cv_);
  if (status_ != FUNNEL_PROGRESS) {
    return;
  }
  guard.wait(waitingTime, threadCtx);
}

void Funnel::notifySuccess() {
  unique_lock<mutex> lock(mutex_);
  status_ = FUNNEL_END;
  cv_.notify_all();
}

void Funnel::notifyFail() {
  unique_lock<mutex> lock(mutex_);
  status_ = FUNNEL_START;
  cv_.notify_one();
}

bool Barrier::checkForFinish() {
  // lock should be held while calling this method
  WDT_CHECK_GE(numThreads_, numHits_);
  if (numHits_ == numThreads_) {
    isComplete_ = true;
    cv_.notify_all();
  }
  return isComplete_;
}

void Barrier::execute() {
  unique_lock<mutex> lock(mutex_);
  WDT_CHECK(!isComplete_) << "Hitting the barrier after completion";
  ++numHits_;
  if (checkForFinish()) {
    return;
  }
  while (!isComplete_) {
    cv_.wait(lock);
  }
}

void Barrier::deRegister() {
  unique_lock<mutex> lock(mutex_);
  if (isComplete_) {
    return;
  }
  --numThreads_;
  checkForFinish();
}

ThreadsController::ThreadsController(int totalThreads) {
  totalThreads_ = totalThreads;
  for (int threadNum = 0; threadNum < totalThreads; ++threadNum) {
    threadStateMap_[threadNum] = INIT;
  }
  execAtStart_.reset(new ExecuteOnceFunc(totalThreads_, true));
  execAtEnd_.reset(new ExecuteOnceFunc(totalThreads_, false));
}

void ThreadsController::registerThread(int threadIndex) {
  GuardLock lock(controllerMutex_);
  auto it = threadStateMap_.find(threadIndex);
  WDT_CHECK(it != threadStateMap_.end());
  threadStateMap_[threadIndex] = RUNNING;
}

void ThreadsController::deRegisterThread(int threadIndex) {
  GuardLock lock(controllerMutex_);
  auto it = threadStateMap_.find(threadIndex);
  WDT_CHECK(it != threadStateMap_.end());
  threadStateMap_[threadIndex] = FINISHED;
  // Notify all the barriers
  for (auto barrier : barriers_) {
    WDT_CHECK(barrier != nullptr);
    barrier->deRegister();
  }
}

ThreadStatus ThreadsController::getState(int threadIndex) {
  GuardLock lock(controllerMutex_);
  auto it = threadStateMap_.find(threadIndex);
  WDT_CHECK(it != threadStateMap_.end());
  return it->second;
}

void ThreadsController::markState(int threadIndex, ThreadStatus threadState) {
  GuardLock lock(controllerMutex_);
  threadStateMap_[threadIndex] = threadState;
}

unordered_map<int, ThreadStatus> ThreadsController::getThreadStates() const {
  GuardLock lock(controllerMutex_);
  return threadStateMap_;
}

int ThreadsController::getTotalThreads() {
  return totalThreads_;
}

bool ThreadsController::hasThreads(ThreadStatus threadState) {
  // thread indices are all positive
  return hasThreads(-1, threadState);
}

bool ThreadsController::hasThreads(int threadIndex, ThreadStatus threadState) {
  GuardLock lock(controllerMutex_);
  for (auto &threadPair : threadStateMap_) {
    if (threadPair.first == threadIndex) {
      continue;
    }
    if (threadPair.second == threadState) {
      return true;
    }
  }
  return false;
}

shared_ptr<ConditionGuard> ThreadsController::getCondition(
    const uint64_t conditionIndex) {
  bool isExists = (conditionGuards_.size() > conditionIndex) &&
                  (conditionGuards_[conditionIndex] != nullptr);
  WDT_CHECK(isExists)
      << "Requesting for a condition wrapper that doesn't exist."
      << " Request Index : " << conditionIndex
      << ", num condition wrappers : " << conditionGuards_.size();
  return conditionGuards_[conditionIndex];
}

shared_ptr<Barrier> ThreadsController::getBarrier(const uint64_t barrierIndex) {
  bool isExists =
      (barriers_.size() > barrierIndex) && (barriers_[barrierIndex] != nullptr);
  WDT_CHECK(isExists)
      << "Requesting for a barrier that doesn't exist. Request index : "
      << barrierIndex << ", num barriers " << barriers_.size();
  return barriers_[barrierIndex];
}

shared_ptr<Funnel> ThreadsController::getFunnel(const uint64_t funnelIndex) {
  bool isExists = (funnelExecutors_.size() > funnelIndex) &&
                  (funnelExecutors_[funnelIndex] != nullptr);
  WDT_CHECK(isExists)
      << "Requesting for a funnel that doesn't exist. Request index : "
      << funnelIndex << ", num funnels " << funnelExecutors_.size();
  return funnelExecutors_[funnelIndex];
}

void ThreadsController::reset() {
  // Only used in the case of long running mode
  setNumBarriers(barriers_.size());
  setNumConditions(conditionGuards_.size());
  setNumFunnels(funnelExecutors_.size());
  execAtStart_->reset();
  execAtEnd_->reset();
  GuardLock lock(controllerMutex_);
  // Restore threads back to initial state
  for (auto &threadPair : threadStateMap_) {
    threadPair.second = RUNNING;
  }
}

void ThreadsController::setNumBarriers(int numBarriers) {
  // Meant to be called outside of threads
  barriers_.clear();
  for (int i = 0; i < numBarriers; i++) {
    barriers_.push_back(make_shared<Barrier>(getTotalThreads()));
  }
}

void ThreadsController::setNumConditions(int numConditions) {
  conditionGuards_.clear();
  for (int i = 0; i < numConditions; i++) {
    conditionGuards_.push_back(make_shared<ConditionGuard>());
  }
}

void ThreadsController::setNumFunnels(int numFunnels) {
  funnelExecutors_.clear();
  for (int i = 0; i < numFunnels; i++) {
    funnelExecutors_.push_back(make_shared<Funnel>());
  }
}
}
}
