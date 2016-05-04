/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtThread.h>

using namespace std;

namespace facebook {
namespace wdt {

TransferStats WdtThread::moveStats() {
  return std::move(threadStats_);
}

const PerfStatReport &WdtThread::getPerfReport() const {
  return threadCtx_->getPerfReport();
}

const TransferStats &WdtThread::getTransferStats() const {
  return threadStats_;
}

void WdtThread::startThread() {
  if (threadPtr_) {
    WDT_CHECK(false) << "There is a already a thread running " << threadIndex_
                     << " " << getPort();
  }
  auto state = controller_->getState(threadIndex_);
  // Check the state should be running here
  WDT_CHECK_EQ(state, RUNNING);
  threadPtr_.reset(new std::thread(&WdtThread::start, this));
}

ErrorCode WdtThread::finish() {
  if (!threadPtr_) {
    WLOG(ERROR) << "Finish called on an instance while no thread has been "
                << " created to do any work";
    return ERROR;
  }
  threadPtr_->join();
  threadPtr_.reset();
  return OK;
}

WdtThread::~WdtThread() {
  if (threadPtr_) {
    WLOG(INFO) << threadIndex_
               << " has an alive thread while the instance is being "
               << "destructed";
    finish();
  }
}
}
}
