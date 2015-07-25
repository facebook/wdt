/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtResourceController.h"

using namespace std;
const int64_t kDelTimeToSleepMillis = 100;

namespace facebook {
namespace wdt {

void WdtControllerBase::updateMaxReceiversLimit(int64_t maxNumReceivers) {
  GuardLock lock(controllerMutex_);
  maxNumReceivers_ = maxNumReceivers;
  LOG(INFO) << "Updated max number of receivers for " << controllerName_
            << " to " << maxNumReceivers_;
}

void WdtControllerBase::updateMaxSendersLimit(int64_t maxNumSenders) {
  GuardLock lock(controllerMutex_);
  maxNumSenders_ = maxNumSenders;
  LOG(INFO) << "Updated max number of senders for " << controllerName_ << " to "
            << maxNumSenders_;
}

void WdtControllerBase::setThrottler(shared_ptr<Throttler> throttler) {
  throttler_ = throttler;
  LOG(INFO) << "Set the throttler for " << controllerName_ << " " << *throttler;
}

shared_ptr<Throttler> WdtControllerBase::getThrottler() const {
  return throttler_;
}

WdtControllerBase::WdtControllerBase(const string& controllerName) {
  controllerName_ = controllerName;
}

WdtNamespaceController::WdtNamespaceController(const string& wdtNamespace)
    : WdtControllerBase(wdtNamespace) {
}

ErrorCode WdtNamespaceController::createReceiver(
    const WdtTransferRequest& request, ReceiverPtr& receiver) {
  const std::string& transferId = request.transferId;
  receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    // Check for already existing
    auto it = receiversMap_.find(transferId);
    if (it != receiversMap_.end()) {
      LOG(WARNING) << "Receiver already added for transfer " << transferId;
      receiver = it->second;
      return OK;
    }
    // Check for quotas
    if (numReceivers_ >= maxNumReceivers_ && maxNumReceivers_ > 0) {
      LOG(ERROR) << "Exceeded number of receivers for " << controllerName_
                 << " Number of max receivers " << maxNumReceivers_;
      return QUOTA_EXCEEDED;
    }
  }
  receiver = make_shared<Receiver>(request);
  receiver->setThrottler(throttler_);
  {
    GuardLock lock(controllerMutex_);
    receiversMap_[request.transferId] = receiver;
    ++numReceivers_;
  }
  return OK;
}

ErrorCode WdtNamespaceController::createSender(
    const WdtTransferRequest& request, SenderPtr& sender) {
  const std::string& transferId = request.transferId;
  sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    // Check for already existing
    auto it = sendersMap_.find(transferId);
    if (it != sendersMap_.end()) {
      LOG(WARNING) << "Sender already added for transfer " << transferId;
      sender = it->second;
      return OK;
    }
    /// Check for quotas
    if (numSenders_ >= maxNumSenders_ && maxNumSenders_ > 0) {
      LOG(ERROR) << "Exceeded number of senders for " << controllerName_
                 << " Number of max receivers " << maxNumSenders_;
      return QUOTA_EXCEEDED;
    }
  }
  sender = make_shared<Sender>(request);
  sender->setThrottler(throttler_);
  {
    GuardLock lock(controllerMutex_);
    sendersMap_[request.transferId] = sender;
    ++numSenders_;
  }
  return OK;
}

ErrorCode WdtNamespaceController::releaseReceiver(
    const std::string& transferId) {
  ReceiverPtr receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    auto it = receiversMap_.find(transferId);
    if (it == receiversMap_.end()) {
      LOG(ERROR) << "Couldn't find receiver to release with id " << transferId
                 << " for " << controllerName_;
      return NOT_FOUND;
    }
    receiver = std::move(it->second);
    receiversMap_.erase(it);
    --numReceivers_;
  }
  // receiver will be deleted and logs printed by the destructor
  LOG(INFO) << "Released the receiver with id " << receiver->getTransferId();
  return OK;
}

ErrorCode WdtNamespaceController::releaseSender(const std::string& transferId) {
  SenderPtr sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    auto it = sendersMap_.find(transferId);
    if (it == sendersMap_.end()) {
      LOG(ERROR) << "Couldn't find sender to release with id " << transferId
                 << " for " << controllerName_;
      return NOT_FOUND;
    }
    sender = std::move(it->second);
    sendersMap_.erase(it);
    --numSenders_;
  }
  LOG(INFO) << "Released the sender with id " << sender->getTransferId();
  return OK;
}

int64_t WdtNamespaceController::releaseAllSenders() {
  vector<SenderPtr> senders;
  {
    GuardLock lock(controllerMutex_);
    for (auto& senderPair : sendersMap_) {
      senders.push_back(std::move(senderPair.second));
    }
    sendersMap_.clear();
    numSenders_ = 0;
  }
  int numSenders = senders.size();
  LOG(INFO) << "Number of senders released " << numSenders;
  return numSenders;
}

int64_t WdtNamespaceController::releaseAllReceivers() {
  vector<ReceiverPtr> receivers;
  {
    GuardLock lock(controllerMutex_);
    for (auto& receiverPair : receiversMap_) {
      receivers.push_back(std::move(receiverPair.second));
    }
    receiversMap_.clear();
    numReceivers_ = 0;
  }
  int numReceivers = receivers.size();
  LOG(INFO) << "Number of receivers released " << numReceivers;
  return numReceivers;
}

SenderPtr WdtNamespaceController::getSender(const string& transferId) const {
  GuardLock lock(controllerMutex_);
  auto it = sendersMap_.find(transferId);
  if (it == sendersMap_.end()) {
    LOG(ERROR) << "Couldn't find sender trasnfer-id " << transferId << " for "
               << controllerName_;
    return nullptr;
  }
  return it->second;
}

ReceiverPtr WdtNamespaceController::getReceiver(
    const string& transferId) const {
  GuardLock lock(controllerMutex_);
  auto it = receiversMap_.find(transferId);
  if (it == receiversMap_.end()) {
    LOG(ERROR) << "Couldn't find receiver transfer-id " << transferId << " for "
               << controllerName_;
    return nullptr;
  }
  return it->second;
}

vector<SenderPtr> WdtNamespaceController::getSenders() const {
  vector<SenderPtr> senders;
  GuardLock lock(controllerMutex_);
  for (const auto& senderPair : sendersMap_) {
    senders.push_back(senderPair.second);
  }
  return senders;
}

vector<ReceiverPtr> WdtNamespaceController::getReceivers() const {
  vector<ReceiverPtr> receivers;
  GuardLock lock(controllerMutex_);
  for (const auto& receiverPair : receiversMap_) {
    receivers.push_back(receiverPair.second);
  }
  return receivers;
}

WdtNamespaceController::~WdtNamespaceController() {
  releaseAllSenders();
  releaseAllReceivers();
}

WdtResourceController::WdtResourceController() : WdtControllerBase("Global") {
}

void WdtResourceController::shutdown() {
  LOG(WARNING) << "Shutting down the controller";
  GuardLock lock(controllerMutex_);
  for (auto& namespaceController : namespaceMap_) {
    NamespaceControllerPtr controller = namespaceController.second;
    numSenders_ -= controller->releaseAllSenders();
    numReceivers_ -= controller->releaseAllReceivers();
    LOG(WARNING) << "Cleared out controller for " << namespaceController.first;
  }
  namespaceMap_.clear();
  LOG(WARNING) << "Shutdown the wdt resource controller";
}

WdtResourceController::~WdtResourceController() {
  shutdown();
}

ErrorCode WdtResourceController::createSender(
    const std::string& wdtNamespace,
    const WdtTransferRequest& wdtOperationRequest, SenderPtr& sender) {
  NamespaceControllerPtr controller = nullptr;
  sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return NOT_FOUND;
    }
    if (numSenders_ >= maxNumSenders_ && maxNumSenders_ > 0) {
      LOG(ERROR) << "Exceeded quota on max senders. "
                 << "Max num senders " << maxNumSenders_;
      return QUOTA_EXCEEDED;
    }
    ++numSenders_;
  }
  ErrorCode code = controller->createSender(wdtOperationRequest, sender);
  if (!sender) {
    GuardLock lock(controllerMutex_);
    --numSenders_;
    LOG(ERROR) << "Failed in creating sender for " << wdtNamespace;
  } else {
    LOG(INFO) << "Successfully added a sender for " << wdtNamespace;
  }
  return code;
}

ErrorCode WdtResourceController::createReceiver(
    const std::string& wdtNamespace,
    const WdtTransferRequest& wdtOperationRequest, ReceiverPtr& receiver) {
  NamespaceControllerPtr controller = nullptr;
  receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return NOT_FOUND;
    }
    if (numReceivers_ >= maxNumReceivers_ && maxNumReceivers_ > 0) {
      LOG(ERROR) << "Exceeded quota on max receivers. "
                 << "Max num senders " << maxNumReceivers_;
      return QUOTA_EXCEEDED;
    }
    ++numReceivers_;
  }
  ErrorCode code = controller->createReceiver(wdtOperationRequest, receiver);
  if (!receiver) {
    GuardLock lock(controllerMutex_);
    --numReceivers_;
    LOG(ERROR) << "Failed in creating receiver for " << wdtNamespace;
  } else {
    LOG(INFO) << "Successfully added a receiver for " << wdtNamespace;
  }
  return code;
}

ErrorCode WdtResourceController::releaseSender(const std::string& wdtNamespace,
                                               const std::string& transferId) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace, true);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  if (controller->releaseSender(transferId) == OK) {
    GuardLock lock(controllerMutex_);
    --numSenders_;
    return OK;
  }
  LOG(ERROR) << "Couldn't release sender " << transferId << " for "
             << wdtNamespace;
  return ERROR;
}

ErrorCode WdtResourceController::releaseAllSenders(
    const std::string& wdtNamespace) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace, true);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  int64_t numSenders = controller->releaseAllSenders();
  if (numSenders > 0) {
    GuardLock lock(controllerMutex_);
    numSenders_ -= numSenders;
  }
  return OK;
}

ErrorCode WdtResourceController::releaseReceiver(
    const std::string& wdtNamespace, const std::string& transferId) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace, true);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  if (controller->releaseReceiver(transferId) == OK) {
    GuardLock lock(controllerMutex_);
    --numReceivers_;
    return OK;
  }
  LOG(ERROR) << "Couldn't release receiver " << transferId << " for "
             << wdtNamespace;

  return ERROR;
}

ErrorCode WdtResourceController::releaseAllReceivers(
    const std::string& wdtNamespace) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace, true);
    if (!controller) {
      LOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  int64_t numReceivers = controller->releaseAllReceivers();
  if (numReceivers > 0) {
    GuardLock lock(controllerMutex_);
    numReceivers_ -= numReceivers;
  }
  return OK;
}

SenderPtr WdtResourceController::getSender(const string& wdtNamespace,
                                           const string& transferId) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace, true);
  if (!controller) {
    LOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return nullptr;
  }
  return controller->getSender(transferId);
}

vector<SenderPtr> WdtResourceController::getAllSenders(
    const string& wdtNamespace) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace, true);
  if (!controller) {
    LOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return vector<SenderPtr>();
  }
  return controller->getSenders();
}

ReceiverPtr WdtResourceController::getReceiver(const string& wdtNamespace,
                                               const string& transferId) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace, true);
  if (!controller) {
    LOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return nullptr;
  }
  return controller->getReceiver(transferId);
}

vector<ReceiverPtr> WdtResourceController::getAllReceivers(
    const string& wdtNamespace) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace, true);
  if (!controller) {
    LOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return vector<ReceiverPtr>();
  }
  return controller->getReceivers();
}

ErrorCode WdtResourceController::registerWdtNamespace(
    const std::string& wdtNamespace) {
  GuardLock lock(controllerMutex_);
  if (getNamespaceController(wdtNamespace)) {
    LOG(INFO) << "Found existing controller for " << wdtNamespace;
    return OK;
  }
  namespaceMap_[wdtNamespace] =
      make_shared<WdtNamespaceController>(wdtNamespace);
  return OK;
}

ErrorCode WdtResourceController::deRegisterWdtNamespace(
    const std::string& wdtNamespace) {
  NamespaceControllerPtr controller;
  {
    GuardLock lock(controllerMutex_);
    auto it = namespaceMap_.find(wdtNamespace);
    if (it != namespaceMap_.end()) {
      controller = std::move(it->second);
    } else {
      LOG(ERROR) << "Couldn't find the namespace " << wdtNamespace;
      return ERROR;
    }
    namespaceMap_.erase(it);
  }
  int numSenders = controller->releaseAllSenders();
  int numReceivers = controller->releaseAllReceivers();

  {
    GuardLock lock(controllerMutex_);
    numSenders_ -= numSenders;
    numReceivers_ -= numReceivers;
  }

  while (controller.use_count() > 1) {
    /* sleep override */
    usleep(kDelTimeToSleepMillis * 1000);
    LOG(INFO) << "Trying to delete the namespace " << wdtNamespace;
  }
  LOG(INFO) << "Deleted the namespace " << wdtNamespace;
  return OK;
}

void WdtResourceController::updateMaxReceiversLimit(
    const std::string& wdtNamespace, int64_t maxNumReceivers) {
  auto controller = getNamespaceController(wdtNamespace, true);
  if (controller) {
    controller->updateMaxReceiversLimit(maxNumReceivers);
  }
}

void WdtResourceController::updateMaxSendersLimit(
    const std::string& wdtNamespace, int64_t maxNumSenders) {
  auto controller = getNamespaceController(wdtNamespace, true);
  if (controller) {
    controller->updateMaxSendersLimit(maxNumSenders);
  }
}

shared_ptr<WdtNamespaceController>
WdtResourceController::getNamespaceController(const string& wdtNamespace,
                                              bool isLock) const {
  GuardLock lock(controllerMutex_, std::defer_lock);
  if (isLock) {
    lock.lock();
  }
  auto it = namespaceMap_.find(wdtNamespace);
  if (it != namespaceMap_.end()) {
    return it->second;
  }
  return nullptr;
}
}
}
