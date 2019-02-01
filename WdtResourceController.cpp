/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtResourceController.h>

using namespace std;
const int64_t kDelTimeToSleepMillis = 100;

namespace facebook {
namespace wdt {

const char *const WdtResourceController::kGlobalNamespace("Global");

void WdtControllerBase::updateMaxReceiversLimit(int64_t maxNumReceivers) {
  GuardLock lock(controllerMutex_);
  maxNumReceivers_ = maxNumReceivers;
  WLOG(INFO) << "Updated max number of receivers for " << controllerName_
             << " to " << maxNumReceivers_;
}

void WdtControllerBase::updateMaxSendersLimit(int64_t maxNumSenders) {
  GuardLock lock(controllerMutex_);
  maxNumSenders_ = maxNumSenders;
  WLOG(INFO) << "Updated max number of senders for " << controllerName_
             << " to " << maxNumSenders_;
}

WdtControllerBase::WdtControllerBase(const string &controllerName) {
  controllerName_ = controllerName;
}

WdtNamespaceController::WdtNamespaceController(
    const string &wdtNamespace, const WdtResourceController *const parent)
    : WdtControllerBase(wdtNamespace), parent_(parent) {
  auto &options = parent_->getOptions();
  updateMaxSendersLimit(options.namespace_sender_limit);
  updateMaxReceiversLimit(options.namespace_receiver_limit);
}

bool WdtNamespaceController::hasReceiverQuota() const {
  GuardLock lock(controllerMutex_);
  if (numReceivers_ >= maxNumReceivers_ && maxNumReceivers_ > 0) {
    WLOG(WARNING) << "Exceeded number of receivers for " << controllerName_
                  << " Max number of receivers " << maxNumReceivers_;
    return false;
  }
  return true;
}

ErrorCode WdtNamespaceController::createReceiver(
    const WdtTransferRequest &request, const string &identifier,
    ReceiverPtr &receiver) {
  receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    // Check for already existing
    auto it = receiversMap_.find(identifier);
    if (it != receiversMap_.end()) {
      WLOG(ERROR) << "Receiver already created for transfer " << identifier;
      // Return it so the old one can potentially be aborted
      receiver = it->second;
      return ALREADY_EXISTS;
    }
    // Check for quotas
    if (!hasReceiverQuota()) {
      return QUOTA_EXCEEDED;
    }
    receiver = make_shared<Receiver>(request);
    receiver->setThrottler(parent_->getWdtThrottler());
    receiver->setWdtOptions(parent_->getOptions());
    receiversMap_[identifier] = receiver;
    ++numReceivers_;
  }
  return OK;
}

bool WdtNamespaceController::hasSenderQuota() const {
  GuardLock lock(controllerMutex_);
  if (numSenders_ >= maxNumSenders_ && maxNumSenders_ > 0) {
    WLOG(WARNING) << "Exceeded number of senders for " << controllerName_
                  << " Max number of senders " << maxNumSenders_;
    return false;
  }
  return true;
}

ErrorCode WdtNamespaceController::createSender(
    const WdtTransferRequest &request, const string &identifier,
    SenderPtr &sender) {
  sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    // Check for already existing
    auto it = sendersMap_.find(identifier);
    if (it != sendersMap_.end()) {
      WLOG(ERROR) << "Sender already created for transfer " << identifier;
      // Return it so the old one can potentially be aborted
      sender = it->second;
      return ALREADY_EXISTS;
    }
    /// Check for quotas
    if (!hasSenderQuota()) {
      return QUOTA_EXCEEDED;
    }
    sender = make_shared<Sender>(request);
    sender->setThrottler(parent_->getWdtThrottler());
    sender->setWdtOptions(parent_->getOptions());
    sendersMap_[identifier] = sender;
    ++numSenders_;
  }
  return OK;
}

ErrorCode WdtNamespaceController::releaseReceiver(
    const std::string &identifier) {
  ReceiverPtr receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    auto it = receiversMap_.find(identifier);
    if (it == receiversMap_.end()) {
      WLOG(ERROR) << "Couldn't find receiver to release with id " << identifier
                  << " for " << controllerName_;
      return NOT_FOUND;
    }
    receiver = std::move(it->second);
    receiversMap_.erase(it);
    --numReceivers_;
  }
  // receiver will be deleted and logs printed by the destructor
  // if no other thread has the shared pointer, that is...
  WLOG(INFO) << "Released the receiver with id " << receiver->getTransferId();
  return OK;
}

ErrorCode WdtNamespaceController::releaseSender(const std::string &identifier) {
  SenderPtr sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    auto it = sendersMap_.find(identifier);
    if (it == sendersMap_.end()) {
      WLOG(ERROR) << "Couldn't find sender to release with id " << identifier
                  << " for " << controllerName_;
      return NOT_FOUND;
    }
    sender = std::move(it->second);
    sendersMap_.erase(it);
    --numSenders_;
  }
  WLOG(INFO) << "Released the sender with id " << sender->getTransferId();
  return OK;
}

int64_t WdtNamespaceController::releaseAllSenders() {
  vector<SenderPtr> senders;
  {
    GuardLock lock(controllerMutex_);
    for (auto &senderPair : sendersMap_) {
      senders.push_back(std::move(senderPair.second));
    }
    sendersMap_.clear();
    numSenders_ = 0;
  }
  int numSenders = senders.size();
  WVLOG(1) << "Number of senders released " << numSenders;
  return numSenders;
}

vector<string> WdtNamespaceController::releaseStaleSenders() {
  vector<SenderPtr> senders;
  vector<string> erasedIds;
  {
    GuardLock lock(controllerMutex_);
    for (auto it = sendersMap_.begin(); it != sendersMap_.end();) {
      auto sender = it->second;
      string identifier = it->first;
      if (sender->isStale()) {
        it = sendersMap_.erase(it);
        erasedIds.push_back(identifier);
        senders.push_back(std::move(sender));
        --numSenders_;
        continue;
      }
      it++;
    }
  }
  WLOG(INFO) << "Cleared " << senders.size() << " stale senders";
  return erasedIds;
}

int64_t WdtNamespaceController::releaseAllReceivers() {
  vector<ReceiverPtr> receivers;
  {
    GuardLock lock(controllerMutex_);
    for (auto &receiverPair : receiversMap_) {
      receivers.push_back(std::move(receiverPair.second));
    }
    receiversMap_.clear();
    numReceivers_ = 0;
  }
  int numReceivers = receivers.size();
  WVLOG(1) << "Number of receivers released " << numReceivers;
  return numReceivers;
}

vector<string> WdtNamespaceController::releaseStaleReceivers() {
  vector<ReceiverPtr> receivers;
  vector<string> erasedIds;
  {
    GuardLock lock(controllerMutex_);
    for (auto it = receiversMap_.begin(); it != receiversMap_.end();) {
      auto receiver = it->second;
      string identifier = it->first;
      if (receiver->isStale()) {
        it = receiversMap_.erase(it);
        erasedIds.push_back(identifier);
        receivers.push_back(std::move(receiver));
        --numReceivers_;
        continue;
      }
      it++;
    }
  }
  WLOG(INFO) << "Cleared " << receivers.size() << "stale receivers";
  return erasedIds;
}

SenderPtr WdtNamespaceController::getSender(const string &identifier) const {
  GuardLock lock(controllerMutex_);
  auto it = sendersMap_.find(identifier);
  if (it == sendersMap_.end()) {
    WLOG(ERROR) << "Couldn't find sender transfer-id " << identifier << " for "
                << controllerName_;
    return nullptr;
  }
  return it->second;
}

ReceiverPtr WdtNamespaceController::getReceiver(
    const string &identifier) const {
  GuardLock lock(controllerMutex_);
  auto it = receiversMap_.find(identifier);
  if (it == receiversMap_.end()) {
    WLOG(ERROR) << "Couldn't find receiver transfer-id " << identifier
                << " for " << controllerName_;
    return nullptr;
  }
  return it->second;
}

vector<SenderPtr> WdtNamespaceController::getSenders() const {
  vector<SenderPtr> senders;
  GuardLock lock(controllerMutex_);
  for (const auto &senderPair : sendersMap_) {
    senders.push_back(senderPair.second);
  }
  return senders;
}

vector<ReceiverPtr> WdtNamespaceController::getReceivers() const {
  vector<ReceiverPtr> receivers;
  GuardLock lock(controllerMutex_);
  for (const auto &receiverPair : receiversMap_) {
    receivers.push_back(receiverPair.second);
  }
  return receivers;
}

vector<string> WdtNamespaceController::getSendersIds() const {
  vector<string> senderIds;
  GuardLock lock(controllerMutex_);
  for (const auto &senderPair : sendersMap_) {
    senderIds.push_back(senderPair.first);
  }
  return senderIds;
}

WdtNamespaceController::~WdtNamespaceController() {
  // release is done by parent shutdown
}

WdtResourceController::WdtResourceController(
    const WdtOptions &options, std::shared_ptr<Throttler> throttler)
    : WdtControllerBase("_root controller_"), options_(options) {
  updateMaxSendersLimit(options.global_sender_limit);
  updateMaxReceiversLimit(options.global_receiver_limit);
  throttler_ = throttler;
}

WdtResourceController::WdtResourceController(const WdtOptions &options)
    : WdtResourceController(
          options,
          std::make_shared<Throttler>(options.getThrottlerOptions())) {
}

WdtResourceController::WdtResourceController()
    : WdtResourceController(WdtOptions::get()) {
}

WdtResourceController *WdtResourceController::get() {
  static WdtResourceController wdtController(WdtOptions::get());
  return &wdtController;
}

void WdtResourceController::shutdown() {
  WLOG(INFO) << "Shutting down the controller (" << numSenders_ << " senders "
             << numReceivers_ << " receivers)";
  GuardLock lock(controllerMutex_);
  for (auto &namespaceController : namespaceMap_) {
    NamespaceControllerPtr controller = namespaceController.second;
    numSenders_ -= controller->releaseAllSenders();
    numReceivers_ -= controller->releaseAllReceivers();
    WVLOG(1) << "Cleared out controller for " << namespaceController.first;
  }
  namespaceMap_.clear();
  WDT_CHECK_EQ(numReceivers_, 0);
  WDT_CHECK_EQ(numSenders_, 0);
  WVLOG(1) << "Shutdown the wdt resource controller";
}

WdtResourceController::~WdtResourceController() {
  shutdown();
}

ErrorCode WdtResourceController::getCounts(int32_t &numNamespaces,
                                           int32_t &numSenders,
                                           int32_t &numReceivers) {
  GuardLock lock(controllerMutex_);
  numSenders = numSenders_;
  numReceivers = numReceivers_;
  numNamespaces = namespaceMap_.size();
  return OK;
}

bool WdtResourceController::hasSenderQuota(
    const std::string &wdtNamespace) const {
  const auto &controller = getNamespaceController(wdtNamespace);
  return hasSenderQuotaInternal(controller);
}

bool WdtResourceController::hasSenderQuotaInternal(
    const std::shared_ptr<WdtNamespaceController> &controller) const {
  GuardLock lock(controllerMutex_);
  if ((numSenders_ >= maxNumSenders_) && (maxNumSenders_ > 0)) {
    WLOG(WARNING) << "Exceeded quota on max senders. "
                  << "Max num senders " << maxNumSenders_ << " and we have "
                  << numSenders_ << " existing senders";
    return false;
  }
  if (controller && !controller->hasSenderQuota()) {
    return false;
  }
  return true;
}

ErrorCode WdtResourceController::createSender(
    const std::string &wdtNamespace, const std::string &identifier,
    const WdtTransferRequest &wdtOperationRequest, SenderPtr &sender) {
  NamespaceControllerPtr controller = nullptr;
  sender = nullptr;
  {
    GuardLock lock(controllerMutex_);
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      if (strictRegistration_) {
        WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
        return NOT_FOUND;
      } else {
        WLOG(INFO) << "First time "
                   << (wdtNamespace.empty() ? "(default)" : wdtNamespace)
                   << " is seen, creating.";
        controller = createNamespaceController(wdtNamespace);
      }
    }
    if (!hasSenderQuotaInternal(controller)) {
      WLOG(ERROR) << "No quota for more sender.";
      return QUOTA_EXCEEDED;
    }
    ++numSenders_;
  }
  // TODO: not thread safe reading from options_
  throttler_->setThrottlerRates(options_.getThrottlerOptions());
  ErrorCode code =
      controller->createSender(wdtOperationRequest, identifier, sender);
  if (code != OK) {
    GuardLock lock(controllerMutex_);
    --numSenders_;
    WLOG(ERROR) << "Failed in creating sender for " << wdtNamespace << " "
                << errorCodeToStr(code);
  } else {
    WLOG(INFO) << "Successfully added a sender for " << wdtNamespace
               << " identifier " << identifier;
  }
  return code;
}

bool WdtResourceController::hasReceiverQuota(
    const std::string &wdtNamespace) const {
  const auto &controller = getNamespaceController(wdtNamespace);
  return hasReceiverQuotaInternal(controller);
}

bool WdtResourceController::hasReceiverQuotaInternal(
    const std::shared_ptr<WdtNamespaceController> &controller) const {
  GuardLock lock(controllerMutex_);
  if ((numReceivers_ >= maxNumReceivers_) && (maxNumReceivers_ > 0)) {
    WLOG(WARNING) << "Exceeded quota on max receivers. "
                  << "Max num receivers " << maxNumReceivers_ << " and we have "
                  << numReceivers_ << " existing receivers";
    return false;
  }
  if (controller && !controller->hasReceiverQuota()) {
    return false;
  }
  return true;
}

ErrorCode WdtResourceController::createReceiver(
    const std::string &wdtNamespace, const string &identifier,
    const WdtTransferRequest &wdtOperationRequest, ReceiverPtr &receiver) {
  NamespaceControllerPtr controller = nullptr;
  receiver = nullptr;
  {
    GuardLock lock(controllerMutex_);
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      if (strictRegistration_) {
        WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
        return NOT_FOUND;
      } else {
        WLOG(INFO) << "First time " << wdtNamespace << " is seen, creating.";
        controller = createNamespaceController(wdtNamespace);
      }
    }
    if (!hasReceiverQuotaInternal(controller)) {
      WLOG(ERROR) << "No quota for more receiver.";
      return QUOTA_EXCEEDED;
    }
    ++numReceivers_;
  }
  // TODO: not thread safe reading from options_
  throttler_->setThrottlerRates(options_.getThrottlerOptions());
  ErrorCode code =
      controller->createReceiver(wdtOperationRequest, identifier, receiver);
  if (code != OK) {
    GuardLock lock(controllerMutex_);
    --numReceivers_;
    WLOG(ERROR) << "Failed in creating receiver for " << wdtNamespace << " "
                << errorCodeToStr(code);
  } else {
    WLOG(INFO) << "Successfully added a receiver for " << wdtNamespace
               << " identifier " << identifier;
  }
  return code;
}

ErrorCode WdtResourceController::releaseSender(const std::string &wdtNamespace,
                                               const std::string &identifier) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  if (controller->releaseSender(identifier) == OK) {
    GuardLock lock(controllerMutex_);
    --numSenders_;
    return OK;
  }
  WLOG(ERROR) << "Couldn't release sender " << identifier << " for "
              << wdtNamespace;
  return ERROR;
}

ErrorCode WdtResourceController::releaseAllSenders(
    const std::string &wdtNamespace) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
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
    const std::string &wdtNamespace, const std::string &identifier) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
      return ERROR;
    }
  }
  if (controller->releaseReceiver(identifier) == OK) {
    GuardLock lock(controllerMutex_);
    --numReceivers_;
    return OK;
  }
  WLOG(ERROR) << "Couldn't release receiver " << identifier << " for "
              << wdtNamespace;

  return ERROR;
}

ErrorCode WdtResourceController::releaseAllReceivers(
    const std::string &wdtNamespace) {
  NamespaceControllerPtr controller = nullptr;
  {
    controller = getNamespaceController(wdtNamespace);
    if (!controller) {
      WLOG(WARNING) << "Couldn't find controller for " << wdtNamespace;
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

SenderPtr WdtResourceController::getSender(const string &wdtNamespace,
                                           const string &identifier) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return nullptr;
  }
  return controller->getSender(identifier);
}

vector<SenderPtr> WdtResourceController::getAllSenders(
    const string &wdtNamespace) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return vector<SenderPtr>();
  }
  return controller->getSenders();
}

ErrorCode WdtResourceController::releaseStaleSenders(
    const string &wdtNamespace, vector<string> &erasedIds) {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return NOT_FOUND;
  }
  erasedIds = controller->releaseStaleSenders();
  if (erasedIds.size() > 0) {
    GuardLock lock(controllerMutex_);
    numSenders_ -= erasedIds.size();
  }
  return OK;
}

ReceiverPtr WdtResourceController::getReceiver(const string &wdtNamespace,
                                               const string &identifier) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return nullptr;
  }
  return controller->getReceiver(identifier);
}

vector<ReceiverPtr> WdtResourceController::getAllReceivers(
    const string &wdtNamespace) const {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return vector<ReceiverPtr>();
  }
  return controller->getReceivers();
}

std::vector<std::string> WdtResourceController::getAllSendersIds(
    const string &wdtNamespace) const {
  vector<string> senderIds;
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return senderIds;
  }
  return controller->getSendersIds();
}

ErrorCode WdtResourceController::releaseStaleReceivers(
    const string &wdtNamespace, vector<string> &erasedIds) {
  NamespaceControllerPtr controller = nullptr;
  controller = getNamespaceController(wdtNamespace);
  if (!controller) {
    WLOG(ERROR) << "Couldn't find the controller for " << wdtNamespace;
    return NOT_FOUND;
  }
  erasedIds = controller->releaseStaleReceivers();
  if (erasedIds.size() > 0) {
    GuardLock lock(controllerMutex_);
    numReceivers_ -= erasedIds.size();
  }
  return OK;
}

WdtResourceController::NamespaceControllerPtr
WdtResourceController::createNamespaceController(
    const std::string &wdtNamespace) {
  auto namespaceController =
      make_shared<WdtNamespaceController>(wdtNamespace, this);
  namespaceMap_[wdtNamespace] = namespaceController;
  return namespaceController;
}

ErrorCode WdtResourceController::registerWdtNamespace(
    const std::string &wdtNamespace) {
  GuardLock lock(controllerMutex_);
  if (getNamespaceController(wdtNamespace)) {
    WLOG(INFO) << "Found existing controller for " << wdtNamespace;
    return OK;
  }
  createNamespaceController(wdtNamespace);
  return OK;
}

ErrorCode WdtResourceController::deRegisterWdtNamespace(
    const std::string &wdtNamespace) {
  NamespaceControllerPtr controller;
  {
    GuardLock lock(controllerMutex_);
    auto it = namespaceMap_.find(wdtNamespace);
    if (it != namespaceMap_.end()) {
      controller = std::move(it->second);
    } else {
      WLOG(ERROR) << "Couldn't find the namespace " << wdtNamespace;
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
    WLOG(INFO) << "Trying to delete the namespace " << wdtNamespace;
  }
  WLOG(INFO) << "Deleted the namespace " << wdtNamespace;
  return OK;
}

void WdtResourceController::updateMaxReceiversLimit(
    const std::string &wdtNamespace, int64_t maxNumReceivers) {
  auto controller = getNamespaceController(wdtNamespace);
  if (controller) {
    controller->updateMaxReceiversLimit(maxNumReceivers);
  }
}

void WdtResourceController::updateMaxSendersLimit(
    const std::string &wdtNamespace, int64_t maxNumSenders) {
  auto controller = getNamespaceController(wdtNamespace);
  if (controller) {
    controller->updateMaxSendersLimit(maxNumSenders);
  }
}

std::shared_ptr<Throttler> WdtResourceController::getWdtThrottler() const {
  return throttler_;
}

const WdtOptions &WdtResourceController::getOptions() const {
  return options_;
}

// TODO: consider putting strict/not strict handling logic here...
shared_ptr<WdtNamespaceController>
WdtResourceController::getNamespaceController(
    const string &wdtNamespace) const {
  GuardLock lock(controllerMutex_);
  auto it = namespaceMap_.find(wdtNamespace);
  if (it != namespaceMap_.end()) {
    return it->second;
  }
  return nullptr;
}

void WdtResourceController::requireRegistration(bool strict) {
  strictRegistration_ = strict;
}

}  // namespace wdt
}  // namespace facebook
