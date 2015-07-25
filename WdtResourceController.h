/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <unordered_map>
#include <vector>
#include <folly/Memory.h>
#include "ErrorCodes.h"
#include "Receiver.h"
#include "Sender.h"
#include "DirectorySourceQueue.h"
namespace facebook {
namespace wdt {
typedef std::shared_ptr<Receiver> ReceiverPtr;
typedef std::shared_ptr<Sender> SenderPtr;

/// A sample abort checker using std::atomic for abort
class WdtAbortChecker : public WdtBase::IAbortChecker {
 public:
  explicit WdtAbortChecker(const std::atomic<bool> &abortTrigger)
      : abortTriggerPtr_(&abortTrigger) {
  }
  bool shouldAbort() const {
    return abortTriggerPtr_->load();
  }

 private:
  std::atomic<bool> const *abortTriggerPtr_;
};

/**
 * Base class for both wdt global and namespace controller
 */
class WdtControllerBase {
 public:
  /// Constructor with a name for the controller
  explicit WdtControllerBase(const std::string& controllerName);

  /// Destructor
  virtual ~WdtControllerBase() {
  }

  /// Update max receivers limit
  virtual void updateMaxReceiversLimit(int64_t maxNumReceivers);

  /// Update max senders limit
  virtual void updateMaxSendersLimit(int64_t maxNumSenders);

  /// Setter for throttler
  virtual void setThrottler(std::shared_ptr<Throttler> throttler);

  /// Getter for throttler
  virtual std::shared_ptr<Throttler> getThrottler() const;

 protected:
  using GuardLock = std::unique_lock<std::mutex>;
  /// Number of active receivers
  int64_t numReceivers_{0};

  /// Number of active senders
  int64_t numSenders_{0};

  /// Maximum number of senders allowed for this namespace
  int64_t maxNumSenders_{0};

  /// Maximum number of receivers allowed for this namespace
  int64_t maxNumReceivers_{0};

  /// Mutex that protects all the private members of this class
  mutable std::mutex controllerMutex_;

  /// Throttler for this namespace
  std::shared_ptr<Throttler> throttler_{nullptr};

  /// Name of the resource controller
  std::string controllerName_;
};

/**
 * Controller defined per namespace if the user wants to divide
 * things between different namespaces (ex db shards)
 */
class WdtNamespaceController : public WdtControllerBase {
 public:
  /// Constructor with a name for namespace
  explicit WdtNamespaceController(const std::string& wdtNamespace);

  /// Add a receiver for this namespace
  ErrorCode createReceiver(const WdtTransferRequest& request,
                           ReceiverPtr& receiver);

  /// Add a sender for this namespace
  ErrorCode createSender(const WdtTransferRequest& request, SenderPtr& sender);

  /// Delete a receiver from this namespace
  ErrorCode releaseReceiver(const std::string& transferId);

  /// Delete a sender from this namespace
  ErrorCode releaseSender(const std::string& transferId);

  /// Releases all senders in this namespace
  int64_t releaseAllSenders();

  /// Releases all receivers in this namespace
  int64_t releaseAllReceivers();

  /// Get the sender created for transfer id
  SenderPtr getSender(const std::string& transferId) const;

  /// Get the receiver created for transfer id
  ReceiverPtr getReceiver(const std::string& transferId) const;

  /// Destructor, clears the senders and receivers
  virtual ~WdtNamespaceController() override;

 private:
  /// Map of receivers assosicated with transferId
  std::unordered_map<std::string, ReceiverPtr> receiversMap_;

  /// Map of senders assosciated with transferId
  std::unordered_map<std::string, SenderPtr> sendersMap_;
};

/**
 * A generic resource controller for wdt objects
 * User can set the maximum limit for receiver/sender
 * and organize them in different namespace
 */
class WdtResourceController : public WdtControllerBase {
 public:
  WdtResourceController();

  /// Add a sender specified with namespace
  ErrorCode createSender(const std::string& wdtNameSpace,
                         const WdtTransferRequest& request, SenderPtr& sender);

  /// Add a receiver specified with namespace
  ErrorCode createReceiver(const std::string& wdtNameSpace,
                           const WdtTransferRequest& request,
                           ReceiverPtr& receiver);

  /// Release a sender specified with namespace and identifier
  ErrorCode releaseSender(const std::string& wdtNamespace,
                          const std::string& transferId);

  /// Release a receiver specified with namespace and identifier
  ErrorCode releaseReceiver(const std::string& wdtNamespace,
                            const std::string& transferId);

  /// Register a wdt namespace
  ErrorCode registerWdtNamespace(const std::string& wdtNameSpace);

  /// De register a wdt namespace
  ErrorCode deRegisterWdtNamespace(const std::string& wdtNameSpace);

  /// Use the base class methods for global limits
  using WdtControllerBase::updateMaxReceiversLimit;
  using WdtControllerBase::updateMaxSendersLimit;

  /// Update max receivers limit of namespace
  void updateMaxReceiversLimit(const std::string& wdtNamespace,
                               int64_t maxNumReceivers);

  /// Update max senders limit of namespace
  void updateMaxSendersLimit(const std::string& wdtNamespace,
                             int64_t maxNumSenders);

  /// Release all senders in the specified namespace
  ErrorCode releaseAllSenders(const std::string& wdtNameSpace);

  /// Releases all receivers in specified namespace
  ErrorCode releaseAllReceivers(const std::string& wdtNamespace);

  /// Get a particular sender from a wdt namespace
  SenderPtr getSender(const std::string& wdtNamespace,
                      const std::string& transferId) const;

  /// Get a particular receiver from a wdt namespace
  ReceiverPtr getReceiver(const std::string& wdtNamespace,
                          const std::string& transferId) const;

  /// Cleanly shuts down the controller
  void shutdown();

  /// Destructor for the global resource controller
  virtual ~WdtResourceController() override;

 protected:
  typedef std::shared_ptr<WdtNamespaceController> NamespaceControllerPtr;
  /// Get the namespace controller
  NamespaceControllerPtr getNamespaceController(const std::string& wdtNamespace,
                                                bool isLock = false) const;

 private:
  /// Map containing the resource controller per namespace
  std::unordered_map<std::string, NamespaceControllerPtr> namespaceMap_;
};
}
}
