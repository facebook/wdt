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
#include <wdt/Receiver.h>
#include <wdt/Sender.h>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace wdt {

typedef std::shared_ptr<Receiver> ReceiverPtr;
typedef std::shared_ptr<Sender> SenderPtr;

/**
 * Base class for both wdt global and namespace controller
 */
class WdtControllerBase {
 public:
  /// Constructor with a name for the controller
  explicit WdtControllerBase(const std::string &controllerName);

  /// Destructor
  virtual ~WdtControllerBase() {
  }

  /// Update max receivers limit
  virtual void updateMaxReceiversLimit(int64_t maxNumReceivers);

  /// Update max senders limit
  virtual void updateMaxSendersLimit(int64_t maxNumSenders);

 protected:
  using GuardLock = std::unique_lock<std::recursive_mutex>;
  /// Number of active receivers
  int64_t numReceivers_{0};

  /// Number of active senders
  int64_t numSenders_{0};

  /// Maximum number of senders allowed for this namespace
  int64_t maxNumSenders_{0};

  /// Maximum number of receivers allowed for this namespace
  int64_t maxNumReceivers_{0};

  /// Mutex that protects all the private members of this class
  mutable std::recursive_mutex controllerMutex_;

  /// Name of the resource controller
  std::string controllerName_;
};

class WdtResourceController;

/**
 * Controller defined per namespace if the user wants to divide
 * things between different namespaces (ex db shards)
 */
class WdtNamespaceController : public WdtControllerBase {
 public:
  /// Constructor with a name for namespace
  WdtNamespaceController(const std::string &wdtNamespace,
                         const WdtResourceController *const parent);

  /// Is free to create sender.
  bool hasSenderQuota() const;

  /// Add a receiver for this namespace with identifier
  ErrorCode createReceiver(const WdtTransferRequest &request,
                           const std::string &identifier,
                           ReceiverPtr &receiver);

  /// Is free to create receiver.
  bool hasReceiverQuota() const;

  /// Add a sender for this namespace with identifier
  ErrorCode createSender(const WdtTransferRequest &request,
                         const std::string &identifier, SenderPtr &sender);

  /// Delete a receiver from this namespace
  ErrorCode releaseReceiver(const std::string &identifier);

  /// Delete a sender from this namespace
  ErrorCode releaseSender(const std::string &identifier);

  /// Releases all senders in this namespace
  int64_t releaseAllSenders();

  /// Releases all receivers in this namespace
  int64_t releaseAllReceivers();

  /**
   * Get the sender you created by the createSender API
   * using the same identifier you mentioned before
   */
  SenderPtr getSender(const std::string &identifier) const;

  /**
   * Get the receiver you created by the createReceiver API
   * using the same identifier you mentioned before
   */
  ReceiverPtr getReceiver(const std::string &identifier) const;

  /// Get all senders
  std::vector<SenderPtr> getSenders() const;

  /// Get all receivers
  std::vector<ReceiverPtr> getReceivers() const;

  // Get all senders ids
  std::vector<std::string> getSendersIds() const;

  /// Clear the senders that are not active anymore
  std::vector<std::string> releaseStaleSenders();

  /// Clear the receivers that are not active anymore
  std::vector<std::string> releaseStaleReceivers();

  /// Destructor, clears the senders and receivers
  ~WdtNamespaceController() override;

 private:
  /// Map of receivers associated with identifier
  std::unordered_map<std::string, ReceiverPtr> receiversMap_;

  /// Map of senders associated with identifier
  std::unordered_map<std::string, SenderPtr> sendersMap_;

  /// Throttler for this namespace
  const WdtResourceController *const parent_;
};

/**
 * A generic resource controller for wdt objects
 * User can set the maximum limit for receiver/sender
 * and organize them in different namespace
 */
class WdtResourceController : public WdtControllerBase {
 public:
  /// resource controller should take the option as reference so that it can be
  /// changed later from the parent object
  WdtResourceController(const WdtOptions &options,
                        std::shared_ptr<Throttler> throttler);
  explicit WdtResourceController(const WdtOptions &options);
  WdtResourceController();

  /// Is free to create sender specified by namespace.
  bool hasSenderQuota(const std::string &wdtNamespace) const;

  /**
   * Add a sender specified by namespace and a identifier.
   * You can get this sender back by using the same identifier
   */
  ErrorCode createSender(const std::string &wdtNamespace,
                         const std::string &identifier,
                         const WdtTransferRequest &request, SenderPtr &sender);

  /// Is free to create receiver specified by namespace.
  bool hasReceiverQuota(const std::string &wdtNamespace) const;

  /// Add a receiver specified with namespace and identifier
  ErrorCode createReceiver(const std::string &wdtNamespace,
                           const std::string &identifier,
                           const WdtTransferRequest &request,
                           ReceiverPtr &receiver);

  /// Release a sender specified with namespace and identifier
  ErrorCode releaseSender(const std::string &wdtNamespace,
                          const std::string &identifier);

  /// Release a receiver specified with namespace and identifier
  ErrorCode releaseReceiver(const std::string &wdtNamespace,
                            const std::string &identifier);

  /// Register a wdt namespace (if strict mode)
  ErrorCode registerWdtNamespace(const std::string &wdtNamespace);

  /// De register a wdt namespace
  ErrorCode deRegisterWdtNamespace(const std::string &wdtNamespace);

  /// Use the base class methods for global limits
  using WdtControllerBase::updateMaxReceiversLimit;
  using WdtControllerBase::updateMaxSendersLimit;

  /// Update max receivers limit of namespace
  void updateMaxReceiversLimit(const std::string &wdtNamespace,
                               int64_t maxNumReceivers);

  /// Update max senders limit of namespace
  void updateMaxSendersLimit(const std::string &wdtNamespace,
                             int64_t maxNumSenders);

  /// Release all senders in the specified namespace
  ErrorCode releaseAllSenders(const std::string &wdtNamespace);

  /// Releases all receivers in specified namespace
  ErrorCode releaseAllReceivers(const std::string &wdtNamespace);

  /// Get a particular sender from a wdt namespace
  SenderPtr getSender(const std::string &wdtNamespace,
                      const std::string &identifier) const;

  /// Get a particular receiver from a wdt namespace
  ReceiverPtr getReceiver(const std::string &wdtNamespace,
                          const std::string &identifier) const;

  /// Get all senders in a namespace
  std::vector<SenderPtr> getAllSenders(const std::string &wdtNamespace) const;

  /// Get all senders ids in a namespace
  std::vector<std::string> getAllSendersIds(
      const std::string &wdtNamespace) const;

  /// Get all receivers in a namespace
  std::vector<ReceiverPtr> getAllReceivers(
      const std::string &wdtNamespace) const;

  /// Clear the senders that are no longer active.
  ErrorCode releaseStaleSenders(const std::string &wdtNamespace,
                                std::vector<std::string> &erasedIds);

  /// Clear the receivers that are no longer active
  ErrorCode releaseStaleReceivers(const std::string &wdtNamespace,
                                  std::vector<std::string> &erasedIds);

  /**
   * Call with true to require registerWdtNameSpace() to be called
   * before requesting sender/receiver for that namespace.
   */
  void requireRegistration(bool isStrict);

  /// Cleanly shuts down the controller
  void shutdown();

  /// @return     Singleton instance of the controller
  static WdtResourceController *get();

  /// Destructor for the global resource controller
  ~WdtResourceController() override;

  /// Default global namespace
  static const char *const kGlobalNamespace;

  /// Return current counts
  ErrorCode getCounts(int32_t &numNamespaces, int32_t &numSenders,
                      int32_t &numReceivers);

  /**
   * getter for throttler.
   * setThrottlerRates to this throttler may not take effect. Instead, update
   * WdtOptions accordingly.
   * Applications have to register transfers with the throttler, and at the end
   * de-register it. For example-
   * throttler->startTransfer();
   * ...
   * throttler->limit(numBytes);
   * ...
   * throttler->endTransfer();
   */
  std::shared_ptr<Throttler> getWdtThrottler() const;

  const WdtOptions &getOptions() const;

 protected:
  typedef std::shared_ptr<WdtNamespaceController> NamespaceControllerPtr;
  /// Get the namespace controller
  NamespaceControllerPtr getNamespaceController(
      const std::string &wdtNamespace) const;

 private:
  NamespaceControllerPtr createNamespaceController(const std::string &name);
  /// Map containing the resource controller per namespace
  std::unordered_map<std::string, NamespaceControllerPtr> namespaceMap_;
  /// Whether namespace need to be created explictly
  bool strictRegistration_{false};
  /// Throttler for all the namespaces
  std::shared_ptr<Throttler> throttler_{nullptr};
  const WdtOptions &options_;
  /// Internal method for checking hasSenderQuota & hasReceiverQuota
  bool hasSenderQuotaInternal(const std::shared_ptr<WdtNamespaceController>
                                  &controller = nullptr) const;
  bool hasReceiverQuotaInternal(const std::shared_ptr<WdtNamespaceController>
                                    &controller = nullptr) const;
};
}  // namespace wdt
}  // namespace facebook
