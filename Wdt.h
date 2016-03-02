/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/Sender.h>
// For Options
#include <wdt/WdtOptions.h>
// For ErrorCode
#include <wdt/ErrorCodes.h>
// For IAbortChecker and WdtTransferRequest - TODO: split out ?
#include <wdt/WdtBase.h>
#include <wdt/util/EncryptionUtils.h>
#include <ostream>

namespace facebook {
namespace wdt {

// Note: we use Wdt in the method names even if redundant with the class name
// so we can search callers easily

/**
 * This class is the main API and entry point for using WDT (Warp speed Data
 * Transfers).
 *
 * Example of use:
 * // During the single threaded part of your service's initialization
 * Wdt &wdt = Wdt::initializeWdt();
 * // Optionally: change the WdtOptions as you need, for instance:
 * wdt.getWdtOptions().overwrite = true;
 * // Will use the (possibly changed above) settings, to configure wdt,
 * // for instance throttler options
 * // Sender for already setup receiver: (abortChecker is optional)
 * wdtSend(transferRequest, myAbortChecker);
 */
class Wdt {
 public:
  /**
   * Initialize the Wdt library and parse options from gflags.
   * Also initialize crypto library if needed/not already initialized.
   * @param:
   *   applicationName is used at fb for scuba reporting to differentiate apps
   */
  static Wdt &initializeWdt(const std::string &appName);
  /**
   * Mutable reference to WdtOptions
   */
  WdtOptions &getWdtOptions();
  /**
   * Return Wdt lib handle which has been previously initialized by calling
   * @see initializeWdt()
   * This is only needed if the caller code doesn't want to pass the Wdt
   * instance around.
   */
  static Wdt &getWdt();

  /// High level APIs:

  // TODO: add receiver side...

  /**
   * Send data for the shard identified by shardId to an already running/setup
   * receiver whose connection url was used to make a WdtTransferRequest.
   * Optionally passes an abort checker.
   */
  virtual ErrorCode wdtSend(
      const std::string &wdtNamespace, const WdtTransferRequest &wdtRequest,
      std::shared_ptr<IAbortChecker> abortChecker = nullptr,
      bool terminateExistingOne = false);

  virtual ErrorCode printWdtOptions(std::ostream &out);

 protected:
  /// Set to true when the single instance is initialized
  bool initDone_{false};

  /**
   * Set to true when settings are applied to avoid applying again
   * This is needed because you can call wdtSend() independent of the thrift
   * service
   */
  bool settingsApplied_{false};

  /// App name which is used in scuba reporting
  std::string appName_;

  WdtOptions &options_;

  /// responsible for initializing openssl
  WdtCryptoIntializer cryptoInitializer_;

  // Internal initialization so sub classes can share the code
  virtual ErrorCode initializeWdtInternal(const std::string &appName);

  // Optionally set socket creator and progress reporter (used for fb)
  virtual ErrorCode wdtSetAbortSocketCreatorAndReporter(
      const std::string &wdtNamespace, Sender *sender,
      const WdtTransferRequest &req,
      std::shared_ptr<IAbortChecker> abortChecker);

  // Internal singleton creator/holder
  static Wdt &getWdtInternal();

  /**
   * Apply the possibly changed settings, eg throttler.
   * Automatically called for you.
   */
  ErrorCode applySettings();

  /// Private constructor (TODO options to be returned by initializeFromFlags)
  explicit Wdt(WdtOptions &opts) : options_(opts) {
  }

  Wdt() = delete;

  /// Not copyable
  Wdt(const Wdt &) = delete;
  Wdt &operator=(const Wdt &) = delete;

  /// Virtual Destructor (for class hierarchy)
  virtual ~Wdt() {
  }
};
}
}  // namespaces
