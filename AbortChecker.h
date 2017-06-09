/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <atomic>

namespace facebook {
namespace wdt {

/// Interface for external abort checks (pull mode)
class IAbortChecker {
 public:
  virtual bool shouldAbort() const = 0;
  virtual ~IAbortChecker() {
  }
};

/// A sample abort checker using std::atomic for abort
class WdtAbortChecker : public IAbortChecker {
 public:
  explicit WdtAbortChecker(const std::atomic<bool> &abortTrigger)
      : abortTriggerPtr_(&abortTrigger) {
  }
  bool shouldAbort() const override {
    return abortTriggerPtr_->load();
  }

 private:
  std::atomic<bool> const *abortTriggerPtr_;
};

}  // end namespaces
}
