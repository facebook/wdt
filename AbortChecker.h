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
  bool shouldAbort() const {
    return abortTriggerPtr_->load();
  }

 private:
  std::atomic<bool> const *abortTriggerPtr_;
};

}}
