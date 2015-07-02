#include "WdtOptions.h"
namespace facebook {
namespace wdt {

WdtOptions* WdtOptions::instance_ = nullptr;

const WdtOptions& WdtOptions::get() {
  return getMutable();
}

WdtOptions& WdtOptions::getMutable() {
  if (instance_ == nullptr) {
    instance_ = new WdtOptions();
  }
  return *instance_;
}
}
}
