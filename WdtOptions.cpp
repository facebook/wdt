#include "WdtOptions.h"
using std::string;
namespace facebook {
namespace wdt {
WdtOptions* WdtOptions::instance_ = new WdtOptions();
const WdtOptions& WdtOptions::get() {
  return *instance_;
}
WdtOptions& WdtOptions::getMutable() {
  return *instance_;
}
}
}
