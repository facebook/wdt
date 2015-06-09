#include "WdtOptions.h"
using std::string;
namespace facebook {
namespace wdt {
const WdtOptions& WdtOptions::get() {
  return getMutable();
}
WdtOptions& WdtOptions::getMutable() {
  static WdtOptions* instance = new WdtOptions();
  return *instance;
}
}
}
