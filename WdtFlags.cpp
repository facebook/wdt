#include "WdtFlags.h"
#include "WdtFlags.cpp.inc"
namespace facebook {
namespace wdt {
void WdtFlags::initializeFromFlags() {
  #define ASSIGN_OPT
  #include "WdtFlags.cpp.inc" //nolint
}
void WdtFlags::printOptions() {
  #define PRINT_OPT
  #include "WdtFlags.cpp.inc" //nolint
}
}
}
