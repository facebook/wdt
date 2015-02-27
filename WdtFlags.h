#pragma once
#include <gflags/gflags.h>
#include <iostream>
#include "WdtOptions.h"
#define DECLARE_ONLY
#include "WdtFlags.cpp.inc"
#undef DECLARE_ONLY
namespace facebook {
namespace wdt {
class WdtFlags {
 public:
  /**
   * Set the values of options in WdtOptions from corresponding flags
   */
  static void initializeFromFlags();

  static void printOptions();
};
}
}
