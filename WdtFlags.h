/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
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
