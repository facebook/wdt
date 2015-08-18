/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtFlags.h"
#include "WdtFlags.cpp.inc"
#include <glog/logging.h>
#include "Protocol.h"

namespace facebook {
namespace wdt {
void WdtFlags::initializeFromFlags() {
  LOG(INFO) << "Running WDT " << Protocol::getFullVersion();
#define ASSIGN_OPT
#include "WdtFlags.cpp.inc"  //nolint
}
void WdtFlags::printOptions() {
#define PRINT_OPT
#include "WdtFlags.cpp.inc"  //nolint
}
}
}
