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

#define FLAGS_OPTION_TYPE FLAG_VALUE(option_type)
FLAG_DECLARATION(string, PREFIX(option_type))

namespace facebook {
namespace wdt {
class WdtFlags {
 public:
  /**
   * Set the values of options in WdtOptions from corresponding flags
   */
  static void initializeFromFlags();

  static void printOptions();

  /**
   * Returns option name from flag name
   */
  static std::string getOptionNameFromFlagName(const std::string &flagName);

  /**
   * Returns flag name from option name
   */
  static std::string getFlagNameFromOptionName(const std::string &optionName);

  /// returns list of options specified in the cmd line by the user
  static std::set<std::string> getUserSpecifiedOptions();
};
}
}
