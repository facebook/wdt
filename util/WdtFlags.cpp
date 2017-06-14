/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/WdtFlags.h>
#include <wdt/Protocol.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

#include "WdtFlags.cpp.inc"

WDT_FLAG_DEFINITION(
    string, WDT_FLAG_SYM(option_type),
    facebook::wdt::WdtOptions::FLASH_OPTION_TYPE,
    "WDT option type. Options are initialized to different values "
    "depending on the type. Individual options can still be changed using "
    "specific flags. Use -" WDT_FLAG_STR(print_options) " to see values")

namespace facebook {
namespace wdt {

const std::string FLAGS_PREFIX = WDT_TOSTR(WDT_LONG_PREFIX);

// Internal utilities

std::string getOptionNameFromFlagName(const std::string &flagName) {
#ifndef STANDALONE_APP
  // extra wdt_ prefix is added in this case
  if (flagName.compare(0, FLAGS_PREFIX.size(), FLAGS_PREFIX) == 0) {
    // flagname begins with wdt_
    return flagName.substr(FLAGS_PREFIX.size());
  }
#endif
  return flagName;
}

// getFlagNameFromOptionName is WDT_FLAG_STR()

std::set<std::string> WdtFlags::getUserSpecifiedOptions() {
  std::set<std::string> userSpecifiedFlags;
  std::vector<GFLAGS_NAMESPACE::CommandLineFlagInfo> allFlags;
  GFLAGS_NAMESPACE::GetAllFlags(&allFlags);
  for (const auto &flag : allFlags) {
    if (!flag.is_default) {
      // is_default is false if the flag has been specified in the cmd line.
      // Even if the value specified is same as the default value, this boolean
      // is still marked as false. If the flag is set directly like
      // FLAGS_num_ports=1, is_default won't change. But, if it is set using
      // SetCommandLineOption, this will change.
      userSpecifiedFlags.emplace(getOptionNameFromFlagName(flag.name));
    }
  }
  return userSpecifiedFlags;
}

void WdtFlags::initializeFromFlags(WdtOptions &options) {
  WVLOG(1) << "Running WDT " << Protocol::getFullVersion();
#define ASSIGN_OPT
#include "WdtFlags.cpp.inc"  //nolint
#undef ASSIGN_OPT
  std::set<std::string> userSpecifiedFlags = getUserSpecifiedOptions();
  options.modifyOptions(WDT_FLAG_VAR(option_type), userSpecifiedFlags);
}

void WdtFlags::printOptions(std::ostream &out, const WdtOptions &options) {
  out << "Options current value:" << std::endl;
#define PRINT_OPT
#include "WdtFlags.cpp.inc"  //nolint
#undef PRINT_OPT
}
}
}
