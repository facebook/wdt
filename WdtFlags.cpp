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
#include <folly/Conv.h>

FLAG_DEFINITION(string, PREFIX(option_type),
                facebook::wdt::WdtOptions::FLASH_OPTION_TYPE,
                "WDT option type. Options are initialized to different values "
                "depending on the type. Individual options can still be "
                "changed using specific flags.")

namespace facebook {
namespace wdt {

const std::string FLAGS_PREFIX = "wdt_";

void WdtFlags::initializeFromFlags() {
  LOG(INFO) << "Running WDT " << Protocol::getFullVersion();
#define ASSIGN_OPT
#include "WdtFlags.cpp.inc"  //nolint
#undef ASSIGN_OPT
  std::set<std::string> userSpecifiedFlags = getUserSpecifiedOptions();
  WdtOptions::getMutable().modifyOptions(FLAGS_OPTION_TYPE, userSpecifiedFlags);
}

void WdtFlags::printOptions() {
#define PRINT_OPT
#include "WdtFlags.cpp.inc"  //nolint
#undef PRINT_OPT
}

std::string WdtFlags::getOptionNameFromFlagName(const std::string &flagName) {
#ifndef STANDALONE_APP
  // extra wdt_ prefix is added in this case
  if (flagName.compare(0, FLAGS_PREFIX.size(), FLAGS_PREFIX) == 0) {
    // flagname begins with wdt_
    return flagName.substr(FLAGS_PREFIX.size());
  }
#endif
  return flagName;
}

std::string WdtFlags::getFlagNameFromOptionName(const std::string &optionName) {
#ifndef STANDALONE_APP
  // extra wdt_ prefix has to be added
  std::string flagName;
  folly::toAppend(FLAGS_PREFIX, optionName, &flagName);
  return flagName;
#endif
  return optionName;
}

std::set<std::string> WdtFlags::getUserSpecifiedOptions() {
  std::set<std::string> userSpecifiedFlags;
  std::vector<google::CommandLineFlagInfo> allFlags;
  google::GetAllFlags(&allFlags);
  for (const auto &flag : allFlags) {
    if (!flag.is_default) {
      // is_default is false if the flag has been specified in the cmd line.
      // Even if the value specified is same as the default value, this boolean
      // is still marked as false. If the flag is directly like
      // FLAGS_num_ports=1, is_default won't change. But, if it set using
      // SetCommandLineOption, this will change.
      userSpecifiedFlags.emplace(getOptionNameFromFlagName(flag.name));
    }
  }
  return userSpecifiedFlags;
}
}
}
