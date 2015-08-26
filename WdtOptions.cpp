/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtOptions.h"
#include <glog/logging.h>
namespace facebook {
namespace wdt {

#define CHANGE_IF_NOT_SPECIFIED(option, specifiedOptions, value)  \
  if (specifiedOptions.find(#option) == specifiedOptions.end()) { \
    option = value;                                               \
  }

WdtOptions* WdtOptions::instance_ = nullptr;
const std::string WdtOptions::FLASH_OPTION_TYPE = "flash";
const std::string WdtOptions::DISK_OPTION_TYPE = "disk";

void WdtOptions::modifyOptions(
    const std::string& optionType,
    const std::set<std::string>& userSpecifiedOptions) {
  if (optionType == DISK_OPTION_TYPE) {
    CHANGE_IF_NOT_SPECIFIED(num_ports, userSpecifiedOptions, 1)
    CHANGE_IF_NOT_SPECIFIED(block_size_mbytes, userSpecifiedOptions, -1)
    return;
  }
  if (optionType != FLASH_OPTION_TYPE) {
    LOG(WARNING) << "Invalid option type " << optionType << ". Valid types are "
                 << FLASH_OPTION_TYPE << ", " << DISK_OPTION_TYPE;
  }
  // options are initialized for flash. So, no need to change anything
}

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
