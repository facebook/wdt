/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtOptions.h>
#include <glog/logging.h>
#include <wdt/Throttler.h>

namespace facebook {
namespace wdt {

/**
 * Macro to change the default of some flags based on some other flag
 * Example of usage:
 *  if (enable_download_resumption) {
 *    CHANGE_IF_NOT_SPECIFIED(overwrite, userSpecifiedOptions, true,
 *                            "(download resumption)")
 *  }
 */
#define CHANGE_IF_NOT_SPECIFIED(option, specifiedOptions, value, msg)         \
  if (specifiedOptions.find(#option) == specifiedOptions.end()) {             \
    WLOG(INFO) << "Setting " << #option << " to " << value << " " << msg;     \
    option = value;                                                           \
  } else {                                                                    \
    WLOG(INFO) << "Not overwriting user specified " << #option << " " << msg; \
  }

const char* WdtOptions::FLASH_OPTION_TYPE = "flash";
const char* WdtOptions::DISK_OPTION_TYPE = "disk";

void WdtOptions::modifyOptions(
    const std::string& optionType,
    const std::set<std::string>& userSpecifiedOptions) {
  if (optionType == DISK_OPTION_TYPE) {
    std::string msg("(disk option type)");
    CHANGE_IF_NOT_SPECIFIED(num_ports, userSpecifiedOptions, 3, msg)
    CHANGE_IF_NOT_SPECIFIED(block_size_mbytes, userSpecifiedOptions, -1, msg)
    CHANGE_IF_NOT_SPECIFIED(disable_preallocation, userSpecifiedOptions, true,
                            msg)
    CHANGE_IF_NOT_SPECIFIED(resume_using_dir_tree, userSpecifiedOptions, true,
                            msg)
    return;
  }
  if (optionType != FLASH_OPTION_TYPE) {
    WLOG(WARNING) << "Invalid option type " << optionType
                  << ". Valid types are " << FLASH_OPTION_TYPE << ", "
                  << DISK_OPTION_TYPE;
  }
  // options are initialized for flash. So, no need to change anything
  if (userSpecifiedOptions.find("start_port") != userSpecifiedOptions.end() &&
      !static_ports) {
    WLOG(INFO) << "start_port is specified, setting static_ports true";
    static_ports = true;
  }
}

bool WdtOptions::shouldPreallocateFiles() const {
#ifdef HAS_POSIX_FALLOCATE
  return !disable_preallocation;
#else
  return false;
#endif
}

bool WdtOptions::isLogBasedResumption() const {
  return enable_download_resumption && !resume_using_dir_tree;
}

bool WdtOptions::isDirectoryTreeBasedResumption() const {
  return enable_download_resumption && resume_using_dir_tree;
}

ThrottlerOptions WdtOptions::getThrottlerOptions() const {
  ThrottlerOptions throttlerOptions;
  throttlerOptions.avg_rate_per_sec = avg_mbytes_per_sec * kMbToB;
  throttlerOptions.max_rate_per_sec = max_mbytes_per_sec * kMbToB;
  throttlerOptions.throttler_bucket_limit = throttler_bucket_limit * kMbToB;
  throttlerOptions.throttler_log_time_millis = throttler_log_time_millis;
  // Expected request size equal to buffer size
  throttlerOptions.single_request_limit = buffer_size;
  return throttlerOptions;
}

/* static */
const WdtOptions& WdtOptions::get() {
  return getMutable();
}

WdtOptions& WdtOptions::getMutable() {
  static WdtOptions opt;
  return opt;
}

void WdtOptions::copyInto(const WdtOptions& src) {
  *this = src;
}
}
}
