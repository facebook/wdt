/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <wdt/ErrorCodes.h>

namespace facebook {
namespace wdt {

/**
 * Interface to the OS
 */
class System {
 public:
  /// Constructor
  System(){};

  /// Destructor
  virtual ~System(){};

  /// Factory (typically returns "the" underlying System - except for tests,
  /// or alternate like hdfs...)
  static System &getDefault();
};
}
}  // namespace facebook::wdt
