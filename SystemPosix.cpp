/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "SystemPosix.h"

#include <glog/logging.h>

namespace facebook {
namespace wdt {

PosixSystem::PosixSystem() {
  LOG(INFO) << "Created PosixSystem instance";
}

PosixSystem::~PosixSystem() {
  LOG(INFO) << "Deleting instance of PosixSystem";
}

System &System::getDefault() {
  static PosixSystem sPosixSystem;
  return sPosixSystem;
}
}
}
