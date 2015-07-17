/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtOptions.h"
namespace facebook {
namespace wdt {

WdtOptions* WdtOptions::instance_ = nullptr;

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
