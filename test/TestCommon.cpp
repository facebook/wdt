/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <random>
#include <mutex>

using namespace std;

namespace facebook {
namespace wdt {

uint32_t rand32() {
  static std::default_random_engine randomEngine{std::random_device()()};
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);
  return randomEngine();
}
}
}
