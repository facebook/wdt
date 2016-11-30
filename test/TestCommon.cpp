/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <mutex>
#include <random>

using namespace std;

namespace facebook {
namespace wdt {

uint64_t rand64() {
  static std::default_random_engine randomEngine{std::random_device()()};
  static std::mutex mutex;
  std::lock_guard<std::mutex> lock(mutex);
  return randomEngine();
}

uint32_t rand32() {
  return static_cast<uint32_t>(rand64());
}
}
}
