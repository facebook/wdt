/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#include <wdt/ErrorCodes.h>
#include <wdt/test/TestCommon.h>

#include <boost/filesystem.hpp>
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

TemporaryDirectory::TemporaryDirectory() {
  char dir[] = "/tmp/wdtTest.XXXXXX";
  if (!mkdtemp(dir)) {
    WPLOG(FATAL) << "unable to make " << dir;
  }
  dir_ = dir;
}

TemporaryDirectory::~TemporaryDirectory() {
  boost::filesystem::remove_all(dir_);
}
}  // namespace wdt
}  // namespace facebook
