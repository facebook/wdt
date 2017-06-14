/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */

#include <wdt/test/TestCommon.h>

#include <stdlib.h>

#include <mutex>
#include <random>

#include <boost/filesystem.hpp>
#include <wdt/ErrorCodes.h>

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
}
}
