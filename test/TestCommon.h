/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <gtest/gtest.h>
#include <cstdint>

namespace facebook {
namespace wdt {
uint32_t rand32();
uint64_t rand64();

class TemporaryDirectory {
 public:
  TemporaryDirectory();
  ~TemporaryDirectory();

  const std::string& dir() const { return dir_; }

 private:
  std::string dir_;
};
}
}
