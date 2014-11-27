/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "DirectorySourceQueue.h"

#include <chrono>
#include <memory>

namespace facebook {
namespace wdt {

class DirectorySourceQueue;

typedef std::chrono::high_resolution_clock Clock;

class Sender {
 public:
  Sender(const std::string &destHost, int port, int numSockets,
         const std::string &srcDir,
         const std::vector<FileInfo> &srcFileInfo = {});

  virtual ~Sender() {
  }

  void start();

 private:
  void sendOne(Clock::time_point startTime, const std::string &destHost,
               int port, DirectorySourceQueue *queue, size_t *pHeaderBytes,
               size_t *pDataBytes, double avgRateBytes, double maxRateBytes,
               double bucketLimitBytes);

 private:
  std::string destHost_;
  int port_;
  int numSockets_;
  std::string srcDir_{""};
  std::vector<FileInfo> srcFileInfo_;
};
}
}  // namespace facebook::wdt
