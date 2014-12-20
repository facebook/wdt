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

#include "FileCreator.h"
#include "ErrorCodes.h"
#include "WdtOptions.h"
#include <memory>
#include <string>

namespace facebook {
namespace wdt {

class Receiver {
 public:
  Receiver(int port, int numSockets, std::string destDir);

  virtual ~Receiver() {
  }

  std::vector<ErrorCode> start();

 private:
  void receiveOne(int port, int backlog, const std::string &destDir,
                  size_t bufferSize, ErrorCode &errCode);

 private:
  int port_;
  int numSockets_;
  std::string destDir_;
  std::unique_ptr<FileCreator> fileCreator_;
};
}
}  // namespace facebook::wdt
