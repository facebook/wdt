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
#include "ErrorCodes.h"
#include "Throttler.h"
#include "ClientSocket.h"
#include "WdtOptions.h"
#include "Reporting.h"

#include <chrono>
#include <memory>

namespace facebook {
namespace wdt {

class DirectorySourceQueue;

typedef std::chrono::high_resolution_clock Clock;

class Sender {
 public:
  Sender(const std::string &destHost, const std::string &srcDir);
  Sender(int port, int numSockets, const std::string &destHost,
         const std::string &srcDir);
  virtual ~Sender() {
  }

  TransferReport start();

  void setIncludeRegex(const std::string &includeRegex);

  void setExcludeRegex(const std::string &excludeRegex);

  void setPruneDirRegex(const std::string &pruneDirRegex);

  void setPort(const int port);

  void setNumSockets(const int numSockets);

  void setSrcFileInfo(const std::vector<FileInfo> &srcFileInfo);

  void setFollowSymlinks(const bool followSymlinks);

  // Making the following 2 functions public for unit testing. Need to find way
  // to unit test private functions
  virtual TransferStats sendOneByteSource(
      const std::unique_ptr<ClientSocket> &socket,
      const std::unique_ptr<Throttler> &throttler,
      const std::unique_ptr<ByteSource> &source, const bool doThrottling,
      const size_t totalBytes);

  void sendOne(Clock::time_point startTime, const std::string &destHost,
               int port, DirectorySourceQueue &queue, double avgRateBytes,
               double maxRateBytes, double bucketLimitBytes,
               TransferStats &threadStats,
               std::vector<TransferStats> &transferredFileStats);

  virtual std::unique_ptr<ClientSocket> makeSocket(const std::string &destHost,
                                                   int port);

 private:
  std::unique_ptr<ClientSocket> connectToReceiver(const std::string &destHost,
                                                  const int port,
                                                  ErrorCode &errCode,
                                                  Clock::time_point startTime);
  void validateTransferStats(
      const std::vector<TransferStats> &transferredSourceStats,
      const std::vector<TransferStats> &failedSourceStats,
      const std::vector<TransferStats> &threadStats);

  std::string destHost_;
  int port_;
  int numSockets_;
  std::string srcDir_;
  std::string pruneDirRegex_;
  std::string includeRegex_;
  std::string excludeRegex_;
  std::vector<FileInfo> srcFileInfo_;
  bool followSymlinks_;
};
}
}  // namespace facebook::wdt
