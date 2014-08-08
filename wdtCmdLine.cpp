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

#include "Sender.h"
#include "Receiver.h"

#include <folly/String.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <iostream>

DEFINE_string(directory, ".", "Source/Destination directory");
DEFINE_bool(files,
            false,
            "If true, read a list of files and optional "
            "filesizes from stdin relative to the directory and transfer then");
DEFINE_string(
  destination,
  "",
  "empty is server (destination) mode, non empty is destination host");
DEFINE_int32(num_sockets, 8, "Number of sockets");
DEFINE_int32(port, 22356, "Starting port number"); // W (D) T = 0x5754
DEFINE_int32(backlog, 1, "Accept backlog");
// 256k is fastest for test on localhost and shm : > 5 Gbytes/sec
DEFINE_int32(buffer_size, 256 * 1024, "Buffer size (per thread/socket)");
DEFINE_int32(max_retries, 20, "how many attempts to connect/listen");
DEFINE_int32(sleep_ms, 50, "how many ms to wait between attempts");

int main(int argc, char* argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  LOG(INFO) << "Starting with directory=" << FLAGS_directory
            << " and destination=" << FLAGS_destination
            << " num sockets=" << FLAGS_num_sockets
            << " from port=" << FLAGS_port;

  if (FLAGS_destination.empty()) {
    facebook::wdt::Receiver receiver(
      FLAGS_port, FLAGS_num_sockets, FLAGS_directory);
    receiver.start();
  } else {
    std::vector<facebook::wdt::FileInfo> fileInfo;
    if (FLAGS_files) {
      // Each line should have the filename and optionally
      // the filesize separated by a single space
      std::string line;
      while (std::getline(std::cin, line)) {
        std::vector<std::string> fields;
        folly::split('\t', line, fields, true);
        if (fields.empty() || fields.size() > 2) {
          LOG(FATAL) << "Invalid input in stdin: " << line;
        }
        int64_t filesize = fields.size() > 1 ? folly::to<int64_t>(fields[1])
                                             : -1;
        fileInfo.emplace_back(fields[0], filesize);
      }
    }
    facebook::wdt::Sender sender(FLAGS_destination,
                                 FLAGS_port,
                                 FLAGS_num_sockets,
                                 FLAGS_directory,
                                 fileInfo);
    sender.start();
  }
  return 0;
}
