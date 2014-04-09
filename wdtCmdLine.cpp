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

#include "wdtlib.h"

#include <gflags/gflags.h>
#include <glog/logging.h>

DEFINE_string(directory, ".",
              "Source/Destination directory");
DEFINE_string(destination, "",
         "empty is server (destination) mode, non empty is destination host");
DEFINE_int32(num_sockets, 8, "Number of sockets");
DEFINE_int32(port, 22356, "Starting port number"); // W (D) T = 0x5754

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  LOG(INFO) << "Starting with directory=" << FLAGS_directory
            << " and destination=" << FLAGS_destination
            << " num sockets=" << FLAGS_num_sockets
            << " from port=" << FLAGS_port;

  if (FLAGS_destination.empty()) {
    wdtServer(FLAGS_port, FLAGS_num_sockets, FLAGS_directory);
  } else {
    wdtClient(FLAGS_destination, FLAGS_port, FLAGS_num_sockets,
              FLAGS_directory);
  }
  return 0;
}
