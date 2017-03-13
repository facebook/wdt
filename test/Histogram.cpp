/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/**
 * Main/Test and standalone for Histogram:
 * reads values from stdin and prints the stats and histogram at the end.
 *
 *  Created on: Oct 18, 2011
 *      Author: ldemailly
 */

#include <gflags/gflags.h>
#include <wdt/util/Stats.h>

DEFINE_int32(divider, 1, "divider/scale");
DEFINE_int64(offset, 0, "offset for the data");
DEFINE_double(percentile1, 85, "target percentile 1");
DEFINE_double(percentile2, 99.9, "target percentile 2");

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int64_t v;
  facebook::wdt::Histogram h(FLAGS_divider, FLAGS_percentile1,
                             FLAGS_percentile2, FLAGS_offset);
  while (1) {
    std::cin >> v;
    if (std::cin.eof()) {
      break;
    }
    h.record(v);
  }
  h.print(std::cout);
  return 0;  // not reached
}
