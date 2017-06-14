/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/**
 * Extracts bigrams from input. Example use:
 * cat * | wdt_gen_stats -wrap -binary > bigrams
 *
 */
#include <algorithm>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <wdt/WdtConfig.h>
#include "Bigram.h"

DEFINE_bool(binary, false, "Binary format output");
DEFINE_bool(wrap, false, "Consider last byte to wrap back to first");

std::ostream &operator<<(std::ostream &os, const MapOfBigramToCount &t) {
  // Transfer into vector to sort
  std::vector<PairBigramCount> dv(std::begin(t), std::end(t));
  std::sort(dv.begin(), dv.end(),
            [](const PairBigramCount &x, const PairBigramCount &y) {
              if (x.second == y.second) {
                // secondary criteria: ascending
                return x.first < y.first;
              } else {
                // primary sort: descending by count
                return x.second > y.second;
              }
            });
  for (const auto &entry : dv) {
    Bigram key = entry.first;
    uint32_t count = entry.second;
    if (FLAGS_binary) {
      key.binarySerialize(os);
      // Note that this generates a format not portable across endianness
      os.write(reinterpret_cast<const char *>(&count), sizeof(count));
    } else {
      os << "\t{" << key << ", " << count << "}," << std::endl;
    }
  }
  return os;
}
using std::string;

int main(int argc, char **argv) {
  FLAGS_logtostderr = true;
  // gflags api is nicely inconsistent here
  GFLAGS_NAMESPACE::SetArgv(argc, const_cast<const char **>(argv));
  GFLAGS_NAMESPACE::SetVersionString(WDT_VERSION_STR);
  string usage("Extract statistical model from input. v");
  usage.append(GFLAGS_NAMESPACE::VersionString());
  usage.append(". Sample usage:\n\t");
  usage.append(GFLAGS_NAMESPACE::ProgramInvocationShortName());
  usage.append(" [flags] < input > bigrams");
  GFLAGS_NAMESPACE::SetUsageMessage(usage);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  MapOfBigramToCount map;
  int first = getchar_unlocked(), c, prev = first;
  while ((c = getchar_unlocked()) >= 0) {
    Bigram b(prev, c);
    ++map[b];
    prev = c;
  }
  if (FLAGS_wrap) {
    Bigram wrap(prev, first);
    ++map[wrap];
  }
  std::cout << map;
  return 0;
}
