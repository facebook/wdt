/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <assert.h>
#include <time.h>
#include <array>
#include <fstream>
#include <future>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <random>
#include <vector>

#include <unistd.h>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <wdt/WdtConfig.h>
#include "Bigram.h"

/* Future "interface"/"features":

DEFINE_int32(dir_depth, 8, "Directory depth");
DEFINE_int32(files_per_dir, 8, "Files per directory");
DEFINE_int32(ascii_percent, 30, "Percentage of ASCII files");
DEFINE_double(total_size_gb, 10.0, "Total data set size in gigabytes");
*/
/// For now:
DEFINE_double(gen_size_mb, 10, "Size of data to generate in MBytes");
DEFINE_int32(gen_block_size, 16384, "Size of blocks to generate (in bytes)");
DEFINE_int32(num_threads, 16, "Number of threads");
DEFINE_bool(seed_with_time, false, "New (time based) seed for randomizer");
DEFINE_string(stats_source, "-",
              "Where to read bigram stats from, default is - for stdin");
DEFINE_string(start, "",
              "Start the chain at start - empty/default = random bigram");
DEFINE_string(reset_char, "",
              "In case of dead-end, try to start at that character, or rnd");
DEFINE_string(directory, ".", "Directory in which to generate data");
DEFINE_string(filename, "gen.data", "Base filename to use for generated data");

template <typename T>
class ProbabilityTable;  // forward for operator below

// needs to be before the friend below
template <typename T>
std::ostream &operator<<(std::ostream &os, const ProbabilityTable<T> &t) {
  uint32_t sz = t.size();
  os << "probT " << sz << ": ";
  if (!sz) {
    os << " <empty>" << std::endl;
    return os;
  }
  T prev = t.data_[0];
  int count = 1;
  for (uint32_t i = 1; i < t.size(); ++i) {
    Bigram cur = t.data_[i];
    if (cur == prev) {
      ++count;
      continue;
    }
    // else
    os << prev << " x " << count << ", ";
    count = 1;
    prev = cur;
  }
  os << prev << " x " << count << std::endl;
  return os;
}

// typedef std::knuth_b RndEngine;
// typedef std::mt19937 RndEngine;
// typedef std::ranlux48 RndEngine;
// This seems to be the fastest one:
typedef std::minstd_rand RndEngine;

static std::shared_ptr<RndEngine> createRandomGenerator(int offset) {
  std::shared_ptr<RndEngine> gen = std::make_shared<RndEngine>();
  time_t t = offset;
  if (FLAGS_seed_with_time) {
    t += time(nullptr);
    // Should technically be LOG(INFO) so if something fails the test can be
    // reproduced with the same seed - but it's verbose with lots of threads
    // and tests
    VLOG(1) << "Time Initializing RndEngine with " << t;
  } else {
    VLOG(1) << "Thread Index Initializing RndEngine with " << t;
  }
  gen->seed(t);
  return gen;
}

// Only ok to copy T multiple time inside if sizeof(T) !>> log2(input.size())*8
// TODO consider a tree or inlining the first few entries (ifs/range test equiv)
template <typename T>
class ProbabilityTable {
 public:
  // Constant based/source version:
  ProbabilityTable(std::initializer_list<std::pair<T, uint32_t>> input) {
    init(input);
  }
  explicit ProbabilityTable(std::vector<std::pair<T, uint32_t>> input) {
    init(input);
  }

  void init(std::vector<std::pair<T, uint32_t>> input) {
    uint32_t sum = 0;
    for (auto const &p : input) {
      auto const &b = p.first;
      VLOG(1) << "PT " << b << " weight " << p.second;
      sum += p.second;
    }
    LOG(INFO) << "Creating probability distribution for " << input.size()
              << " entries, total weight " << sum;
    data_.reserve(sum);
    // TODO: auto scale (divide by min/3 for instance?)
    dist_.reset(new std::uniform_int_distribution<int>(0, sum - 1));
    size_ = sum;
    for (auto const &p : input) {
      auto const &b = p.first;
      auto const &v = p.second;
      VLOG(1) << "PT " << b << " scaled weight " << v;
      for (uint32_t n = v; n--;) {
        data_.push_back(b);
      }
    }
    assert(data_.size() == sum);
  }
  // Incremental version
  ProbabilityTable() : size_(0) {
  }
  void append(const T &value, uint32_t count) {
    for (; count--;) {
      data_.push_back(value);
    }
  }
  void seal(const ProbabilityTable & /*parent*/) {
    size_ = data_.size();
    if (size_ > 0) {
      dist_.reset(new std::uniform_int_distribution<int>(0, size_ - 1));
    }
  }
  uint32_t size() const {
    return size_;
  }
  const T &operator[](uint32_t idx) const {
    return data_[idx];
  }
  const T &random(RndEngine &gen) {
    if (!size_) {
      static const T EMPTY;
      LOG(ERROR) << "Called random on 0 sized PT " << this;
      return EMPTY;
    }
    const int rIdx = (*dist_)(gen);
    VLOG(2) << "Random index " << rIdx << " -> " << data_[rIdx];
    return data_[rIdx];
  }

 private:
  uint32_t size_;
  std::vector<T> data_;
  std::unique_ptr<std::uniform_int_distribution<int>> dist_;
  template <typename A>
  friend std::ostream &operator<<(std::ostream &os,
                                  const ProbabilityTable<A> &t);
};

typedef ProbabilityTable<Bigram> PTB;

class SentenceGen {
 public:
  // For compiled in/static version:
  SentenceGen(std::initializer_list<PairBigramCount> input) : ptb0_(input) {
    init(input);
  }
  explicit SentenceGen(std::vector<PairBigramCount> input) : ptb0_(input) {
    init(input);
  }
  void init(std::vector<PairBigramCount> input) {
    if (!FLAGS_reset_char.empty()) {
      reset_char_ = FLAGS_reset_char[0];
    }
    for (auto const &p : input) {
      const Bigram &b = p.first;
      const char first = b[0];
      const int idx = char2index(first);
      ptb1_[idx].append(b, p.second);
    }
    // set the distribution/count and share the random gen
    for (int c = 0; c <= 255; ++c) {
      const int idx = char2index(c);
      ptb1_[idx].seal(ptb0_);
      if (ptb1_[idx].size() <= 0) {
        continue;
      }
      if (std::isprint(c)) {
        VLOG(1) << "For " << c << " '" << (char)c << "': " << ptb1_[idx];
      } else {
        VLOG(1) << "For " << c << " n/p: " << ptb1_[idx];
      }
    }
  }
  const Bigram &initial(RndEngine &gen) {
    return ptb0_.random(gen);
  }
  // Sets result a random Bigram starting with c - or returns false
  bool next(RndEngine &gen, Bigram &result, char c) {
    const int idx = char2index(c);
    VLOG(3) << "looking up for '" << Bigram::toPrintableString(c)
            << "' (at position " << idx << ") :" << ptb1_[idx] << std::endl;
    if (!ptb1_[idx].size()) {
      VLOG(1) << "No successor for " << Bigram::toPrintableString(c)
              << std::endl;
      return false;
    }
    Bigram newB = ptb1_[idx].random(gen);
    VLOG(2) << "picked " << Bigram::toPrintableString(c) << " -> " << newB
            << std::endl;
    result = newB;
    return true;
  }
  // Implace next() logic, chain the bigrams if possible, return false otherwise
  bool next(RndEngine &gen, Bigram &previous) {
    return next(gen, previous, previous[1]);
  }

  // will generate exactly len bytes unless len is < FLAGS_start.size()
  Bigram generateInitial(RndEngine &gen, std::string &result, int32_t len) {
    Bigram previous;
    // Start with given string or start randomly:
    size_t startLen = FLAGS_start.size();
    bool initDone = false;
    if (startLen) {
      result.append(FLAGS_start.substr(0, --startLen));
      len -= startLen;
      const char lastCharOfFirst = FLAGS_start[startLen];
      if (next(gen, previous, lastCharOfFirst)) {
        initDone = true;
      } else {
        result.push_back(lastCharOfFirst);
        --len;
      }
    }
    if (!initDone) {
      previous = initial(gen);
    }
    result.push_back(previous[0]);
    generate(gen, result, len - 1, previous);
    return previous;
  }

  void generate(RndEngine &gen, std::string &result, int32_t len,
                Bigram &previous) {
    VLOG(1) << "generate " << len << " " << previous;
    // main loop:
    while (len > 0) {
      bool found = next(gen, previous);
      if (!found) {
        result.push_back(previous[1]);
        --len;
        if (reset_char_) {
          if (next(gen, previous, reset_char_)) {
            continue;
          }
        }
        previous = initial(gen);
      }
      if (len <= 0) {
        return;
      }
      result.push_back(previous[0]);
      --len;
    }
  }

  static SentenceGen &getTestInstance() {
    static SentenceGen s_wg{
        {{"la"}, 3},    {{"ah"}, 2}, {{" B"}, 1}, {{" b"}, 1}, {{" f"}, 1},
        {{" l"}, 1},    {{". "}, 1}, {{"Bl"}, 1}, {{"Th"}, 1}, {{"az"}, 1},
        {{"bl"}, 1},    {{"e "}, 1}, {{"fo"}, 1}, {{"h "}, 1}, {{"h."}, 1},
        {{"he"}, 1},    {{"ox"}, 1}, {{"x."}, 1}, {{"y "}, 1}, {{"zy"}, 1},
        {{".\012"}, 1},
    };
    return s_wg;
  }

 private:
  uint32_t char2index(const char c) {
    return (unsigned char)c;
    /*
        assert(c >= 'a' && c <= 'z');
        return c - 'a';
    */
  }
  /// First level (new 'word')
  PTB ptb0_;
  /// Second level
  PTB ptb1_[256 /*1 + 'z' - 'a'*/];  // = 26
  /// Reset character (0/NUL means no special reset char)
  char reset_char_{0};
};

void deserialize(std::vector<PairBigramCount> &statsData, std::istream &sin) {
  while (!sin.fail()) {
    Bigram b;
    if (!b.binaryDeserialize(sin)) {
      break;
    }
    uint32_t count = 0;
    sin.read(reinterpret_cast<char *>(&count), sizeof(count));
    statsData.push_back(PairBigramCount(b, count));
  }
}

using std::string;

int main(int argc, char **argv) {
  FLAGS_logtostderr = true;
  // gflags api is nicely inconsistent here
  GFLAGS_NAMESPACE::SetArgv(argc, const_cast<const char **>(argv));
  GFLAGS_NAMESPACE::SetVersionString(WDT_VERSION_STR);
  string usage("Generates test files for wdt transfer benchmark. v");
  usage.append(GFLAGS_NAMESPACE::VersionString());
  usage.append(". Sample usage:\n\t");
  usage.append(GFLAGS_NAMESPACE::ProgramInvocationShortName());
  usage.append(" [flags] < Bigrams > generated");
  GFLAGS_NAMESPACE::SetUsageMessage(usage);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  std::vector<PairBigramCount> statsData;
  if (FLAGS_stats_source == "-") {
    LOG(INFO) << "Reading stdin for Bigram data... (produced by wdt_gen_stats)";
    deserialize(statsData, std::cin);
  } else {
    LOG(INFO) << "Reading " << FLAGS_stats_source << " for Bigram data... "
              << "(produced by wdt_gen_stats)";
    std::ifstream statsin(FLAGS_stats_source);
    if (!statsin.good()) {
      PLOG(FATAL) << "Unable to read bigrams from " << FLAGS_stats_source;
    }
    deserialize(statsData, statsin);
  }
  LOG(INFO) << "Found " << statsData.size() << " entries.";

  LOG(INFO) << "Will generate in directory=" << FLAGS_directory;
  if (chdir(FLAGS_directory.c_str())) {
    PLOG(FATAL) << "Error changing directory to " << FLAGS_directory;
  }

  const size_t totalTargetSz = FLAGS_gen_size_mb * 1024 * 1024;
  const size_t numThreads = FLAGS_num_threads;
  const size_t targetSzPerThread = totalTargetSz / numThreads;
  if (targetSzPerThread < 1) {
    LOG(FATAL) << "Invalid gen_size_mb and num_threads combo " << totalTargetSz;
  }
  const size_t totalSz = targetSzPerThread * numThreads;
  if (FLAGS_gen_block_size < 1) {
    LOG(FATAL) << "Invalid gen_block_size " << FLAGS_gen_block_size;
  }
  const size_t blockSz =
      std::min(targetSzPerThread, (size_t)FLAGS_gen_block_size);
  LOG(INFO) << "Requested " << totalSz << " (" << targetSzPerThread
            << " data * " << numThreads << " thread), " << blockSz
            << " at a time";
  LOG(INFO) << "Writting to " << FLAGS_filename;
  const int fd = open(FLAGS_filename.c_str(), O_CREAT | O_WRONLY, 0644);
  if (fd < 0) {
    PLOG(FATAL) << "Unable to open " << FLAGS_filename;
  }
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  SentenceGen sg(statsData);
  for (int i = numThreads; i--;) {
    threads.push_back(std::thread([targetSzPerThread, blockSz, fd, i, &sg] {
      std::shared_ptr<RndEngine> rndEngine = createRandomGenerator(i);
      size_t targetSz = targetSzPerThread;
      size_t toWrite = blockSz;
      off_t offset = i * targetSzPerThread;
      string res;
      res.reserve(toWrite);

      Bigram previous = sg.generateInitial(*rndEngine, res, toWrite);
      while (1) {
        CHECK_EQ(toWrite, res.size());
        ssize_t w = pwrite(fd, res.data(), res.size(), offset);
        if (w != static_cast<ssize_t>(res.size())) {
          PLOG(FATAL) << "Expected to write " << res.size() << " got " << w;
        }
        res.clear();
        targetSz -= toWrite;
        offset += toWrite;
        if (targetSz == 0) {
          break;
        }
        if (toWrite > targetSz) {
          toWrite = targetSz;
        }
        sg.generate(*rndEngine, res, toWrite, previous);
      }
    }));
  }
  for (int i = numThreads; i--;) {
    threads[i].join();
  }
  // TODO: calculate and print throughput
  return 0;
}
