/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 *
 *  Created on: Oct 18, 2011
 *      Author: ldemailly
 */
#include <iostream>

#include <folly/Benchmark.h>
#include <folly/Singleton.h>

#include <wdt/util/Stats.h>

#include <fb303/ExportedHistogramMapImpl.h>

DEFINE_int32(num_threads, 8, "number of threads");
DEFINE_double(print_interval, 1, "number of seconds in between 2 stats dump");

namespace facebook {
namespace wdt {

template <typename... Args>
void forkjoin(int numThreads, void (*f)(Args...), Args... args) {
  if (numThreads < 1) {
    numThreads = 1;
  }
  std::vector<std::thread> threads;
  threads.reserve(numThreads);
  for (int i = 0; i < numThreads; ++i) {
    threads.emplace_back(std::thread(f, std::forward<Args>(args)...));
  }
  for (auto& t : threads) {
    t.join();
  }
}

struct TestTLHistograms {
  TestTLHistograms()
      : h1(100, [](const Histogram& h) { h.print(std::cerr); }),
        h2(100, [](const Histogram& h) { h.print(std::cerr); }) {
    LOG(INFO) << "Created 2 thread local histograms " << this << " " << &h1;
  }
  ~TestTLHistograms() {
    LOG(INFO) << "Deleting thread local histograms " << this;
  }
  ThreadLocalHistogram h1;
  ThreadLocalHistogram h2;
};

folly::Singleton<TestTLHistograms> test_histograms_s;

BENCHMARK_MULTI(HistogramBench) {
  int iters = 10000000;  // 10M
  VLOG(1) << "called with " << iters;
  ThreadLocalHistogram* h = &(test_histograms_s.try_get()->h1);
  PeriodicCounters pcs({h});
  pcs.schedule(FLAGS_print_interval);
  for (int i = 0; i < iters; ++i) {
    h->record(i);
  }
  return iters;
}

void runPHistTest(int maxVal, ThreadLocalHistogram* h) {
  for (int i = 0; i < maxVal; ++i) {
    h->record(i);
  }
}

BENCHMARK_MULTI(MtPerHistogram) {
  int iters = 400000000;  // 400 M
  ThreadLocalHistogram* h = &(test_histograms_s.try_get()->h2);
  PeriodicCounters pcs({h});
  pcs.schedule(FLAGS_print_interval);
  forkjoin(FLAGS_num_threads, runPHistTest, iters, h);
  return iters;
}

static fb303::DynamicCounters dynamicCounters;
static fb303::DynamicStrings dynamicStrings;
static fb303::ExportedHistogram basehist(1000, 0, 10000);
static fb303::ExportedHistogramMapImpl ch(&dynamicCounters, &dynamicStrings,
                                          basehist);

void runCommonHistTest(int iters) {
  time_t now = time(nullptr);
  for (int64_t i = 0; i < iters; ++i) {
    ch.addValue("foo", now, i);
  }
}

BENCHMARK_MULTI(MtExpHistMap) {
  int iters = 1000000;  // 1M (a lot slower than above)
  ch.addHistogram("foo");
  forkjoin(FLAGS_num_threads, runCommonHistTest, iters);
  BENCHMARK_SUSPEND {
    std::cout << "exported hist count:" << ch.getHistogram("foo")->count(0)
              << std::endl;
  }
  return iters;
}

// experiments thread local vs atomic basic increment: (ifdef'ed out for normal)
#if 0
/*__thread */ int64_t inc = 0;

struct MyTL {
  MyTL() {
    void* cb = 0;
    LOG(VERBOSE) << "making a new TL " << this << " cb=" << cb;
  }
  ~MyTL() {
    LOG(VERBOSE) << "~TL " << this;
  }
  int64_t c;
};

folly::ThreadLocal<MyTL> tl;

void incrOne(int v) {
  //int j = v + v + inc;
  tl->c += v;
}

void runIncrementTest(int iters) {
  for (int i=0; i < iters; ++i) {
    //atomic_add(inc, i);
    incrOne(i);
  }
}

BENCHMARK_MULTI(MtIncrement) {
  int iters = 10000;
  forkjoin(FLAGS_num_threads, runIncrementTest, iters);
  std::cout << "mt count:" << inc << std::endl;
  return iters;
}
#endif

}  // namespace wormhole
}  // namespace facebook

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  folly::runBenchmarks();
  return 0;
}
