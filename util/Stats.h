/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/*
* Stats.h
*
* Lockless, threadsafe, efficient semi log periodic histogram/stats.
*
* Originally written for wormhole but now opensource as part of WDT
*
* This file is performance sensitive - for any change make sure :
* (print interval set at 1ms to check that there is no contention between
* the printing thread and the others)
* With opt build, sends the stats to stderr as benchmark now prints to stdout:
*
* ./buck-out/gen/wdt/stats_benchmark -bm_regex="MtPer" \
*   -print_interval=0.001 -minloglevel=3 -num_threads=6 2> /tmp/out
*
*  2016 results:  on Intel(R) Xeon(R) CPU E5-2660 0 @ 2.20GHz - 6 threads:
*  MtPerHistogram   1 thread (no concurrency/best case:)  12.40ns   80.64M,
*  6 threads: 12.78ns   78.24,  12 threads:     13.04ns   76.67M
*  so it scales linearly
*
*  Old 2011 results:
*  gives above 45M/s (on 8 core Xeon L5410  @ 2.33GHz):
*  BM_mt_per_histogram                 400000000  8.432 s   21.08 ns  45.24 M
*  and above 80M/s (on 32 htcores Xeon(R) CPU E5-2660 0 @ 2.20GHz):
*  BM_mt_per_histogram                 400000000  4.644 s   11.61 ns  82.14 M
*  should also scale up to # thread equal to number of real cores,
*  ie 16 for the above
*
*  and make sure that despite resetting every millisecond we do get:
*  awk -F,   '($1 ~ /[<>]/) {sum+=$4} END {print "sum is", sum}' /tmp/out
*  gives (for -num_threads=12):
*  sum is 4800000000
*  (12 threads doing 400M each -> 4.8B should be found in the histograms)
*  and /tmp/out should be large (~3 Mbytes)
*
* ps: could be even faster if templating the divider as this is the most
*     expensive part
*
*  Created on: Oct 18, 2011
*      Author: ldemailly
*/
#ifndef STATS_H_
#define STATS_H_

#include <sys/types.h>
#include <cmath>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <thread>

#include <boost/noncopyable.hpp>

#include <folly/ThreadLocal.h>

#include "glog/logging.h"

namespace facebook {
namespace wdt {

// Note that atomic operations across threads are actually quite slow
// so we use ThreadLocal storage instead and swipe the aggregates
typedef volatile int64_t vint64_t;

/**
 * Atomic add
 */
inline static int64_t atomic_add(vint64_t& data, int64_t v) {
  return __sync_add_and_fetch(&data, v);
}

/**
 * Atomic set - older/previous likely value is known/fetched already
 * Safest (spin lock until it's set)
 */
inline static void atomic_set_old(vint64_t& dest, int64_t oldVal, int64_t val) {
  int64_t prevVal;
  while (1) {
    prevVal = __sync_val_compare_and_swap(&dest, oldVal, val);
    if (prevVal == oldVal) {
      break;  // swap/assign did work
    }
    VLOG(5) << "one more iter to set " << oldVal << " found " << prevVal;
    oldVal = prevVal;  // try again
  }
}

/**
 * Atomic set when the previous value is unknown. Safest.
 */
inline static void atomic_set(vint64_t& dest, int64_t value) {
  atomic_set_old(dest, dest, value);
}

/**
 * Atomic set when the previous value is unknown
 * Avoid loop and might be working like the above - or not :-)
 * http://gcc.gnu.org/onlinedocs/gcc-4.6.2/gcc/Atomic-Builtins.html
 */
inline static void atomic_set2(vint64_t& dest, int64_t value) {
  __sync_lock_test_and_set(&dest, value);
}

/**
 * Atomic get
 * not const arg because we actually +0 ...
 */
inline static int64_t atomic_get(vint64_t& data) {
  return atomic_add(data, 0);
}

/**
 * Because of gcc strangeness in array initialization (in Hold2 below):
 * it requires the presence of a visible copy constructor - yet (thankfully!)
 * doesn't call it so we need a replacement for boost::noncopyable
 */
class crashifcopied {
 public:
  crashifcopied(const crashifcopied& /*c*/) {
    CHECK(false);
  }

 protected:
  crashifcopied() {
  }
  ~crashifcopied() {
  }

 private:
  const crashifcopied& operator=(const crashifcopied&);
};

/**
 * Basic counter with stats: count, min, max, avg, stddev.
 * Not atomic and not locked - use PeriodicCounters or ThreadLocalCounter for
 * thread local and aggregated version which is faster than atomic
 */
class Counter : crashifcopied {
 public:
  explicit Counter(double printScale = 1.0)
      : count_(0),
        realMin_(0),
        min_(0),
        max_(0),
        sum_(0),
        sumOfSquares_(0),
        printScale_(printScale) {
  }

  virtual ~Counter() {
  }

  void record(int64_t v);

  inline int64_t getCount() const {
    return count_;
  }

  // Includes 0s
  inline int64_t getRealMin() const {
    return realMin_;
  }
  // Excludes 0
  inline int64_t getMin() const {
    return min_;
  }

  inline int64_t getMax() const {
    return max_;
  }

  inline int64_t getSum() const {
    return sum_;
  }

  inline double getAverage() const {
    if (count_ == 0) {
      return NAN;
    }
    return (double)sum_ / count_;
  }

  double getStdDev() const;

  /**
   * Prints values - data is multipled by printScale_
   * coma separated values:
   * count, avg, min, max, stddev
   * optionally override the constructor passed scale
   */
  void print(std::ostream& os, double scale = 0) const;

  /**
   * prints "count,avg,min,max,stddev"
   */
  void printCounterHeader(std::ostream& os) const;

  /**
   * resets all the data (not quite thread safe - see ThreadLocalHistogram)
   */
  inline void reset() {
    count_ = min_ = max_ = sum_ = sumOfSquares_ = 0;
  }

  /**
   * Takes one counter and flush it into this one as if data for both
   * has been set on this one (used to merge threadlocal copies)
   */
  void merge(const Counter& c);

 private:
  int64_t count_, realMin_, min_, max_, sum_, sumOfSquares_;
  double printScale_;
};

/**
 * Semi logarithmic non atomic and unlocked Histogram. ThreadLocalHistogram
 * is the lock free thread safe version/wrapper - see below.
 */
class Histogram : public Counter {
 public:
  /**
   * Data will have "offset" substracted and be divided by "scale" before
   * being placed into a bucket.
   * The counter will have the row data (stddev may still overflow for large
   * values - good value for scale would be 1-1000)
   *
   * So for instance if the value is in usec and you only care about
   * 1/10th ms resolution; pass 100 as the scale
   *
   * If all your values are between 7000 and 8000 you may want to pass
   * 7000 as the offset and use 100 as the scale.
   *
   * @param scale divider for buckets, multiplier for printing data back
   * @param percentile to calculate the value for during print
   * 85% should be close to avg + stddev (1 sigma)
   * 99.9% should be close to 3 sigmas
   * Unless there is a lot of data with a dense/stable long tail, expect the
   * 99.9% to be noisy/close to "max"
   * @param offset substracted select a bucket, added back for printing
   */
  explicit Histogram(int32_t scale = 1, double percentile1 = 85.0,
                     double percentile2 = 99.9, int64_t offset = 0);
  ~Histogram() override;

  /**
   * record one value
   */
  void record(int64_t value);

  /**
   * Dumps histogram data to the output stream.
   *
   * ex:
   * Histogram numbers from 1 to 10
   *
   * Output:
   * # count,avg,min,max,stddev,10,5.5,1,10,2.88097
   * # range, mid point, percentile, count
   * < 1 , 0 , 0, 0
   * >= 1 < 2 , 1 , 10, 1
   * >= 2 < 3 , 2 , 20, 1
   * >= 3 < 4 , 3 , 30, 1
   * >= 4 < 5 , 4 , 40, 1
   * >= 5 < 6 , 5 , 50, 1
   * >= 6 < 7 , 6 , 60, 1
   * >= 7 < 8 , 7 , 70, 1
   * >= 8 < 9 , 8 , 80, 1
   * >= 9 < 10 , 9 , 90, 1
   * >= 10 < 11 , 10 , 100, 1
   * # target 85.0%,9.5
   * # target 99.9%,10.0
   */
  void print(std::ostream& os) const;

  /**
   * clears all the value
   */
  void reset();

  /**
   * Merges the passed in histogram data into this histogram
   * as if all the addToHist() calls had been made on this object
   */
  void merge(const Histogram& h);

  /**
   * Change the percentile targets for print()
   */
  void setPercentile1(double p);
  void setPercentile2(double p);

  /**
   * Calculate the value that reaches target percentile.
   * It uses a linear fit for the range of the bucket which is greater or equal
   * to the target. Unless it would return a value > getMax() in which case it
   * will use the maximum as the boundary
   */
  double calcPercentile(double p) const;

  // We need the number of buckets to know in advance the
  // size of the Histogram objects (without extra new/malloc; they can fit
  // on the stack...)

  /**
   *  Semi log bucket definitions covering 5 order of magnitude (more
   *  could be added) with high resolution in small numbers and relatively
   *  small number of total buckets
   *  For efficiency a look up table is created so the last value shouldn't
   *  be too large (or will incur large memory overhead)
   *  value between   [ bucket(i-1), bucket(i) [ go in slot i
   *  plus every value > bucket(last) in last bucket and every
   *  value < bucket(0)
   */
  static constexpr int32_t kHistogramBuckets[] = {
      1,     2,     3,     4,     5,     6,
      7,     8,     9,     10,    11,          // by 1 - my amp goes to 11 !
      12,    14,    16,    18,    20,          // by 2
      25,    30,    35,    40,    45,    50,   // by 5
      60,    70,    80,    90,    100,         // by 10
      120,   140,   160,   180,   200,         // line2 *10
      250,   300,   350,   400,   450,   500,  // line3 *10
      600,   700,   800,   900,   1000,        // line4 *10
      2000,  3000,  4000,  5000,  7500,  10000,
      20000, 30000, 40000, 50000, 75000, 100000};

  /** constant with the index of the last data bucket (1 more bucket than
   * the histograms as we need a bucket for > than last entry in the list)
   */
  static const size_t kLastIndex =
      sizeof(kHistogramBuckets) / sizeof(kHistogramBuckets[0]);

 private:
  /** buckets counters */
  int32_t hdata_[kLastIndex + 1];  // n+1 buckets (for last one)
  /** value divider for the buckets */
  const int32_t divider_;
  /** value offset for the buckets */
  const int64_t offset_;
  /** target percentile to printout by default */
  double percentile1_, percentile2_;
};

/**
 * To use lock-free, thread-local counters, defined below, the following
 * examples should get you started. For those interested in the details, you
 * may continue reading after this comment block.
 *
 * The following is an example of using ThreadLocalCounter/Histogram where you
 * manually read all the counts and do something with them.
 *
 * <code>
 * ThreadLocalCounter counter;
 *
 * // From any thread:
 * counter.record(100);
 *
 * // In a single-threaded manner (i.e. from a single-thread), periodically:
 * ThreadLocalCounter merged;
 * counter.readAndReset(merged);
 * LOG(INFO) << "average: " << merged.getAverage();
 * </code>
 *
 * If you want to periodically read thread-local counters and do something with
 * the counters, such as printing them, then you can use an instance of
 * PeriodicCounters that provides this common functionality. In the following
 * example we create two thread-local counters and periodically print them.
 *
 * <code>
 * // Create the counter & histogram.
 * ThreadLocalCounter counter(1.0, [](const Counter& c) {
 *   // Function will be called periodically with fully merged counter, c.
 *   c.print(std::cout);
 * });
 * ThreadLocalHistogram histogram(1.0, [](const Histogram& h) {
 *   h.print(std::cout);
 * });
 *
 * // Create a PeriodicCounters to periodically run callbacks every second.
 * PeriodicCounters periodic({&counter, &histogram});
 * periodic.schedule(1.0);
 * </code>
 */

/**
 * A SwapableNode is a base class to allow for polymorphism for the
 * PeriodicCounters (and ThreadLocalHistogram) classes. A SwapableNode
 * represents a wraper around a Counter-style class that can record data
 * and be merged. The idea is to double-buffer the Counter so that one can
 * be actively written into and the other can be read from.
 *
 * Clients should be using a sub-classes, such as ThreadLocalSwapableNode.
 */
class SwapableNode : private boost::noncopyable {
 protected:
  SwapableNode() : ptr_(0) {
  }
  virtual ~SwapableNode() {
  }

  /// Swap active object with inactive by flip-flopping from 0 to 1.
  inline int swap() {
    ptr_ ^= 1;
    return ptr_;
  }

  /// @return Index of current active object (0 or 1).
  inline int getActiveIndex() {
    return ptr_;
  }

  /**
   * Abstract function that interacts with PeriodicCounters and is called
   * periodically after the node has been swapped.
   */
  virtual void process() = 0;

 private:
  int ptr_;  //!< 0 or 1; index of current active object.
  friend class PeriodicCounters;
};

/**
 * Holds 2 objects that can merge() into a result object upon destruction.
 * This object is basic and meant to be used with thread-local-storage.
 * Objects of type C are assumed to have a constructor that takes one argument
 * of type P.
 */
template <class C, class P>
class Hold2 : boost::noncopyable {
 public:
  Hold2(C* result, const P& p) : datav_{C(p), C(p)}, result_(result) {
    VLOG(100) << "new Hold2 " << this;
    CHECK(result);
  }

  ~Hold2() {
    VLOG(100) << "~Hold2 " << this;
    result_->merge(datav_[0]);
    result_->merge(datav_[1]);
  }

  inline C& get(int idx) {
    return datav_[idx];
  }

 private:
  C datav_[2];  //!< Holds 2 objects.
  C* result_;   //!< Result object to merge into upon destruction.
};

/**
 * ThreadLocalSwapableNode is a template class that implements a SwapableNode
 * counter using thread-local storage and no locking. Each thread will get its
 * own double-buffered Counter. Threads write into the active Counter. When the
 * needs to be read, we swap the counters and merge all the data together into
 * a new Counter that can be safely read. The class C must be a Counter-style
 * class that implements reset(), merge(), and record(), while class P is the
 * type of the first argument to the single-argument constructor to class C.
 */
template <class C, class P>
class ThreadLocalSwapableNode : public SwapableNode {
 public:
  explicit ThreadLocalSwapableNode(const P& param,
                                   std::function<void(const C&)> func = nullptr)
      : param_(param), func_(func) {
    // Note: Logging crashes for static historgrams (which should be avoided)
    VLOG(100) << "new ThreadLocalSwapableNode " << this;
  }

  ~ThreadLocalSwapableNode() override {
    VLOG(100) << "~ThreadLocalSwapableNode " << this;
  }

  /// Call the underlying C::record() on the active object.
  inline void record(int64_t value) {
    getActiveCounter().record(value);
  }

  /// @return Reference to the current active C object.
  inline C& getActiveCounter() {
    // Works without locking because data_ is thread-local.
    auto* h = data_.get();
    if (h == nullptr) {
      // Using UNLIKELY() here isn't faster.
      // ThreadLocalPtr uses a lock to maintain the list of Hold2s.
      h = new Hold2<C, P>(&aggregated_, param_);
      data_.reset(h);
    }
    return h->get(getActiveIndex());
  }

  /**
   * Read all the counters and reset them. The values of the counters up to
   * this point will be dumped into the merged output argument.
   *
   * @param merged  Object that will return merged counters.
   * @param delay   Internally delay slightly when reading values; this is used
   *                to avoid locking.
   */
  inline void readAndReset(C& merged, bool delay = false) {
    swap();
    if (delay) {
      usleep(10);
    }
    mergeAndReset(!getActiveIndex(), merged);
  }

 protected:
  /**
   * Merge all counters from all thread-local storage objects and reset all
   * the counters.
   *
   * @param idx     Index of object to merge (active or in-active).
   * @param merged  Object that will contain fully merged data.
   */
  inline void mergeAndReset(int idx, C& merged) {
    for (Hold2<C, P>& p : data_.accessAllThreads()) {
      auto& h = p.get(idx);
      aggregated_.merge(h);
      h.reset();
    }
    merged.merge(aggregated_);
    aggregated_.reset();
  }

  /// @see PeriodicCounters for details on how this is used.
  void process() override {
    // If we have a callback function, give it the fully aggregated results.
    if (func_ != nullptr) {
      C merged;
      mergeAndReset(!getActiveIndex(), merged);
      func_(merged);
    }
  }

 private:
  P param_;       //!< Value for constructor for class C.
  C aggregated_;  //!< Object to hold aggregated results.
  std::function<void(const C&)> func_;

  /// Thread-local-storage for two swapable objects.
  folly::ThreadLocalPtr<Hold2<C, P>, SwapableNode> data_;
};

// ThreadLocal versions of Histogram and Counter.
typedef ThreadLocalSwapableNode<Histogram, int32_t> ThreadLocalHistogram;
typedef ThreadLocalSwapableNode<Counter, double> ThreadLocalCounter;

/**
 * PeriodicCounters, given a list of SwapableNode counters, will run a
 * periodic job that will read and reset all the counters. The action taken
 * after reading the contents of the counter is simply calling the optional
 * func() argument to SwapableNode. This class can be used, for example, to
 * periodically collect and print counters.
 */
class PeriodicCounters : private boost::noncopyable {
 public:
  explicit PeriodicCounters(const std::vector<SwapableNode*>& counters)
      : counters_(counters) {
  }
  virtual ~PeriodicCounters() {
    std::unique_lock<std::mutex> lk(mutex_);
    stop_ = true;
    lk.unlock();
    cv_.notify_one();
    if (thread_.joinable()) {
      thread_.join();
    }
    swapAndRead(false);
  }

  void swapAndRead(bool delay = true) {
    for (SwapableNode* n : counters_) {
      n->swap();
    }

    // Wait a tiny bit for threads using old value to be done.
    //
    // TODO: This is a hack, but avoid locks which kill performance. Look into
    //       sacrificing some performance for 100% accurate results using some
    //       sort of ultra-fast micro lock that pajoux wrote.
    if (delay) {
      usleep(100);
    }

    for (SwapableNode* n : counters_) {
      n->process();
    }
  }

  bool schedule(double intervalInSec) {
    intervalInMs_ = (1000.0 * intervalInSec + 0.5);
    thread_ = std::thread(&PeriodicCounters::run, this);
    return thread_.joinable();
  }

 private:
  void run() {
    std::unique_lock<std::mutex> lk(mutex_);
    while (!stop_) {
      if (cv_.wait_for(lk, std::chrono::milliseconds(intervalInMs_)) ==
          std::cv_status::timeout) {
        swapAndRead(true);
      }
    }
    LOG(INFO) << "Periodic stats thread ending";
  }

  std::vector<SwapableNode*> counters_;
  int64_t intervalInMs_;
  bool stop_{false};
  std::mutex mutex_;
  std::condition_variable cv_;
  std::thread thread_;
};
}
} /* namespace facebook::wormhole */

#endif /* STATS_H_ */
