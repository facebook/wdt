/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/test/TestCommon.h>
#include <wdt/util/ThreadsController.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>
#include <mutex>
#include <thread>
#include <vector>
using namespace std;
namespace facebook {
namespace wdt {
class ThreadUtil {
 public:
  template <typename Fn>
  void addThread(Fn&& func) {
    threads_.emplace_back(func);
  }
  void joinThreads() {
    for (auto& t : threads_) {
      t.join();
    }
    threads_.clear();
  }

  void threadSleep(int64_t millis) {
    auto waitingTime = chrono::milliseconds(millis);
    WLOG(INFO) << "Sleeping for " << millis << " ms";
    unique_lock<mutex> lock(mutex_);
    cv_.wait_for(lock, waitingTime);
  }

  void notifyThreads() {
    cv_.notify_all();
  }

  ~ThreadUtil() {
    joinThreads();
  }

 private:
  vector<thread> threads_;
  mutex mutex_;
  condition_variable cv_;
};

TEST(ThreadsController, Barrier) {
  int numThreads = 15;
  {
    Barrier barrier(numThreads);
    ThreadUtil threadUtil;
    for (int i = 0; i < numThreads; i++) {
      threadUtil.addThread([&]() { barrier.execute(); });
    }
  }
  {
    Barrier barrier(numThreads);
    srand(time(nullptr));
    ThreadUtil threadUtil;
    for (int i = 0; i < numThreads; i++) {
      threadUtil.addThread([&barrier, &threadUtil, i]() {
        if (i % 2 == 0) {
          int seconds = rand32() % 5;
          threadUtil.threadSleep(seconds * 100);
          barrier.deRegister();
          return;
        }
        barrier.execute();
      });
    }
  }
  {
    Barrier barrier(numThreads);
    ThreadUtil threadUtil;
    for (int i = 0; i < numThreads; i++) {
      threadUtil.addThread([&barrier, &threadUtil, i]() {
        switch (i) {
          case 1:
            threadUtil.threadSleep(1 * 100);
            barrier.execute();
            return;
          case 2:
            threadUtil.threadSleep(2 * 100);
            barrier.deRegister();
            return;
          case 3:
            threadUtil.threadSleep(5 * 100);
            barrier.execute();
            return;
          default:
            barrier.execute();
            return;
        };
      });
    }
  }
}

TEST(ThreadsController, ExecutOnceFunc) {
  int numThreads = 8;
  ExecuteOnceFunc execAtStart(numThreads, true);
  ExecuteOnceFunc execAtEnd(numThreads, false);
  ThreadUtil threadUtil;
  int result = 0;
  for (int i = 0; i < numThreads; i++) {
    threadUtil.addThread([&]() {
      execAtStart.execute([&]() { ++result; });
      execAtEnd.execute([&]() { ++result; });
    });
  }
  threadUtil.joinThreads();
  EXPECT_EQ(result, 2);
}
}
}
