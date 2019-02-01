#include <wdt/Throttler.h>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <chrono>

namespace facebook {
namespace wdt {

void testThrottling(std::shared_ptr<Throttler> throttler, int expectedRate) {
  int64_t numTransferred = 0;
  auto startTime = Clock::now();
  while (durationSeconds(Clock::now() - startTime) <= 2) {
    throttler->limit(1000);
    numTransferred += 1000;
  }
  auto endTime = Clock::now();
  double durationSecs = durationSeconds(endTime - startTime);
  double throughputMBps = numTransferred / durationSecs / kMbToB;
  EXPECT_NEAR(throughputMBps, expectedRate, 1);
}

TEST(ThrottlerTest, RATE_CHANGE) {
  WdtOptions options;
  options.avg_mbytes_per_sec = 50;
  std::shared_ptr<Throttler> throttler =
      std::make_shared<Throttler>(options.getThrottlerOptions());
  throttler->startTransfer();

  testThrottling(throttler, 50);

  // test rate decrease
  options.avg_mbytes_per_sec = 30;
  throttler->setThrottlerRates(options.getThrottlerOptions());
  testThrottling(throttler, 30);

  // test rate increase
  options.avg_mbytes_per_sec = 70;
  throttler->setThrottlerRates(options.getThrottlerOptions());
  testThrottling(throttler, 70);

  throttler->endTransfer();
}

TEST(ThrottlerTest, FAIRNESS) {
  WdtOptions options;
  options.avg_mbytes_per_sec = 60;
  options.buffer_size = 256 * 1024;
  std::shared_ptr<Throttler> throttler =
      std::make_shared<Throttler>(options.getThrottlerOptions());

  const int numThread = 40;
  const int testDurationSec = 5;
  double throughputs[numThread];

  auto transfer = [&](int index, int buffer) {
    throttler->startTransfer();
    int64_t numTransferred = 0;
    auto startTime = Clock::now();
    while (durationSeconds(Clock::now() - startTime) <= testDurationSec) {
      throttler->limit(buffer);
      numTransferred += buffer;
    }
    throttler->endTransfer();
    throughputs[index] = numTransferred / testDurationSec / kMbToB;
  };

  auto getBuffer = [&](int index) -> int {
    return options.buffer_size * (index + 1);
  };

  std::vector<std::thread> threads;
  for (int i = 0; i < numThread; i++) {
    std::thread t(transfer, i, getBuffer(i));
    threads.emplace_back(std::move(t));
  }
  for (auto &t : threads) {
    t.join();
  }
  for (int i = 0; i < numThread; i++) {
    LOG(INFO) << "Buffer: " << getBuffer(i) / kMbToB
              << "MB, Throughput: " << throughputs[i];
  }
  const double minThroughput =
      *std::min_element(throughputs, throughputs + numThread);
  const double maxThroughput =
      *std::max_element(throughputs, throughputs + numThread);
  const double fairness = maxThroughput / minThroughput;
  LOG(INFO) << "Min throughput " << minThroughput << ", max throughput "
            << maxThroughput << ", fairness " << fairness;
  EXPECT_LT(fairness, 2.2);
}
}
}
