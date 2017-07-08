/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/*
 * Stats_test.cpp
 *
 *  Created on: Oct 18, 2011
 *      Author: ldemailly
 */

#include <cmath>
#include <iostream>
#include <sstream>

#include <gtest/gtest.h>

#include <wdt/util/Stats.h>

using std::ostringstream;

namespace facebook {
namespace wdt {

TEST(StatsTest, CounterNoData) {
  Counter c;
  EXPECT_EQ(0, c.getCount());
  EXPECT_TRUE(std::isnan(c.getAverage()));
  ostringstream oss;
  c.print(oss);
  std::string res = oss.str();
  EXPECT_EQ(res, "0,nan,0,0,0");
}

TEST(StatsTest, CounterSimpleTest) {
  Counter c;
  c.record(-7);
  EXPECT_EQ(1, c.getCount());
  EXPECT_EQ(-7, c.getMin());
  EXPECT_EQ(-7, c.getMax());
  EXPECT_EQ(-7.0, c.getAverage());
  c.record(-9);
  EXPECT_EQ(2, c.getCount());
  EXPECT_EQ(-9, c.getMin());
  EXPECT_EQ(-7, c.getMax());
  EXPECT_EQ(-8.0, c.getAverage());
  ostringstream oss;
  c.print(oss);
  EXPECT_EQ(oss.str(), "2,-8,-9,-7,1");
}

TEST(StatsTest, CounterZeroSpecialMinAndPrintScaling) {
  Counter c(10);
  c.record(0);
  c.record(0);
  EXPECT_EQ(2, c.getCount());
  EXPECT_EQ(0, c.getMin());
  EXPECT_EQ(0, c.getMax());
  EXPECT_EQ(0.0, c.getAverage());
  c.record(30);
  EXPECT_EQ(3, c.getCount());
  // we treat 0 specially - it doesn't count as interesting value for min:
  EXPECT_EQ(30, c.getMin());
  EXPECT_EQ(30, c.getMax());
  EXPECT_EQ(10.0, c.getAverage());
  c.record(-10);
  EXPECT_EQ(-10, c.getMin());
  EXPECT_EQ(30, c.getMax());
  EXPECT_EQ(5.0, c.getAverage());
  ostringstream oss;
  c.print(oss);  // 10 is default passed above
  EXPECT_EQ(oss.str(), "4,50,-100,300,150");
  oss.str("");
  c.print(oss, 1);
  EXPECT_EQ(oss.str(), "4,5,-10,30,15");
  oss.str("");
  c.print(oss, .1);
  EXPECT_EQ(oss.str(), "4,0.5,-1,3,1.5");
}

TEST(StatsTest, HistogramPercentile) {
  Histogram h(10, 75);
  h.record(1);
  h.record(251);
  h.record(501);
  h.record(751);
  h.record(1001);
  h.print(std::cout);
  for (int i = 25; i <= 100; i += 25) {
    std::cout << i << "% at " << h.calcPercentile(i) << std::endl;
  }
  EXPECT_EQ(501, h.getAverage());
  EXPECT_EQ(1, h.calcPercentile(-1));
  EXPECT_EQ(1, h.calcPercentile(0));
  EXPECT_EQ(1.045, h.calcPercentile(0.1));
  EXPECT_EQ(1.45, h.calcPercentile(1));
  EXPECT_EQ(2.8, h.calcPercentile(4));
  EXPECT_EQ(10, h.calcPercentile(20));
  EXPECT_EQ(250.25, h.calcPercentile(20.1));
  EXPECT_EQ(550, h.calcPercentile(50));
  EXPECT_EQ(775, h.calcPercentile(75));
  EXPECT_EQ(1000.5, h.calcPercentile(90));
  EXPECT_EQ(1000.95, h.calcPercentile(99));
  EXPECT_EQ(1000.995, h.calcPercentile(99.9));
  EXPECT_EQ(1001, h.calcPercentile(100));
  EXPECT_EQ(1001, h.calcPercentile(101));
}

TEST(StatsTest, HistogramPercentileWithOffset) {
  // +10 000 000 000 on data from above
  int64_t tenB = 10000000000;
  /*
    if (sizeof(stats_type) < 8) {
      tenB /= 10; // 1B when int32_t
    }
  */
  Histogram h(10, 75, 95, tenB);
  h.record(tenB + 1);
  h.record(tenB + 251);
  h.record(tenB + 501);
  h.record(tenB + 751);
  h.record(tenB + 1001);
  h.print(std::cout);
  for (int i = 25; i <= 100; i += 25) {
    std::cout << i << "% at " << h.calcPercentile(i) << std::endl;
  }
  const double tenBD = tenB;
  EXPECT_EQ(501, h.getAverage() - tenBD);
  EXPECT_EQ(10, h.calcPercentile(20) - tenBD);
  EXPECT_EQ(550, h.calcPercentile(50) - tenBD);
  EXPECT_EQ(775, h.calcPercentile(75) - tenBD);
  EXPECT_EQ(1000.5, h.calcPercentile(90) - tenBD);
  EXPECT_NEAR(1000.95, h.calcPercentile(99) - tenBD, 0.00001);
  EXPECT_NEAR(1000.995, h.calcPercentile(99.9) - tenBD, 0.00001);
  EXPECT_EQ(1001, h.calcPercentile(100) - tenBD);
  EXPECT_EQ(1001, h.calcPercentile(101) - tenBD);
}

TEST(StatsTest, HistogramLastBucketPercentile) {
  Histogram h(1, 75);
  h.record(0);
  h.record(500001);  // over last 100k bucket
  h.print(std::cout);
  for (int i = 25; i <= 100; i += 25) {
    std::cout << i << "% at " << h.calcPercentile(i) << std::endl;
  }
  std::cout.precision(10);
  std::cout << h.calcPercentile(90) << std::endl;
  std::cout << h.calcPercentile(99) << std::endl;
  std::cout << h.calcPercentile(99.9) << std::endl;
  EXPECT_EQ(250000.5, h.getAverage());
  EXPECT_EQ(1, h.calcPercentile(50));
  EXPECT_EQ(300000.5, h.calcPercentile(75));
  EXPECT_EQ(420000.8, h.calcPercentile(90));
  EXPECT_EQ(492000.98, h.calcPercentile(99));
  EXPECT_EQ(499200.998, h.calcPercentile(99.9));
  EXPECT_EQ(500001, h.calcPercentile(100));
  EXPECT_EQ(500001, h.calcPercentile(101));
  EXPECT_EQ(0, h.calcPercentile(-1));
}

TEST(StatsTest, HistogramLastBucketPercentileWithOffset) {
  Histogram h(1, 75, 95, 10000);
  h.record(0);
  h.record(500001);  // over last 100k bucket
  h.print(std::cout);
  for (int i = 25; i <= 100; i += 25) {
    std::cout << i << "% at " << h.calcPercentile(i) << std::endl;
  }
  std::cout.precision(10);
  std::cout << h.calcPercentile(90) << std::endl;
  std::cout << h.calcPercentile(99) << std::endl;
  std::cout << h.calcPercentile(99.9) << std::endl;
  EXPECT_EQ(250000.5, h.getAverage());
  EXPECT_EQ(10001, h.calcPercentile(50));
  EXPECT_EQ(305000.5, h.calcPercentile(75));
  EXPECT_EQ(422000.8, h.calcPercentile(90));
  EXPECT_EQ(492200.98, h.calcPercentile(99));
  EXPECT_EQ(499220.998, h.calcPercentile(99.9));
  EXPECT_EQ(500001, h.calcPercentile(100));
  EXPECT_EQ(500001, h.calcPercentile(101));
  EXPECT_EQ(0, h.calcPercentile(-1));
}

// could argue that getting closer to 50 but coming from 100 is actually
// more correct but it seems odd to have a smaller results as %tile increases
TEST(StatsTest, HistogramPercentileOffsetOneBucket) {
  Histogram h(100, 75, 95, 100);
  h.record(50);  // all data before first bucket
  h.print(std::cout);
  for (int i = 0; i <= 110; i += 10) {
    double v = h.calcPercentile(i);
    std::cout << i << "% at " << v << std::endl;
    EXPECT_GE(50, v);
  }
}

TEST(StatsTest, HistogramPercentile64BitDiv) {
  Histogram h(10000000);  // divide by 10M
  h.record(2000000000);   // value is 2B
  h.record(3000000000);   // value is 3B
  h.record(4000000000);   // value is 4B
  h.print(std::cout);
  for (int i = 0; i <= 110; i += 10) {
    double v = h.calcPercentile(i);
    std::cout << i << "% at " << v << std::endl;
    EXPECT_LE(0, v);  // must be >= 0
  }
  EXPECT_LE(2500000000, h.calcPercentile(95));
  EXPECT_EQ(4000000000, h.calcPercentile(100));
}

}  // namespace wormhole
}  // namespace facebook

int main(int argc, char** argv) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
