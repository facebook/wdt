/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/*
* Stats.cpp
*
*  Created on: Oct 18, 2011
*      Author: ldemailly
*/

#include "Stats.h"

#include <math.h>

#include <fstream>
#include <iomanip>
#include <sstream>

namespace facebook {
namespace wdt {

/*static*/
const size_t Histogram::kLastIndex;  // storage placeholder- value is from .h
constexpr int32_t Histogram::kHistogramBuckets[];  // buckets, values are in .h

static const int32_t kFirstValue = Histogram::kHistogramBuckets[0];
static const int32_t kLastValue =
    Histogram::kHistogramBuckets[Histogram::kLastIndex - 1];

int32_t* initHistLookup() {
  int32_t* res = (int32_t*)calloc(sizeof(int32_t), kLastValue);
  CHECK(res != nullptr);
  int32_t idx = 0;
  for (int i = 0; i < kLastValue; ++i) {
    if (i >= Histogram::kHistogramBuckets[idx]) {
      ++idx;
    }
    res[i] = idx;
  }
  CHECK_EQ(idx, Histogram::kLastIndex - 1);
  return res;
}

static const int32_t* kVal2Bucket = initHistLookup();

void Counter::record(int64_t v) {
  int64_t newCount = ++count_;
  sum_ += v;
  if (newCount == 1) {
    realMin_ = min_ = max_ = v;
  }
  // don't count 0s in the min - makes for a slightly more interesting min
  // in most cases
  if (v && ((min_ == 0) || (v < min_))) {
    min_ = v;
  }
  // real min (can be 0):
  if (v < realMin_ ) {
    realMin_ = v;
  }
  if (v > max_) {
    max_ = v;
  }
  int64_t v2 = 0;
  if (v > INT32_MAX || v < INT32_MIN) {
    LOG(WARNING) << "v too big for stddev " << v;
  } else {
    v2 = v * v;
  }
  sumOfSquares_ += v2;  // atomic_add(sumOfSquares_, v2);
}

void Counter::merge(const Counter& c) {
  // merge count_, min_, max_, sum_, sumOfSquares_;
  count_ += c.count_;
  if (c.min_ && c.min_ < min_) {
    min_ = c.min_;
  }
  if (c.max_ > max_) {
    max_ = c.max_;
  }
  sum_ += c.sum_;
  sumOfSquares_ += c.sumOfSquares_;
}

double Counter::getStdDev() const {
  if (count_ < 1) {
    return 0.0;
  }
  if (sum_ > INT32_MAX || sum_ < INT32_MIN ||
      (sumOfSquares_ - sum_ * sum_ / count_) < 0) {
    LOG(WARNING) << "std dev wrap " << count_ << " " << sum_ << " "
                 << sumOfSquares_;
    return 0.0;
  }
  double sigma = ((double)sumOfSquares_ - sum_ * sum_ / count_) / count_;
  return sqrt(sigma);
}

void Counter::print(std::ostream& os, double multiplier) const {
  if (multiplier == 0.) {
    multiplier = printScale_;
  }
  int64_t intMult = (int64_t)multiplier;
  os << getCount() << "," << multiplier * getAverage() << ",";
  if (intMult == multiplier) {
    // still int based
    os << intMult * getMin() << "," << intMult * getMax() << ","
       << multiplier * getStdDev();
  } else {
    os << multiplier * getMin() << "," << multiplier * getMax() << ","
       << multiplier * getStdDev();
  }
}
void Counter::printCounterHeader(std::ostream& os) const {
  // should match the above
  os << "count,avg,min,max,stddev";
}

void Histogram::record(int64_t value) {
  Counter::record(value);
  int64_t scaledVal = (value - offset_);
  // If you touch those next lines, rerun the benchmark mentioned in Stats.h
  if (/*((divider_>>31)==0) &&*/ ((scaledVal >> 31) == 0)) {
    scaledVal = (int32_t)scaledVal / (int32_t)divider_;
  } else {
    // 64 bits is 2x more expensive so do it only if needed
    scaledVal /= divider_;
  }
  int32_t idx = 0;
  if (scaledVal >= kLastValue) {
    idx = kLastIndex;
  } else if (scaledVal >= kFirstValue) {
    idx = kVal2Bucket[scaledVal];
  }  // else it's <  and idx 0
  VLOG(3) << "addHist v " << value << " d " << divider_ << " o " << offset_
          << " s " << scaledVal << " -> " << idx << " (lastval " << kLastValue
          << ")" << std::endl;
  ++hdata_[idx];  // atomic_add(data[idx], 1);
}

void Histogram::merge(const Histogram& h) {
  Counter::merge(h);
  for (size_t i = 0; i <= kLastIndex; ++i) {
    hdata_[i] += h.hdata_[i];
  }
}

Histogram::Histogram(int32_t scale, double p1, double p2, int64_t offset)
    : Counter(1),
      divider_(scale),
      offset_(offset),
      percentile1_(p1),
      percentile2_(p2) {
  VLOG(100) << "New Histogram " << this;
  Counter();
  memset((void*)hdata_, 0, sizeof(hdata_));
}

Histogram::~Histogram() {
  VLOG(100) << "~Histogram " << this;
}

void Histogram::setPercentile1(double p) {
  percentile1_ = p;
}
void Histogram::setPercentile2(double p) {
  percentile2_ = p;
}

double Histogram::calcPercentile(double percentile) const {
  if (percentile >= 100) {
    return getMax();
  }
  if (percentile <= 0) {
    return getRealMin();
  }
  // Initial value of prev should in theory be offset_
  // but if the data is wrong (smaller than offset - eg 'negative') that
  // yields to strangeness (see one bucket test)
  int64_t prev = 0;
  int64_t total = 0;
  const int64_t ctrTotal = getCount();
  const int64_t ctrMax = getMax();
  const int64_t ctrMin = getRealMin();
  double prevPerc = 0;
  double perc = 0;
  bool found = false;
  int64_t cur = offset_;
  // last bucket is virtual/special - we'll use max if we reach it
  // we also use max if the bucket is past the max for better accuracy
  // and the property that target = 100 will always return max
  // (+/- rouding issues) and value close to 100 (99.9...) will be close to max
  // if the data is not sampled in several buckets
  for (size_t i = 0; i < kLastIndex; ++i) {
    cur = (int64_t)Histogram::kHistogramBuckets[i] * divider_ + offset_;
    total += hdata_[i];
    perc = 100. * (double)total / ctrTotal;
    if (cur > ctrMax) {
      break;
    }
    if (perc >= percentile) {
      found = true;
      break;
    }
    prevPerc = perc;
    prev = cur;
  }
  if (!found) {
    // covers the > ctrMax case
    cur = ctrMax;
    perc = 100.;  // can't be removed
  }
  // Fix up the min too to never return < min and increase low p accuracy
  if (prev < ctrMin) {
    prev = ctrMin;
  }
  return (prev + (percentile - prevPerc) * (cur - prev) / (perc - prevPerc));
}

// histogram print
void Histogram::print(std::ostream& os) const {
  const int64_t multiplier = divider_;

  // calculate the last bucket index
  int32_t lastIdx = -1;
  for (int i = kLastIndex; i >= 0; --i) {
    if (hdata_[i] > 0) {
      lastIdx = i;
      break;
    }
  }
  if (lastIdx == -1) {
    os << "no data" << std::endl;
    return;
  }

  // comment out header in gnuplot data file
  os << "# ";
  Counter::printCounterHeader(os);
  os << ",";           // awk -F, '/^count/ {sum+=$6} END {print "sum is", sum}'
  Counter::print(os);  // the base counter part
  os << std::endl << "# range, mid point, percentile, count" << std::endl;
  int64_t prev = 0;
  int64_t total = 0;
  const int64_t ctrTotal = getCount();
  auto oldPrecision = os.precision(5);
  // we can combine this loop and the calcPercentile() one but it's
  // easier to read/maintain/test when separated and it's only 2 pass on
  // very little data

  // output the data of each bucket of the histogram
  for (size_t i = 0; i <= static_cast<size_t>(lastIdx); ++i) {
    total += hdata_[i];

    // data in each row is separated by comma (",")
    if (i > 0) {
      os << ">= " << multiplier * prev + offset_ << " ";
    }
    double perc = 100. * (double)total / ctrTotal;
    if (i < kLastIndex) {
      int64_t cur = Histogram::kHistogramBuckets[i];
      os << "< " << multiplier * cur + offset_ << " ";
      double midpt = multiplier * (prev + cur) / 2 + offset_;
      os << ", " << midpt << " ";
      prev = cur;
    } else {
      os << ", " << multiplier * prev + offset_ << " ";
    }
    os << ", " << perc << ", " << hdata_[i] << std::endl;
  }

  // print the information of target percentiles
  os.precision(1);
  os << std::fixed << "# target " << percentile1_ << "%,"
     << calcPercentile(percentile1_) << std::endl
     << "# target " << percentile2_ << "%," << calcPercentile(percentile2_)
     << std::endl;
  os.unsetf(std::ios_base::floatfield);
  os.precision(oldPrecision);
}

void Histogram::reset() {
  Counter::reset();
  memset((void*)hdata_, 0, sizeof(hdata_));
}
}
} /* namespace facebook::wormhole */
