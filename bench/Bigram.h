/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/*
 * Bigram.h
 *
 *  Created on: Jun 24, 2015
 *      Author: ldemailly
 */

#pragma once

#include <cassert>
#include <iostream>
#include <unordered_map>

/**
 * A structure holding 2 bytes/characters
 */
class Bigram {
 public:
  Bigram(char c1, char c2);
  // Allow initialization from string constants
  // Only allow "xy" as input in that case if we used directly
  // std::array of 2 it complains of mismatch (size 3 because of the NUL)
  // e.g.  Bigram b{"XY"}; but not "X" or "XYZ" at compile time
  /* implicit */ Bigram(const char (&in)[3]);
  Bigram();
  //  ~Bigram();

  // Inlining [] gains about 5% or 20 Mbytes/sec (385->405)
  inline char operator[](int idx) const {
    assert(idx >= 0 && idx < 2);
    return b_[idx];
  }

  bool operator==(const Bigram &o) const;
  bool operator!=(const Bigram &o) const {
    return !(operator==(o));
  }
  bool operator<(const Bigram &o) const;
  void toPrintableString(std::string &result) const;
  void toBinary(std::string &result) const;
  void binarySerialize(std::ostream &os) const;
  /// Reads from the stream the 2 bytes for this bigram - returns true if
  /// no error/eof triggered when reading
  bool binaryDeserialize(std::istream &is);
  std::string toPrintableString() const;
  /// Utility only to be used for logging to escape 1 character
  static std::string toPrintableString(char c);

 private:
  char b_[2];
};

/// Dumps a bigram in human readable format
std::ostream &operator<<(std::ostream &os, const Bigram &b);

namespace std {
/// Hash function for Bigrams
template <>
struct hash<Bigram> {
  std::size_t operator()(const Bigram &k) const {
    return ((k[0] << 1) | k[1]);
  }
};
}
/// Handy types
typedef std::unordered_map<Bigram, uint32_t> MapOfBigramToCount;
typedef std::pair<Bigram, uint32_t> PairBigramCount;
