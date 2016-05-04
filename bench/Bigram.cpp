/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
/*
 * Bigram.cpp
 *
 *  Created on: Jun 24, 2015
 *      Author: ldemailly
 */

#include "Bigram.h"
#include <glog/logging.h>

/// Utility functions

/**
 * Append to result a human readable/printable value for C code
 * \\ for \, \" for ", c if c is printable, \0octal otherwise
 */
void toPrintableString(char c, std::string &result) {
  if (!std::isprint(c)) {
    char buf[10];
    snprintf(buf, sizeof(buf), "\\0%o", c);
    result.append(buf);
    return;
  }
  if (c == '\\') {
    result.append("\\\\");
    return;
  }
  if (c == '\"') {
    result.append("\\\"");
    return;
  }
  result.push_back(c);
}

/**
 * Same as toPrintableString but returns the string.
 * Inefficient, for use in LOG()
 */
std::string Bigram::toPrintableString(char c) {
  std::string result;
  ::toPrintableString(c, result);
  return result;
}

std::ostream &operator<<(std::ostream &os, const Bigram &b) {
  os << b.toPrintableString();
  return os;
}

/// Bigram definition

void Bigram::toPrintableString(std::string &result) const {
  result.append("{\"");
  // result.append("Bigram(\"");
  ::toPrintableString(b_[0], result);
  ::toPrintableString(b_[1], result);
  result.append("\"}");
  // result.append("\")");
}

void Bigram::toBinary(std::string &result) const {
  result.push_back(b_[0]);
  result.push_back(b_[1]);
}

void Bigram::binarySerialize(std::ostream &os) const {
  os << b_[0] << b_[1];
}

bool Bigram::binaryDeserialize(std::istream &is) {
  return !is.read(&b_[0], 2).fail();
}

std::string Bigram::toPrintableString() const {
  std::string res;
  toPrintableString(res);
  return res;
}

Bigram::Bigram() : b_{0, 0} {
  VLOG(3) << "Empty constructor " << this << " " << *this;
}

Bigram::Bigram(char c1, char c2) : b_{c1, c2} {
  VLOG(3) << "2 Char Constructor called " << this << " " << *this;
}

Bigram::Bigram(const char (&in)[3]) : b_{in[0], in[1]} {
  VLOG(3) << "Array/string constructor called " << this << " " << *this;
}

/*
Bigram::~Bigram() {
  VLOG(3) << "Destructor for " << this << " " << *this;
}
*/

bool Bigram::operator==(const Bigram &o) const {
  return (b_[0] == o.b_[0]) && (b_[1] == o.b_[1]);
}

bool Bigram::operator<(const Bigram &o) const {
  if (b_[0] < o.b_[0]) {
    return true;
  }
  if (b_[0] > o.b_[0]) {
    return false;
  }
  // first byte is == so use 2nd byte
  return b_[1] < o.b_[1];
}
