/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <folly/Range.h>
#include <wdt/ErrorCodes.h>
#include <string>

namespace facebook {
namespace wdt {
/// encodes value into dest + off
/// moves the off into the dest pointer
void encodeInt(char *dest, int64_t &off, int64_t value);

/// decodes from br and consumes/moves off
/// returns value
int64_t decodeInt(folly::ByteRange &br);

/// encodes str into dest + off
/// moves the off into the dest pointer
void encodeString(char *dest, int64_t &off, const std::string &str);

/// decodes from br and consumes/moves off
/// sets str
/// @return false if there isn't enough data in br
bool decodeString(folly::ByteRange &br, char *src, int64_t max,
                  std::string &str);

/// checks to see if encoding as overflowed or not
/// @param off  final offset
/// @param max  max buffer size
/// @return     true if overflow, false otherwise
bool checkForOverflow(int64_t off, int64_t max);
}
}
