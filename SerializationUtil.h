#pragma once

#include <folly/Range.h>
#include <string>

namespace facebook {
namespace wdt {
/// encodes value into dest + off
/// moves the off into the dest pointer
void encodeInt(char *dest, size_t &off, int64_t value);

/// decodes from br and consumes/moves off
/// returns value
int64_t decodeInt(folly::ByteRange &br);

/// encodes str into dest + off
/// moves the off into the dest pointer
void encodeString(char *dest, size_t &off, const std::string &str);

/// decodes from br and consumes/moves off
/// sets str
/// @return false if there isn't enough data in br
bool decodeString(folly::ByteRange &br, char *src, size_t max,
                  std::string &str);

/// checks to see if encoding as overflowed or not
/// @param off  final offset
/// @param max  max buffer size
/// @return     true if no overflow, false otherwise
bool checkForOverflow(size_t off, size_t max);
}
}
