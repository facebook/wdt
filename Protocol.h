#pragma once

#include <stddef.h>
#include <string>

namespace facebook { namespace wdt {

class Protocol {
 public:
  /// encodes id and size into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encode(char *dest, size_t &off, size_t max,
                     std::string id, int64_t size);
  /// decodes from src+off and consumes/moves off but not past max
  /// sets id and size
  /// @return false if there isn't enough data in src+off to src+max
  static bool decode(char *src, size_t &off, size_t max,
                     std::string &id, int64_t &size);
};
}} // namespace facebook::wdt
