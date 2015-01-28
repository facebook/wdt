#pragma once

#include <stddef.h>
#include <string>
#include <limits.h>

namespace facebook {
namespace wdt {

class Protocol {
 public:
  /// Both version, magic number and command byte
  enum CMD_MAGIC {
    DONE_CMD = 0x44,  // D)one
    FILE_CMD = 0x4C,  // L)oad
    EXIT_CMD = 0x65,  // e)xit
  };

  /// Max size of filename + 2 max varints + 1 byte for cmd + 1 byte for status
  static const size_t kMaxHeader = PATH_MAX + 10 + 10 + 2;

  /// encodes id and size into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encode(char *dest, size_t &off, size_t max, std::string id,
                     int64_t size);
  /// decodes from src+off and consumes/moves off but not past max
  /// sets id and size
  /// @return false if there isn't enough data in src+off to src+max
  static bool decode(char *src, size_t &off, size_t max, std::string &id,
                     int64_t &size);
};
}
}  // namespace facebook::wdt
