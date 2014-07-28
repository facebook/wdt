#pragma once

#include <stddef.h>
#include <string>
#include <limits.h>

namespace facebook {
namespace wdt {

/// Max size of filename + 2 max varints

class Protocol {
 public:
  /// Both version, magic number and command byte
  enum CMD_MAGIC {
    FILE_CMD = 0x4C, // Load
    DONE_CMD = 0x44, // Done
  };

  static const size_t kMaxHeader = PATH_MAX + 10 + 10;

  /// encodes id and size into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encode(
    char* dest, size_t& off, size_t max, std::string id, int64_t size);
  /// decodes from src+off and consumes/moves off but not past max
  /// sets id and size
  /// @return false if there isn't enough data in src+off to src+max
  static bool decode(
    char* src, size_t& off, size_t max, std::string& id, int64_t& size);
};
}
} // namespace facebook::wdt
