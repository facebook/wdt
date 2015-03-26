#pragma once

#include <stddef.h>
#include <string>
#include <vector>
#include <limits.h>

namespace facebook {
namespace wdt {

typedef std::pair<int16_t, int64_t> Checkpoint;

class Protocol {
 public:
  /// Both version, magic number and command byte
  enum CMD_MAGIC {
    DONE_CMD = 0x44,      // D)one
    FILE_CMD = 0x4C,      // L)oad
    WAIT_CMD = 0x57,      // W)ait
    ERR_CMD = 0x45,       // E)rr
    SETTINGS_CMD = 0x53,  // S)ettings
    EXIT_CMD = 0x65,      // e)xit
  };

  /// Max size of filename + 4 max varints + 1 byte for cmd + 1 byte for status
  static const size_t kMaxHeader = PATH_MAX + 4 * 10 + 2 + 2;
  /// min number of bytes that must be send to unblock receiver
  static const size_t kMinBufLength = 256;
  /// max size of local checkpoint encoding
  static const size_t kMaxLocalCheckpoint = 10 + 2 * 10;
  /// max size of done command encoding
  static const size_t kMaxDone = 2 + 10;
  /// max size of settings command encoding
  static const size_t kMaxSettings = 1 + 2 * 10;

  /// encodes id and size into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeHeader(char *dest, size_t &off, size_t max, std::string id,
                           int64_t size, int64_t offset, int64_t fileSize);
  /// decodes from src+off and consumes/moves off but not past max
  /// sets id and size
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeHeader(char *src, size_t &off, size_t max, std::string &id,
                           int64_t &size, int64_t &offset, int64_t &fileSize);

  /// encodes checkpoints into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeCheckpoints(char *dest, size_t &off, size_t max,
                                const std::vector<Checkpoint> &checkpoints);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets checkpoints
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeCheckpoints(char *src, size_t &off, size_t max,
                                std::vector<Checkpoint> &checkpoints);

  /// encodes numBlocks into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeDone(char *dest, size_t &off, size_t max,
                         int64_t numBlocks);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets numBlocks
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeDone(char *src, size_t &off, size_t max,
                         int64_t &numBlocks);

  /// encodes read and write timeout into dest+off
  /// moves the off into dest pointer, not going past max
  /// @return false if there isn't enough room to encode
  static bool encodeSettings(char *dest, size_t &off, size_t max,
                             int64_t readTimeoutMillis,
                             int64_t writeTimeoutMillis);

  /// decodes from src+off and consumes/moves off but not past max
  /// sets readTimeoutMillis and writeTimeoutMillis
  /// @return false if there isn't enough data in src+off to src+max
  static bool decodeSettings(char *src, size_t &off, size_t max,
                             int64_t &readTimeoutMillis,
                             int64_t &writeTimeoutMillis);
};
}
}  // namespace facebook::wdt
