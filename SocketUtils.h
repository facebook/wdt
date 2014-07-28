#pragma once

namespace facebook {
namespace wdt {

class SocketUtils {
 public:
  static int getReceiveBufferSize(int fd);
};
}
} // namespace facebook::wdt
