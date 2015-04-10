#include "ErrorCodes.h"
namespace facebook {
namespace wdt {
std::string errorCodeToStr(ErrorCode code) {
  int numErrorCodes = sizeof(kErrorToStr) / sizeof(kErrorToStr[0]);
  if (code >= 0 && code < numErrorCodes) {
    return kErrorToStr[code];
  }
  return folly::to<std::string>(code);
}
}
}
