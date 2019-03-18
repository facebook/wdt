/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/ErrorCodes.h>
#include <folly/Conv.h>
#include <string.h>

DEFINE_int32(wdt_double_precision, 2, "Precision while printing double");
DEFINE_bool(wdt_logging_enabled, true, "To enable/disable WDT logging.");

namespace facebook {
namespace wdt {
std::string errorCodeToStr(ErrorCode code) {
  int numErrorCodes = sizeof(kErrorToStr) / sizeof(kErrorToStr[0]);
  if (code < numErrorCodes) {
    return kErrorToStr[code];
  }
  return folly::to<std::string>(code);
}

ErrorCode getMoreInterestingError(ErrorCode err1, ErrorCode err2) {
  return std::max(err1, err2);
}

std::string strerrorStr(int errnum) {
  std::string result;
  char buf[1024], *res = buf;
  buf[0] = 0;
#if defined(__APPLE__) || \
    ((_POSIX_C_SOURCE >= 200112L || _XOPEN_SOURCE >= 600) && !_GNU_SOURCE)
  strerror_r(errnum, buf, sizeof(buf));
#else
  res = strerror_r(errnum, buf, sizeof(buf));
#endif
  result.assign(res);
  return result;
}
}
}
