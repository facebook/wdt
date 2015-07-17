/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "ErrorCodes.h"
#include <folly/Conv.h>
#include <string.h>

namespace facebook {
namespace wdt {
std::string errorCodeToStr(ErrorCode code) {
  int numErrorCodes = sizeof(kErrorToStr) / sizeof(kErrorToStr[0]);
  if (code >= 0 && code < numErrorCodes) {
    return kErrorToStr[code];
  }
  return folly::to<std::string>(code);
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
