/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <glog/logging.h>
#include <sys/socket.h>
#include <wdt/WdtOptions.h>
#include <wdt/test/TestCommon.h>
#include <thread>

namespace facebook {
namespace wdt {

const static int kSimulatorSleepDurationMillis = 250;

void simulateNetworkError();
static std::thread errorSimulatorThread(&simulateNetworkError);

void simulateNetworkError() {
  errorSimulatorThread.detach();

  while (true) {
    usleep(kSimulatorSleepDurationMillis * 1000);
    auto &options = facebook::wdt::WdtOptions::getMutable();

    int fd = 3 + rand32() % (2 * options.num_ports + 1);
    // close the chosen socket
    if (shutdown(fd, SHUT_WR) < 0) {
      WPLOG(WARNING) << "socket shutdown failed for fd " << fd;
    } else {
      WLOG(INFO) << "successfully shut down socket for fd " << fd;
    }
  }
}
}
}
