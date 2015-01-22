#include <stdlib.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <thread>

#include "WdtOptions.h"

namespace facebook {
namespace wdt {

const static int kSimulatorSleepDurationMSec = 250;

void simulateNetworkError();
static std::thread errorSimulatorThread(&simulateNetworkError);

void simulateNetworkError() {
  errorSimulatorThread.detach();

  while (true) {
    usleep(kSimulatorSleepDurationMSec * 1000);
    auto &options = facebook::wdt::WdtOptions::getMutable();
    options.retryIntervalMultFactor_ = 1.5;

    int fd = 3 + rand() % (2 * WdtOptions::get().numSockets_ + 1);
    // close the chosen socket
    if (shutdown(fd, SHUT_WR) < 0) {
      PLOG(WARNING) << "socket shutdown failed for fd " << fd;
    } else {
      LOG(INFO) << "successfully shut down socket for fd " << fd;
    }
  }
}
}
}
