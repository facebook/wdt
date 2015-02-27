#include <stdlib.h>
#include <glog/logging.h>
#include <sys/socket.h>
#include <thread>
#include <folly/Random.h>
#include "WdtOptions.h"

namespace facebook {
namespace wdt {

const static int kSimulatorSleepDurationMillis = 250;
const static double kRetryMultFactor = 1.85;

void simulateNetworkError();
static std::thread errorSimulatorThread(&simulateNetworkError);

void simulateNetworkError() {
  errorSimulatorThread.detach();

  while (true) {
    usleep(kSimulatorSleepDurationMillis * 1000);
    auto &options = facebook::wdt::WdtOptions::getMutable();
    options.retry_interval_mult_factor = kRetryMultFactor;

    int fd = 3 + folly::Random::rand32(2 * WdtOptions::get().num_ports + 1);
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
