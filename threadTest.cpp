#include <thread>

#include "folly/Conv.h"

#include <gflags/gflags.h>
#include <glog/logging.h>


using std::string;

DEFINE_int32(port, 10000, "Port number");
DEFINE_int32(num_threads, 4, "Number of threads");
DEFINE_string(dir, "abcdef", "Directory");

void doOne(int port, string dir) {
  LOG(INFO) << "Running in X " << port << " dir " << dir;
}


int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  LOG(INFO) << "Port " << folly::to<string>(FLAGS_port);
  std::thread v[FLAGS_num_threads];
  for (int i=0; i < FLAGS_num_threads; i++) {
    v[i] = std::thread(doOne, FLAGS_port + i, FLAGS_dir);
  }
  for (int i=0; i < FLAGS_num_threads; i++) {
    v[i].join();
  }
  return 0;
}
