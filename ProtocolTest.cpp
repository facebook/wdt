#include "Protocol.h"

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "folly/Random.h"

DEFINE_int32(random_seed, folly::randomNumberSeed(), "random seed");

namespace facebook {
namespace wdt {

using std::string;

void testProtocol() {
  string id("abcdef");
  int64_t size = 3;

  char buf[128];
  size_t off = 0;
  bool success = Protocol::encode(buf, off, sizeof(buf), id, size);
  EXPECT_TRUE(success);
  EXPECT_EQ(off, id.size() + 1 + 1);  // 1 byte varint for id len and size
  string nid;
  int64_t nsize;
  size_t noff = 0;
  success = Protocol::decode(buf, noff, sizeof(buf), nid, nsize);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nid, id);
  EXPECT_EQ(nsize, size);
  noff = 0;
  // exact size:
  success = Protocol::decode(buf, noff, off, nid, nsize);
  EXPECT_TRUE(success);

  LOG(INFO) << "error tests, expect errors";
  // too short
  noff = 0;
  success = Protocol::decode(buf, noff, off - 1, nid, nsize);
  EXPECT_FALSE(success);

  // Long size:
  size = (int64_t)100 * 1024 * 1024 * 1024;  // 100Gb
  id.assign("different");
  off = 0;
  success = Protocol::encode(buf, off, sizeof(buf), id, size);
  EXPECT_TRUE(success);
  EXPECT_EQ(off, id.size() + 1 + 6);  // 1 byte varint for id len and size
  noff = 0;
  success = Protocol::decode(buf, noff, sizeof(buf), nid, nsize);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nid, id);
  EXPECT_EQ(nsize, size);
  LOG(INFO) << "got size of " << nsize;
  // too short for size encoding:
  noff = 0;
  success = Protocol::decode(buf, noff, off - 2, nid, nsize);
  EXPECT_FALSE(success);
}

TEST(Protocol, Simple) {
  testProtocol();
}
}
}  // namespaces

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
