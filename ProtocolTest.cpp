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
  uint64_t seqId = 3;
  int64_t size = 3;

  char buf[128];
  size_t off = 0;
  int64_t blockOffset = 4;
  int64_t fileSize = 10;
  bool success = Protocol::encodeHeader(buf, off, sizeof(buf), id, seqId, size,
                                        blockOffset, fileSize);
  EXPECT_TRUE(success);
  EXPECT_EQ(off,
            id.size() + 1 + 1 + 1 + 1 +
                1);  // 1 byte varint for seqId, len, size, offset and filesize
  string nid;
  uint64_t nseqId;
  int64_t nsize;
  size_t noff = 0;
  int64_t nblockOffset = 0;
  int64_t nfileSize = 0;
  success = Protocol::decodeHeader(buf, noff, sizeof(buf), nid, nseqId, nsize,
                                   nblockOffset, nfileSize);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nid, id);
  EXPECT_EQ(nseqId, seqId);
  EXPECT_EQ(nsize, size);
  EXPECT_EQ(nblockOffset, blockOffset);
  EXPECT_EQ(nfileSize, fileSize);
  noff = 0;
  // exact size:
  success = Protocol::decodeHeader(buf, noff, off, nid, nseqId, nsize,
                                   nblockOffset, nfileSize);
  EXPECT_TRUE(success);

  LOG(INFO) << "error tests, expect errors";
  // too short
  noff = 0;
  success = Protocol::decodeHeader(buf, noff, off - 1, nid, nseqId, nsize,
                                   nblockOffset, nfileSize);
  EXPECT_FALSE(success);

  // Long size:
  size = (int64_t)100 * 1024 * 1024 * 1024;  // 100Gb
  id.assign("different");
  seqId = 5;
  off = 0;
  blockOffset = 3;
  fileSize = 128;
  success = Protocol::encodeHeader(buf, off, sizeof(buf), id, seqId, size,
                                   blockOffset, fileSize);
  EXPECT_TRUE(success);
  EXPECT_EQ(
      off,
      id.size() + 1 + 1 + 6 + 1 + 2);  // 1 byte varint for id len and size
  noff = 0;
  success = Protocol::decodeHeader(buf, noff, sizeof(buf), nid, nseqId, nsize,
                                   nblockOffset, nfileSize);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nid, id);
  EXPECT_EQ(nseqId, seqId);
  EXPECT_EQ(nsize, size);
  EXPECT_EQ(nblockOffset, blockOffset);
  EXPECT_EQ(nfileSize, fileSize);
  LOG(INFO) << "got size of " << nsize;
  // too short for size encoding:
  noff = 0;
  success = Protocol::decodeHeader(buf, noff, off - 2, nid, nseqId, nsize,
                                   nblockOffset, nfileSize);
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
