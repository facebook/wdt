/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "Protocol.h"

#include "WdtFlags.h"
#include <glog/logging.h>
#include <gtest/gtest.h>

#include <folly/Random.h>

DEFINE_int32(random_seed, folly::randomNumberSeed(), "random seed");

namespace facebook {
namespace wdt {

using std::string;

void testHeader() {
  BlockDetails bd;
  bd.fileName = "abcdef";
  bd.seqId = 3;
  bd.dataSize = 3;
  bd.offset = 4;
  bd.fileSize = 10;
  bd.allocationStatus = EXISTS_CORRECT_SIZE;

  char buf[128];
  int64_t off = 0;
  Protocol::encodeHeader(Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf,
                         off, sizeof(buf), bd);
  EXPECT_EQ(off,
            bd.fileName.size() + 1 + 1 + 1 + 1 + 1 +
                1);  // 1 byte varint for seqId, len, size, offset and filesize
  BlockDetails nbd;
  int64_t noff = 0;
  bool success =
      Protocol::decodeHeader(Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf,
                             noff, sizeof(buf), nbd);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nbd.fileName, bd.fileName);
  EXPECT_EQ(nbd.seqId, bd.seqId);
  EXPECT_EQ(nbd.fileSize, bd.fileSize);
  EXPECT_EQ(nbd.offset, bd.offset);
  EXPECT_EQ(nbd.dataSize, nbd.dataSize);
  EXPECT_EQ(nbd.allocationStatus, bd.allocationStatus);
  noff = 0;
  // exact size:
  success = Protocol::decodeHeader(
      Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf, noff, off, nbd);
  EXPECT_TRUE(success);

  LOG(INFO) << "error tests, expect errors";
  // too short
  noff = 0;
  success = Protocol::decodeHeader(
      Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf, noff, off - 1, nbd);
  EXPECT_FALSE(success);

  // Long size:
  bd.dataSize = (int64_t)100 * 1024 * 1024 * 1024;  // 100Gb
  bd.fileName.assign("different");
  bd.seqId = 5;
  bd.offset = 3;
  bd.fileSize = 128;
  bd.allocationStatus = EXISTS_TOO_SMALL;
  bd.prevSeqId = 10;

  off = 0;
  Protocol::encodeHeader(Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf,
                         off, sizeof(buf), bd);
  EXPECT_EQ(off,
            bd.fileName.size() + 1 + 1 + 6 + 1 + 2 + 1 +
                1);  // 1 byte varint for id len and size
  noff = 0;
  success =
      Protocol::decodeHeader(Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf,
                             noff, sizeof(buf), nbd);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nbd.fileName, bd.fileName);
  EXPECT_EQ(nbd.seqId, bd.seqId);
  EXPECT_EQ(nbd.fileSize, bd.fileSize);
  EXPECT_EQ(nbd.offset, bd.offset);
  EXPECT_EQ(nbd.dataSize, nbd.dataSize);
  EXPECT_EQ(nbd.allocationStatus, bd.allocationStatus);
  EXPECT_EQ(nbd.prevSeqId, bd.prevSeqId);
  LOG(INFO) << "got size of " << nbd.dataSize;
  // too short for size encoding:
  noff = 0;
  success = Protocol::decodeHeader(
      Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf, noff, off - 2, nbd);
  EXPECT_FALSE(success);
}

void testFileChunksInfo() {
  FileChunksInfo fileChunksInfo;
  fileChunksInfo.setSeqId(10);
  fileChunksInfo.setFileName("abc");
  fileChunksInfo.setFileSize(128);
  fileChunksInfo.addChunk(Interval(1, 10));
  fileChunksInfo.addChunk(Interval(20, 30));

  char buf[128];
  int64_t off = 0;
  Protocol::encodeFileChunksInfo(buf, off, sizeof(buf), fileChunksInfo);
  FileChunksInfo nFileChunksInfo;
  folly::ByteRange br((uint8_t *)buf, sizeof(buf));
  bool success = Protocol::decodeFileChunksInfo(br, buf, off, nFileChunksInfo);
  EXPECT_TRUE(success);
  int64_t noff = br.start() - (uint8_t *)buf;
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nFileChunksInfo, fileChunksInfo);

  // test with smaller buffer
  br.reset((uint8_t *)buf, sizeof(buf));
  success = Protocol::decodeFileChunksInfo(br, buf, off - 2, nFileChunksInfo);
  EXPECT_FALSE(success);
}

void testSettings() {
  Settings settings;
  int senderProtocolVersion = Protocol::SETTINGS_FLAG_VERSION;
  settings.readTimeoutMillis = 500;
  settings.writeTimeoutMillis = 500;
  settings.transferId = "abc";
  settings.enableChecksum = true;
  settings.sendFileChunks = true;

  char buf[128];
  int64_t off = 0;
  Protocol::encodeSettings(senderProtocolVersion, buf, off, sizeof(buf),
                           settings);

  int nsenderProtocolVersion;
  Settings nsettings;
  int64_t noff = 0;
  bool success =
      Protocol::decodeVersion(buf, noff, off, nsenderProtocolVersion);
  EXPECT_TRUE(success);
  EXPECT_EQ(nsenderProtocolVersion, senderProtocolVersion);
  success = Protocol::decodeSettings(Protocol::SETTINGS_FLAG_VERSION, buf, noff,
                                     off, nsettings);
  EXPECT_TRUE(success);
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nsettings.readTimeoutMillis, settings.readTimeoutMillis);
  EXPECT_EQ(nsettings.writeTimeoutMillis, settings.writeTimeoutMillis);
  EXPECT_EQ(nsettings.transferId, settings.transferId);
  EXPECT_EQ(nsettings.enableChecksum, settings.enableChecksum);
  EXPECT_EQ(nsettings.sendFileChunks, settings.sendFileChunks);
}

TEST(Protocol, Simple) {
  testHeader();
  testSettings();
  testFileChunksInfo();
}
}
}  // namespaces

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::WdtFlags::initializeFromFlags();
  int ret = RUN_ALL_TESTS();
  return ret;
}
