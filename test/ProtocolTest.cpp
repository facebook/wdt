/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Wdt.h>
#include <wdt/util/SerializationUtil.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace facebook {
namespace wdt {

using std::string;
using facebook::wdt::encodeString;

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
                1);  // 1 byte variant for seqId, len, size, offset and filesize
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
  EXPECT_EQ(nbd.dataSize, bd.dataSize);
  EXPECT_EQ(nbd.allocationStatus, bd.allocationStatus);
  noff = 0;
  // exact size:
  success = Protocol::decodeHeader(
      Protocol::HEADER_FLAG_AND_PREV_SEQ_ID_VERSION, buf, noff, off, nbd);
  EXPECT_TRUE(success);

  WLOG(INFO) << "error tests, expect errors";
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
                1);  // 1 byte variant for id len and size
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
  EXPECT_EQ(nbd.dataSize, bd.dataSize);
  EXPECT_EQ(nbd.allocationStatus, bd.allocationStatus);
  EXPECT_EQ(nbd.prevSeqId, bd.prevSeqId);
  WLOG(INFO) << "got size of " << nbd.dataSize;
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
  bool success = Protocol::decodeFileChunksInfo(br, nFileChunksInfo);
  EXPECT_TRUE(success);
  int64_t noff = br.start() - (uint8_t *)buf;
  EXPECT_EQ(noff, off);
  EXPECT_EQ(nFileChunksInfo, fileChunksInfo);

  // test with smaller buffer; exact size:
  br.reset((uint8_t *)buf, off);
  success = Protocol::decodeFileChunksInfo(br, nFileChunksInfo);
  EXPECT_TRUE(success);
  // 1 byte missing :
  br.reset((uint8_t *)buf, off - 1);
  success = Protocol::decodeFileChunksInfo(br, nFileChunksInfo);
  EXPECT_FALSE(success);
}

void testSettings() {
  Settings settings;
  int senderProtocolVersion = Protocol::SETTINGS_FLAG_VERSION;
  settings.readTimeoutMillis = 501;
  settings.writeTimeoutMillis = 503;
  settings.transferId = "abc";
  settings.enableChecksum = true;
  settings.sendFileChunks = false;
  settings.blockModeDisabled = true;

  char buf[128];
  int64_t off = 0;
  EXPECT_TRUE(Protocol::encodeSettings(senderProtocolVersion, buf, off,
                                       sizeof(buf), settings));

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
  EXPECT_EQ(nsettings.blockModeDisabled, settings.blockModeDisabled);
}

TEST(Protocol, EncodeString) {
  string inp1("abc");
  char buf[10];
  folly::ByteRange br((const u_int8_t *)buf, sizeof(buf));
  int64_t off = 0;
  EXPECT_TRUE(encodeString(buf, sizeof(buf), off, inp1));
  EXPECT_EQ(off, inp1.size() + 1);
  string inp2("de");
  EXPECT_TRUE(encodeString(buf, sizeof(buf), off, inp2));
  EXPECT_EQ(off, inp1.size() + inp2.size() + 2);
  string d1;
  EXPECT_TRUE(decodeString(br, d1));
  EXPECT_EQ(d1, inp1);
  string d2;
  EXPECT_TRUE(decodeString(br, d2));
  EXPECT_EQ(d2, inp2);
}

TEST(Protocol, DecodeInt32) {
  string buf;
  EXPECT_TRUE(encodeVarI64(buf, 501));
  EXPECT_TRUE(encodeVarI64(buf, 502));
  EXPECT_TRUE(encodeVarI64(buf, 1L << 32));
  EXPECT_TRUE(encodeVarI64(buf, -7));
  EXPECT_EQ(buf.size(), 2 + 2 + 5 + 1);
  folly::ByteRange br((uint8_t *)buf.data(), buf.size());
  int32_t v = -1;
  EXPECT_TRUE(decodeInt32(br, v));
  EXPECT_EQ(501, v);
  EXPECT_TRUE(decodeInt32(br, v));
  EXPECT_EQ(502, v);
  // this should log an error about overflow:
  EXPECT_FALSE(decodeInt32(br, v));
  EXPECT_EQ(502, v);  // v unchanged on error
  int64_t lv = -1;
  EXPECT_TRUE(decodeInt64(br, lv));
  EXPECT_EQ(lv, 1L << 32);
  EXPECT_TRUE(decodeInt32(br, v));
  EXPECT_EQ(v, -7);
}

TEST(Protocol, encodeVarI64C) {
  char buf[5];
  int64_t pos = 0;
  // Negative values crash the compatible (compact) version of encodeVarI64C
  EXPECT_DEATH(encodeVarI64C(buf, sizeof(buf), pos, -3),
               "Check failed: v >= 0");
}

TEST(Protocol, Simple_Header) {
  testHeader();
}
TEST(Protocol, Simple_Settings) {
  testSettings();
}
TEST(Protocol, FileChunksInfo) {
  testFileChunksInfo();
}
}
}  // namespaces

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  facebook::wdt::Wdt::initializeWdt("wdt");
  int ret = RUN_ALL_TESTS();
  return ret;
}
