#include <wdt/util/EncryptionUtils.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

namespace facebook {
namespace wdt {

// Removed & on plaintext purpose:
// make a copy to help ensure we don't accidentally
// mutate the original data
void testEncryption(const EncryptionType encryptionType,
                    const std::string &plaintext) {
  AESEncryptor encryptor;
  AESDecryptor decryptor;

  std::string iv;
  EncryptionParams encryptionData =
      EncryptionParams::generateEncryptionParams(encryptionType);
  EXPECT_EQ(encryptionType, encryptionData.getType());

  WLOG(INFO) << "Generated encryption key for type " << encryptionType;

  const int length = plaintext.size();
  char encryptedText[length];

  bool success = encryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = encryptor.encrypt(plaintext.data(), length, encryptedText);
  EXPECT_TRUE(success);
  std::string tag;
  success = encryptor.finish(tag);
  EXPECT_TRUE(success);

  EXPECT_NE(plaintext, std::string(encryptedText, encryptedText + length));

  char decryptedText[length];
  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = decryptor.decrypt(encryptedText, length, decryptedText);
  EXPECT_TRUE(success);
  EXPECT_EQ(plaintext, std::string(decryptedText, decryptedText + length));
  success = decryptor.finish(tag);
  EXPECT_TRUE(success);

  // change one byte. Should not decrypt to the correct plain text
  encryptedText[3]++;
  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = decryptor.decrypt(encryptedText, length, decryptedText);
  // EXPECT_EQ((encryptionType != ENC_AES128_GCM), success);
  EXPECT_TRUE(success);
  success = decryptor.finish(tag);
  // gcm does/should detect the error, not the others:
  EXPECT_EQ((encryptionType != ENC_AES128_GCM), success);
  // But none of them should get back our input:
  EXPECT_NE(plaintext, std::string(decryptedText, decryptedText + length));

  if (!encryptionTypeToTagLen(encryptionType)) {
    return;
  }
  // test incremental verification
  const int numInterval = 5;
  std::vector<std::string> tags;
  std::vector<int> lengths;
  int lenSum = 0;

  success = encryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  for (int i = 0; i < numInterval; i++) {
    lengths.emplace_back((length - lenSum) / (numInterval - i));
    success = encryptor.encrypt(plaintext.data() + lenSum, lengths[i],
                                encryptedText + lenSum);
    EXPECT_TRUE(success);
    std::string curTag = encryptor.computeCurrentTag();
    EXPECT_EQ(std::find(tags.begin(), tags.end(), curTag), tags.end());
    tags.emplace_back(curTag);
    lenSum += lengths[i];
  }
  EXPECT_EQ(lenSum, length);
  success = encryptor.finish(tag);
  EXPECT_TRUE(success);
  EXPECT_EQ(tag, tags.back());

  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  lenSum = 0;
  for (int i = 0; i < numInterval; i++) {
    success = decryptor.decrypt(encryptedText + lenSum, lengths[i],
                                decryptedText + lenSum);
    EXPECT_TRUE(success);
    success = decryptor.verifyTag(tags[i]);
    EXPECT_TRUE(success);
    lenSum += lengths[i];
  }
  success = decryptor.finish(tag);
  EXPECT_TRUE(success);
  EXPECT_EQ(plaintext, std::string(decryptedText, decryptedText + length));
}

// TODO: uh... this is really a super basic/not great test (better than
// nothing but barely)
TEST(Encryption, SuperBasic) {
  std::string text1("abcdef");
  text1.push_back('\0');  // let's have some binary in there
  text1.append("89");     // even number and small (9)
  std::string text2;
  for (int i = 0; i < 274; i++) {
    text2.push_back(i);  // will wrap and bigger than 256
  }
  std::string text3 = text2;
  for (int i = ENC_NONE + 1; i < NUM_ENC_TYPES; ++i) {
    EncryptionType t = static_cast<EncryptionType>(i);
    testEncryption(t, text1);
    testEncryption(t, text2);
    EXPECT_EQ(text2, text3);  // paranoia
  }
}
}
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
