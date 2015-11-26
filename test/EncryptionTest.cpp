#include <wdt/util/EncryptionUtils.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>

namespace facebook {
namespace wdt {

void testEncryption(const EncryptionType encryptionType,
                    const std::string &plaintext) {
  AESEncryptor encryptor;
  AESDecryptor decryptor;

  std::string iv;
  EncryptionParams encryptionData =
      EncryptionParams::generateEncryptionParams(encryptionType);
  EXPECT_EQ(encryptionType, encryptionData.getType());

  LOG(INFO) << "Generated encryption key";

  const int length = plaintext.size();
  uint8_t encryptedText[length];

  bool success = encryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success =
      encryptor.encrypt((uint8_t *)(plaintext.data()), length, encryptedText);
  EXPECT_TRUE(success);
  success = encryptor.finish();
  EXPECT_TRUE(success);

  EXPECT_NE(plaintext, std::string(encryptedText, encryptedText + length));

  uint8_t decryptedText[length];
  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = decryptor.decrypt(encryptedText, length, decryptedText);
  EXPECT_TRUE(success);
  EXPECT_EQ(plaintext, std::string(decryptedText, decryptedText + length));
  success = decryptor.finish();
  EXPECT_TRUE(success);

  // change one byte. Should not decrypt to the correct plain text
  encryptedText[0]++;
  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = decryptor.decrypt(encryptedText, length, decryptedText);
  EXPECT_TRUE(success);
  EXPECT_NE(plaintext, std::string(decryptedText, decryptedText + length));
  success = decryptor.finish();
  EXPECT_TRUE(success);
}

TEST(Encryption, Simple) {
  std::string text1("abcdef");
  std::string text2;
  for (int i = 0; i < 200; i++) {
    text2.push_back(i);
  }
  testEncryption(ENC_AES128_CTR, text1);
  testEncryption(ENC_AES128_CTR, text2);
  testEncryption(ENC_AES128_OFB, text1);
  testEncryption(ENC_AES128_OFB, text2);
}
}
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
