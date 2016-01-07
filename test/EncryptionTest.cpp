#include <wdt/util/EncryptionUtils.h>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include <cstring>

namespace facebook {
namespace wdt {

// Removed & on plaintext purpose:
// make a copy to help ensure we don't accidentally
// mutate the original data
void testEncryption(const EncryptionType encryptionType,
                    const std::string plaintext) {
  AESEncryptor encryptor;
  AESDecryptor decryptor;

  std::string iv;
  EncryptionParams encryptionData =
      EncryptionParams::generateEncryptionParams(encryptionType);
  EXPECT_EQ(encryptionType, encryptionData.getType());

  LOG(INFO) << "Generated encryption key for type " << encryptionType;

  const int length = plaintext.size();
  uint8_t encryptedText[length];

  bool success = encryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success =
      encryptor.encrypt((uint8_t *)(plaintext.data()), length, encryptedText);
  EXPECT_TRUE(success);
  success = encryptor.finish();
  EXPECT_TRUE(success);

  decryptor.setTag(encryptor.getTag());

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
  encryptedText[3]++;
  success = decryptor.start(encryptionData, iv);
  EXPECT_TRUE(success);
  success = decryptor.decrypt(encryptedText, length, decryptedText);
  // EXPECT_EQ((encryptionType != ENC_AES128_GCM), success);
  EXPECT_EQ(true, success);
  success = decryptor.finish();
  // gcm does/should detect the error, not the others:
  EXPECT_EQ((encryptionType != ENC_AES128_GCM), success);
  // But none of them should get back our input:
  EXPECT_NE(plaintext, std::string(decryptedText, decryptedText + length));
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
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
