/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/EncryptionUtils.h>
#include <folly/Conv.h>
#include <folly/SpinLock.h>
#include <folly/String.h>  // for humanify

#include <openssl/rand.h>
#include <openssl/crypto.h>

#include <string.h>  // for memset

namespace facebook {
namespace wdt {

using std::string;

// When we get more than 15 types we need to start encoding with more
// than 1 hex character, the decoding already support more than 1
static_assert(NUM_ENC_TYPES <= 16, "need to change encoding for types");

const char* const kEncryptionTypeDescriptions[] = {"none", "aes128ctr",
                                                   "aes128gcm"};

static_assert(NUM_ENC_TYPES ==
                  sizeof(kEncryptionTypeDescriptions) /
                      sizeof(kEncryptionTypeDescriptions[0]),
              "must provide description for all encryption types");

std::string encryptionTypeToStr(EncryptionType encryptionType) {
  if (encryptionType < 0 || encryptionType >= NUM_ENC_TYPES) {
    LOG(ERROR) << "Unknown encryption type " << encryptionType;
    return folly::to<std::string>(encryptionType);
  }
  return kEncryptionTypeDescriptions[encryptionType];
}

static int s_numOpensslLocks = 0;
static folly::SpinLock* s_opensslLocks{nullptr};
static void opensslLock(int mode, int type, const char* file, int line) {
  WDT_CHECK_LT(type, s_numOpensslLocks);
  if (mode & CRYPTO_LOCK) {
    s_opensslLocks[type].lock();
    VLOG(3) << "Lock requested for " << type << " " << file << " " << line;
    return;
  }
  VLOG(3) << "unlock requested for " << type << " " << file << " " << line;
  s_opensslLocks[type].unlock();
}

static void opensslThreadId(CRYPTO_THREADID* id) {
  CRYPTO_THREADID_set_numeric(id, (unsigned long)pthread_self());
}

WdtCryptoIntializer::WdtCryptoIntializer() {
  if (CRYPTO_get_locking_callback()) {
    LOG(WARNING) << "Openssl crypto library already initialized";
    return;
  }
  s_numOpensslLocks = CRYPTO_num_locks();
  s_opensslLocks = new folly::SpinLock[s_numOpensslLocks];
  if (!s_opensslLocks) {
    LOG(ERROR) << "Unable to allocate openssl locks " << s_numOpensslLocks;
    return;
  }
  CRYPTO_set_locking_callback(opensslLock);
  if (!CRYPTO_THREADID_get_callback()) {
    CRYPTO_THREADID_set_callback(opensslThreadId);
  } else {
    LOG(INFO) << "Openssl id callback already set";
  }
  LOG(INFO) << "Openssl library initialized";
}

WdtCryptoIntializer::~WdtCryptoIntializer() {
  VLOG(1) << "Cleaning up openssl";
  if (CRYPTO_get_locking_callback() != opensslLock) {
    LOG(WARNING) << "Openssl not initialized by wdt";
    return;
  }
  CRYPTO_set_locking_callback(nullptr);
  if (CRYPTO_THREADID_get_callback() == opensslThreadId) {
    CRYPTO_THREADID_set_callback(nullptr);
  }
  delete[] s_opensslLocks;
}

EncryptionType parseEncryptionType(const std::string& str) {
  if (str == kEncryptionTypeDescriptions[ENC_AES128_GCM]) {
    return ENC_AES128_GCM;
  }
  if (str == kEncryptionTypeDescriptions[ENC_AES128_CTR]) {
    return ENC_AES128_CTR;
  }
  if (str == kEncryptionTypeDescriptions[ENC_NONE]) {
    return ENC_NONE;
  }
  LOG(WARNING) << "Unknown encryption type" << str << ", defaulting to none";
  return ENC_NONE;
}

EncryptionParams::EncryptionParams(EncryptionType type, const string& data)
    : type_(type), data_(data) {
  LOG(INFO) << "New encryption params " << this << " " << getLogSafeString();
  if (type_ < 0 || type_ >= NUM_ENC_TYPES) {
    LOG(ERROR) << "Unsupported type " << type;
    erase();
  }
}

bool EncryptionParams::operator==(const EncryptionParams& that) const {
  return (type_ == that.type_) && (data_ == that.data_);
}

void EncryptionParams::erase() {
  VLOG(1) << " Erasing EncryptionParams " << this << " " << type_;
  // Erase the key (once for now...)
  if (!data_.empty()) {
    // Can't use .data() here (copy on write fbstring)
    memset(&data_.front(), 0, data_.size());
  }
  data_.clear();
  type_ = ENC_NONE;
}

EncryptionParams::~EncryptionParams() {
  erase();
}

char toHex(unsigned char v) {
  WDT_CHECK_LT(v, 16);
  if (v <= 9) {
    return '0' + v;
  }
  return 'a' + v - 10;
}

int fromHex(char c) {
  if (c < '0' || (c > '9' && (c < 'a' || c > 'f'))) {
    return -1;  // invalid not 0-9a-f hex char
  }
  if (c <= '9') {
    return c - '0';
  }
  return c - 'a' + 10;
}

string EncryptionParams::getUrlSafeString() const {
  string res;
  res.reserve(/* 1 byte type, 1 byte colon */ 2 +
              /* hex is 2x length */ (2 * data_.length()));
  res.push_back(toHex(type_));
  res.push_back(':');
  for (unsigned char c : data_) {
    res.push_back(toHex(c >> 4));
    res.push_back(toHex(c & 0xf));
  }
  return res;
}

string EncryptionParams::getLogSafeString() const {
  string res;
  res.push_back(toHex(type_));
  res.push_back(':');
  res.append("...");
  res.append(std::to_string(std::hash<string>()(data_)));
  res.append("...");
  return res;
}

/* static */
ErrorCode EncryptionParams::unserialize(const string& input,
                                        EncryptionParams& out) {
  out.erase();
  enum {
    IN_TYPE,
    FIRST_HEX,
    LEFT_HEX,
    RIGHT_HEX,
  } state = IN_TYPE;
  int type = 0;
  int byte = 0;
  for (char c : input) {
    if (state == IN_TYPE) {
      // In type section (before ':')
      if (c == ':') {
        if (type == 0) {
          LOG(ERROR) << "Enc type still none when ':' reached " << input;
          return ERROR;
        }
        state = FIRST_HEX;
        continue;
      }
    }
    int v = fromHex(c);
    if (v < 0) {
      LOG(ERROR) << "Not hex found " << (int)c << " in " << input;
      return ERROR;
    }
    if (state == IN_TYPE) {
      // Pre : hex digits
      type = (type << 4) | v;
      continue;
    }
    if (state != RIGHT_HEX) {
      // First or Left (even) hex digit:
      byte = v << 4;
      state = RIGHT_HEX;
      continue;
    }
    // Right (odd) hex digit:
    out.data_.push_back((char)(byte | v));
    state = LEFT_HEX;
    byte = 0;  // not needed but safer
  }
  if (state == IN_TYPE) {
    LOG(ERROR) << "Missing ':' in encryption data " << input;
    return ERROR;
  }
  if (state != LEFT_HEX) {
    LOG(ERROR) << "Odd number of hex in encryption data " << input
               << " decoded up to: " << out.data_;
    return ERROR;
  }
  if (type <= ENC_NONE || type >= NUM_ENC_TYPES) {
    LOG(ERROR) << "Encryption type out of range " << type;
    return ERROR;
  }
  out.type_ = static_cast<EncryptionType>(type);
  VLOG(1) << "Deserialized Encryption Params " << out.getLogSafeString();
  return OK;
}

/* static */
EncryptionParams EncryptionParams::generateEncryptionParams(
    EncryptionType type) {
  if (type == ENC_NONE) {
    return EncryptionParams();
  }
  WDT_CHECK(type > ENC_NONE && type < NUM_ENC_TYPES);
  uint8_t key[kAESBlockSize];
  if (RAND_bytes(key, kAESBlockSize) != 1) {
    LOG(ERROR) << "RAND_bytes failed, unable to generate symmetric key";
    return EncryptionParams();
  }
  return EncryptionParams(type, std::string(key, key + kAESBlockSize));
}

const EVP_CIPHER* AESBase::getCipher(const EncryptionType encryptionType) {
  if (encryptionType == ENC_AES128_CTR) {
    return EVP_aes_128_ctr();
  }
  if (encryptionType == ENC_AES128_GCM) {
    return EVP_aes_128_gcm();
  }
  LOG(ERROR) << "Unknown encryption type " << encryptionType;
  return nullptr;
}

bool AESEncryptor::start(const EncryptionParams& encryptionData,
                         std::string& ivOut) {
  WDT_CHECK(!started_);

  type_ = encryptionData.getType();

  const std::string& key = encryptionData.getSecret();
  if (key.length() != kAESBlockSize) {
    LOG(ERROR) << "Encryption key size must be " << kAESBlockSize
               << ", but input size length " << key.length();
    return false;
  }

  ivOut.resize(kAESBlockSize);

  uint8_t* ivPtr = (uint8_t*)(&ivOut.front());
  uint8_t* keyPtr = (uint8_t*)(&key.front());
  if (RAND_bytes(ivPtr, kAESBlockSize) != 1) {
    LOG(ERROR) << "RAND_bytes failed, unable to generate initialization vector";
    return false;
  }

  EVP_CIPHER_CTX_init(&evpCtx_);

  const EVP_CIPHER* cipher = getCipher(type_);
  if (cipher == nullptr) {
    return false;
  }
  int cipherBlockSize = EVP_CIPHER_block_size(cipher);
  WDT_CHECK_EQ(1, cipherBlockSize);

  // Not super clear this is actually needed - but probably if not set
  // gcm only uses 96 out of the 128 bits of IV. Let's use all of it to
  // reduce chances of attacks on large data transfers.
  if (type_ == ENC_AES128_GCM) {
    if (EVP_EncryptInit_ex(&evpCtx_, cipher, nullptr, nullptr, nullptr) != 1) {
      LOG(ERROR) << "GCM First init error";
    }
    if (EVP_CIPHER_CTX_ctrl(&evpCtx_, EVP_CTRL_GCM_SET_IVLEN, ivOut.size(),
                            nullptr) != 1) {
      LOG(ERROR) << "Encrypt Init ivlen set failed";
    }
  }

  if (EVP_EncryptInit_ex(&evpCtx_, cipher, nullptr, keyPtr, ivPtr) != 1) {
    LOG(ERROR) << "Encrypt Init failed";
    EVP_CIPHER_CTX_cleanup(&evpCtx_);
    return false;
  }
  started_ = true;
  return true;
}

bool AESEncryptor::encrypt(const uint8_t* in, const int inLength,
                           uint8_t* out) {
  WDT_CHECK(started_);

  int outLength;
  if (EVP_EncryptUpdate(&evpCtx_, out, &outLength, in, inLength) != 1) {
    LOG(ERROR) << "EncryptUpdate failed";
    return false;
  }
  WDT_CHECK_EQ(inLength, outLength);
  return true;
}

bool AESEncryptor::finish() {
  if (!started_) {
    return true;
  }

  int outLength;
  int status = EVP_EncryptFinal(&evpCtx_, nullptr, &outLength);
  started_ = false;
  if (status != 1) {
    LOG(ERROR) << "EncryptFinal failed";
    EVP_CIPHER_CTX_cleanup(&evpCtx_);
    return false;
  }
  WDT_CHECK_EQ(0, outLength);
  const int tagSize = expectsTag();
  if (tagSize) {
    tag_.resize(tagSize);
    status = EVP_CIPHER_CTX_ctrl(&evpCtx_, EVP_CTRL_GCM_GET_TAG, tag_.size(),
                                 &(tag_.front()));
    if (status != 1) {
      LOG(ERROR) << "EncryptFinal Tag extraction error "
                 << folly::humanify(tag_);
      tag_.clear();
    } else {
      LOG(INFO) << "Encryption finish tag = " << folly::humanify(tag_);
    }
  }
  EVP_CIPHER_CTX_cleanup(&evpCtx_);
  return true;
}

AESEncryptor::~AESEncryptor() {
  finish();
}

bool AESDecryptor::start(const EncryptionParams& encryptionData,
                         const std::string& iv) {
  WDT_CHECK(!started_);

  type_ = encryptionData.getType();

  const std::string& key = encryptionData.getSecret();
  if (key.length() != kAESBlockSize) {
    LOG(ERROR) << "Encryption key size must be " << kAESBlockSize
               << ", but input size length " << key.length();
    return false;
  }
  if (iv.length() != kAESBlockSize) {
    LOG(ERROR) << "Initialization size must be " << kAESBlockSize
               << ", but input size length " << iv.length();
    return false;
  }

  uint8_t* ivPtr = (uint8_t*)(&iv.front());
  uint8_t* keyPtr = (uint8_t*)(&key.front());
  EVP_CIPHER_CTX_init(&evpCtx_);

  const EVP_CIPHER* cipher = getCipher(type_);
  if (cipher == nullptr) {
    return false;
  }
  int cipherBlockSize = EVP_CIPHER_block_size(cipher);
  // block size for ctr mode should be 1
  WDT_CHECK_EQ(1, cipherBlockSize);

  if (type_ == ENC_AES128_GCM) {
    if (EVP_EncryptInit_ex(&evpCtx_, cipher, nullptr, nullptr, nullptr) != 1) {
      LOG(ERROR) << "GCM Decryptor First init error";
    }
    if (EVP_CIPHER_CTX_ctrl(&evpCtx_, EVP_CTRL_GCM_SET_IVLEN, iv.size(),
                            nullptr) != 1) {
      LOG(ERROR) << "Encrypt Init ivlen set failed";
    }
  }

  if (EVP_DecryptInit_ex(&evpCtx_, cipher, nullptr, keyPtr, ivPtr) != 1) {
    LOG(ERROR) << "Decrypt Init failed";
    EVP_CIPHER_CTX_cleanup(&evpCtx_);
    return false;
  }
  started_ = true;
  return true;
}

bool AESDecryptor::decrypt(const uint8_t* in, const int inLength,
                           uint8_t* out) {
  WDT_CHECK(started_);

  int outLength;
  if (EVP_DecryptUpdate(&evpCtx_, out, &outLength, in, inLength) != 1) {
    LOG(ERROR) << "DecryptUpdate failed";
    return false;
  }
  WDT_CHECK_EQ(inLength, outLength);
  return true;
}

bool AESDecryptor::finish() {
  if (!started_) {
    return true;
  }
  int status;
  const size_t tagSize = expectsTag();
  if (tagSize) {
    if (tag_.size() != tagSize) {
      LOG(ERROR) << "Need tag for gcm mode " << folly::humanify(tag_);
      EVP_CIPHER_CTX_cleanup(&evpCtx_);
      started_ = false;
      return false;
    }
    status = EVP_CIPHER_CTX_ctrl(&evpCtx_, EVP_CTRL_GCM_SET_TAG, tag_.size(),
                                 &(tag_.front()));
    if (status != 1) {
      LOG(ERROR) << "Decrypt final tag set error " << folly::humanify(tag_);
    }
  }

  int outLength = 0;
  status = EVP_DecryptFinal(&evpCtx_, nullptr, &outLength);
  EVP_CIPHER_CTX_cleanup(&evpCtx_);
  started_ = false;
  if (status != 1) {
    LOG(ERROR) << "DecryptFinal failed " << outLength;
    return false;
  }
  LOG(INFO) << "Succcesful end of decryption with tag = "
            << folly::humanify(tag_);
  WDT_CHECK_EQ(0, outLength);
  return true;
}

AESDecryptor::~AESDecryptor() {
  finish();
}
}
}  // end of namespaces
