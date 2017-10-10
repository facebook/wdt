/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/EncryptionUtils.h>
#include <folly/Conv.h>
#include <folly/SpinLock.h>
#include <folly/String.h>            // for humanify
#include <wdt/WdtTransferRequest.h>  // for to/fromHex utils

#include <openssl/crypto.h>
#include <openssl/rand.h>

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
  if (encryptionType >= NUM_ENC_TYPES) {
    WLOG(ERROR) << "Unknown encryption type " << encryptionType;
    return folly::to<std::string>(encryptionType);
  }
  return kEncryptionTypeDescriptions[encryptionType];
}

size_t encryptionTypeToTagLen(EncryptionType type) {
  return (type == ENC_AES128_GCM) ? kAESBlockSize : 0;
}

static int s_numOpensslLocks = 0;
static folly::SpinLock* s_opensslLocks{nullptr};
static void opensslLock(int mode, int type, const char* file, int line) {
  WDT_CHECK_LT(type, s_numOpensslLocks);
  if (mode & CRYPTO_LOCK) {
    s_opensslLocks[type].lock();
    WVLOG(3) << "Lock requested for " << type << " " << file << " " << line;
    return;
  }
  WVLOG(3) << "unlock requested for " << type << " " << file << " " << line;
  s_opensslLocks[type].unlock();
}

static void opensslThreadId(CRYPTO_THREADID* id) {
  CRYPTO_THREADID_set_numeric(id, (unsigned long)pthread_self());
}

WdtCryptoIntializer::WdtCryptoIntializer() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  if (CRYPTO_get_locking_callback()) {
    WLOG(WARNING) << "Openssl crypto library already initialized";
    return;
  }
  s_numOpensslLocks = CRYPTO_num_locks();
  s_opensslLocks = new folly::SpinLock[s_numOpensslLocks];
  if (!s_opensslLocks) {
    WLOG(ERROR) << "Unable to allocate openssl locks " << s_numOpensslLocks;
    return;
  }
  CRYPTO_set_locking_callback(opensslLock);
  if (!CRYPTO_THREADID_get_callback()) {
    CRYPTO_THREADID_set_callback(opensslThreadId);
  } else {
    WLOG(INFO) << "Openssl id callback already set";
  }
  WLOG(INFO) << "Openssl library initialized";
#endif
}

WdtCryptoIntializer::~WdtCryptoIntializer() {
#if OPENSSL_VERSION_NUMBER < 0x10100000L
  WVLOG(1) << "Cleaning up openssl";
  if (CRYPTO_get_locking_callback() != opensslLock) {
    WLOG(WARNING) << "Openssl not initialized by wdt";
    return;
  }
  CRYPTO_set_locking_callback(nullptr);
  if (CRYPTO_THREADID_get_callback() == opensslThreadId) {
    CRYPTO_THREADID_set_callback(nullptr);
  }
  delete[] s_opensslLocks;
#endif
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
  WLOG(WARNING) << "Unknown encryption type" << str << ", defaulting to none";
  return ENC_NONE;
}

EncryptionParams::EncryptionParams(EncryptionType type, const string& data)
    : type_(type), data_(data) {
  WLOG(INFO) << "New encryption params " << this << " " << getLogSafeString();
  if (type_ >= NUM_ENC_TYPES) {
    WLOG(ERROR) << "Unsupported type " << type;
    erase();
  }
}

bool EncryptionParams::operator==(const EncryptionParams& that) const {
  return (type_ == that.type_) && (data_ == that.data_);
}

void EncryptionParams::erase() {
  WVLOG(1) << " Erasing EncryptionParams " << this << " " << type_;
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

string EncryptionParams::getUrlSafeString() const {
  string res;
  res.reserve(/* 1 byte type, 1 byte colon */ 2 +
              /* hex is 2x length */ (2 * data_.length()));
  res.push_back(WdtUri::toHex(type_));
  res.push_back(':');
  for (unsigned char c : data_) {
    res.push_back(WdtUri::toHex(c >> 4));
    res.push_back(WdtUri::toHex(c & 0xf));
  }
  return res;
}

string EncryptionParams::getLogSafeString() const {
  string res;
  res.push_back(WdtUri::toHex(type_));
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
          WLOG(ERROR) << "Enc type still none when ':' reached " << input;
          return ERROR;
        }
        state = FIRST_HEX;
        continue;
      }
    }
    int v = WdtUri::fromHex(c);
    if (v < 0) {
      WLOG(ERROR) << "Not hex found " << (int)c << " in " << input;
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
    WLOG(ERROR) << "Missing ':' in encryption data " << input;
    return ERROR;
  }
  if (state != LEFT_HEX) {
    WLOG(ERROR) << "Odd number of hex in encryption data " << input
                << " decoded up to: " << out.data_;
    return ERROR;
  }
  if (type <= ENC_NONE || type >= NUM_ENC_TYPES) {
    WLOG(ERROR) << "Encryption type out of range " << type;
    return ERROR;
  }
  out.type_ = static_cast<EncryptionType>(type);
  WVLOG(1) << "Deserialized Encryption Params " << out.getLogSafeString();
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
    WLOG(ERROR) << "RAND_bytes failed, unable to generate symmetric key";
    return EncryptionParams();
  }
  return EncryptionParams(type, std::string(key, key + kAESBlockSize));
}

bool AESBase::cloneCtx(EVP_CIPHER_CTX* ctxOut) const {
  WDT_CHECK(encryptionTypeToTagLen(type_));
  int status = EVP_CIPHER_CTX_copy(ctxOut, evpCtx_.get());
  if (status != 1) {
    WLOG(ERROR) << "Cipher ctx copy failed " << status;
    return false;
  }
  return true;
}

const EVP_CIPHER* AESBase::getCipher(const EncryptionType encryptionType) {
  if (encryptionType == ENC_AES128_CTR) {
    return EVP_aes_128_ctr();
  }
  if (encryptionType == ENC_AES128_GCM) {
    return EVP_aes_128_gcm();
  }
  WLOG(ERROR) << "Unknown encryption type " << encryptionType;
  return nullptr;
}

EVP_CIPHER_CTX* createAndInitCtx() {
  auto ctx = EVP_CIPHER_CTX_new();
  EVP_CIPHER_CTX_init(ctx);
  return ctx;
}

void cleanupAndDestroyCtx(EVP_CIPHER_CTX* ctx) {
  EVP_CIPHER_CTX_cleanup(ctx);
  EVP_CIPHER_CTX_free(ctx);
}

bool AESEncryptor::start(const EncryptionParams& encryptionData,
                         std::string& ivOut) {
  WDT_CHECK(!started_);

  // reset the enc ctx
  // To reuse the same ctx, we have to have different reset code for different
  // openssl version. So, we will just create another ctx for simplification
  evpCtx_.reset(createAndInitCtx());

  type_ = encryptionData.getType();

  const std::string& key = encryptionData.getSecret();
  if (key.length() != kAESBlockSize) {
    WLOG(ERROR) << "Encryption key size must be " << kAESBlockSize
                << ", but input size length " << key.length();
    return false;
  }

  ivOut.resize(kAESBlockSize);

  uint8_t* ivPtr = (uint8_t*)(&ivOut.front());
  uint8_t* keyPtr = (uint8_t*)(&key.front());
  if (RAND_bytes(ivPtr, kAESBlockSize) != 1) {
    WLOG(ERROR)
        << "RAND_bytes failed, unable to generate initialization vector";
    return false;
  }

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
    if (EVP_EncryptInit_ex(evpCtx_.get(), cipher, nullptr, nullptr, nullptr) !=
        1) {
      WLOG(ERROR) << "GCM First init error";
    }
    if (EVP_CIPHER_CTX_ctrl(evpCtx_.get(), EVP_CTRL_GCM_SET_IVLEN, ivOut.size(),
                            nullptr) != 1) {
      WLOG(ERROR) << "Encrypt Init ivlen set failed";
    }
  }

  if (EVP_EncryptInit_ex(evpCtx_.get(), cipher, nullptr, keyPtr, ivPtr) != 1) {
    WLOG(ERROR) << "Encrypt Init failed";
    return false;
  }
  started_ = true;
  return true;
}

bool AESEncryptor::encrypt(const char* in, const int inLength, char* out) {
  WDT_CHECK(started_);

  int outLength;
  if (EVP_EncryptUpdate(evpCtx_.get(), (uint8_t*)out, &outLength, (uint8_t*)in,
                        inLength) != 1) {
    WLOG(ERROR) << "EncryptUpdate failed";
    return false;
  }
  WDT_CHECK_EQ(inLength, outLength);
  numProcessed_ += inLength;
  return true;
}

/* static */
bool AESEncryptor::finishInternal(EVP_CIPHER_CTX* ctx,
                                  const EncryptionType type,
                                  std::string& tagOut) {
  int outLength;
  int status = EVP_EncryptFinal(ctx, nullptr, &outLength);
  if (status != 1) {
    WLOG(ERROR) << "EncryptFinal failed";
    return false;
  }
  WDT_CHECK_EQ(0, outLength);
  size_t tagSize = encryptionTypeToTagLen(type);
  if (tagSize) {
    tagOut.resize(tagSize);
    status = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, tagOut.size(),
                                 &(tagOut.front()));
    if (status != 1) {
      WLOG(ERROR) << "EncryptFinal Tag extraction error "
                  << folly::humanify(tagOut);
      tagOut.clear();
    }
  }
  return true;
}

bool AESEncryptor::finish(std::string& tagOut) {
  tagOut.clear();
  if (!started_) {
    return true;
  }
  started_ = false;
  bool status = finishInternal(evpCtx_.get(), type_, tagOut);
  WLOG_IF(INFO, status) << "Encryption finish tag = "
                        << folly::humanify(tagOut);
  return status;
}

std::string AESEncryptor::computeCurrentTag() {
  std::unique_ptr<EVP_CIPHER_CTX, CipherCtxDeleter> ctx{createAndInitCtx()};
  std::string tag;
  if (!cloneCtx(ctx.get())) {
    return tag;
  }
  finishInternal(ctx.get(), type_, tag);
  return tag;
}

AESEncryptor::~AESEncryptor() {
  std::string tag;
  finish(tag);
}

bool AESDecryptor::start(const EncryptionParams& encryptionData,
                         const std::string& iv) {
  WDT_CHECK(!started_);

  // reset the enc ctx
  evpCtx_.reset(createAndInitCtx());

  type_ = encryptionData.getType();

  const std::string& key = encryptionData.getSecret();
  if (key.length() != kAESBlockSize) {
    WLOG(ERROR) << "Encryption key size must be " << kAESBlockSize
                << ", but input size length " << key.length();
    return false;
  }
  if (iv.length() != kAESBlockSize) {
    WLOG(ERROR) << "Initialization size must be " << kAESBlockSize
                << ", but input size length " << iv.length();
    return false;
  }

  uint8_t* ivPtr = (uint8_t*)(&iv.front());
  uint8_t* keyPtr = (uint8_t*)(&key.front());

  const EVP_CIPHER* cipher = getCipher(type_);
  if (cipher == nullptr) {
    return false;
  }
  int cipherBlockSize = EVP_CIPHER_block_size(cipher);
  // block size for ctr mode should be 1
  WDT_CHECK_EQ(1, cipherBlockSize);

  if (type_ == ENC_AES128_GCM) {
    if (EVP_EncryptInit_ex(evpCtx_.get(), cipher, nullptr, nullptr, nullptr) !=
        1) {
      WLOG(ERROR) << "GCM Decryptor First init error";
    }
    if (EVP_CIPHER_CTX_ctrl(evpCtx_.get(), EVP_CTRL_GCM_SET_IVLEN, iv.size(),
                            nullptr) != 1) {
      WLOG(ERROR) << "Encrypt Init ivlen set failed";
    }
  }

  if (EVP_DecryptInit_ex(evpCtx_.get(), cipher, nullptr, keyPtr, ivPtr) != 1) {
    WLOG(ERROR) << "Decrypt Init failed";
    return false;
  }
  started_ = true;
  return true;
}

bool AESDecryptor::decrypt(const char* in, const int inLength, char* out) {
  WDT_CHECK(started_);

  int outLength;
  if (EVP_DecryptUpdate(evpCtx_.get(), (uint8_t*)out, &outLength, (uint8_t*)in,
                        inLength) != 1) {
    WLOG(ERROR) << "DecryptUpdate failed";
    return false;
  }
  WDT_CHECK_EQ(inLength, outLength);
  numProcessed_ += inLength;
  return true;
}

bool AESDecryptor::verifyTag(const std::string& tag) {
  WDT_CHECK_EQ(ENC_AES128_GCM, type_);
  std::unique_ptr<EVP_CIPHER_CTX, CipherCtxDeleter> clonedCtx{
      createAndInitCtx()};
  if (!cloneCtx(clonedCtx.get())) {
    return false;
  }
  return finishInternal(clonedCtx.get(), type_, tag);
}

/* static */
bool AESDecryptor::finishInternal(EVP_CIPHER_CTX* ctx,
                                  const EncryptionType type,
                                  const std::string& tag) {
  int status;
  size_t tagSize = encryptionTypeToTagLen(type);
  if (tagSize) {
    if (tag.size() != tagSize) {
      WLOG(ERROR) << "Need tag for gcm mode " << folly::humanify(tag);
      return false;
    }
    // EVP_CIPHER_CTX_ctrl takes a non const buffer. But, for set tag the buffer
    // will not be modified. So, it is safe to use const_cast here.
    char* tagBuf = const_cast<char*>(tag.data());
    status = EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, tag.size(), tagBuf);
    if (status != 1) {
      WLOG(ERROR) << "Decrypt final tag set error " << folly::humanify(tag);
    }
  }

  int outLength = 0;
  status = EVP_DecryptFinal(ctx, nullptr, &outLength);
  if (status != 1) {
    WLOG(ERROR) << "DecryptFinal failed " << outLength;
    return false;
  }
  WDT_CHECK_EQ(0, outLength);
  return true;
}

bool AESDecryptor::finish(const std::string& tag) {
  if (!started_) {
    return true;
  }
  started_ = false;
  bool status = finishInternal(evpCtx_.get(), type_, tag);
  WLOG_IF(INFO, status) << "Successful end of decryption with tag = "
                        << folly::humanify(tag);
  return status;
}

AESDecryptor::~AESDecryptor() {
  std::string tag;
  finish(tag);
}
}
}  // end of namespaces
