/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <memory>
#include <string>

#include <folly/Memory.h>
#include <openssl/evp.h>
#include <wdt/ErrorCodes.h>

namespace facebook {
namespace wdt {

/// AES encryption block size
const int kAESBlockSize = 16;

enum EncryptionType { ENC_NONE, ENC_AES128_CTR, ENC_AES128_GCM, NUM_ENC_TYPES };

/// @return  string description for encryption type
std::string encryptionTypeToStr(EncryptionType encryptionType);

/// @return  encryption type for the input string
EncryptionType parseEncryptionType(const std::string& str);

/// @returns 0 if no tag for the algorithm or the size in bytes
///  for now only gcm produces/requires tag (hmac) of 128bits (16 bytes)
size_t encryptionTypeToTagLen(EncryptionType type);

/// class responsible for initializing openssl
class WdtCryptoIntializer {
 public:
  WdtCryptoIntializer();
  ~WdtCryptoIntializer();
};

class EncryptionParams {
 public:
  /**
   * Generates encryption params
   *
   * @param type       type of encryption
   *
   * @return           generated encryption params
   */
  static EncryptionParams generateEncryptionParams(EncryptionType type);

  // Returns a string safe to print in logs (doesn't contain the secret but
  // a hash of it instead)
  std::string getLogSafeString() const;
  /**
   * Returns a string "type:encodedData" containing the secret using only
   * alphabetical characters
   */
  std::string getUrlSafeString() const;
  /// Returns the type or "error: msg" upon error
  EncryptionType getType() const {
    return type_;
  }
  /// Returns the data/secret part or empty upon error
  const std::string& getSecret() const {
    return data_;
  }
  /// isSet()
  bool isSet() const {
    return type_ != ENC_NONE;
  }

  /// Empty constructor - use the default assignement operator to fill later
  EncryptionParams() : type_{ENC_NONE} {
  }

  /// Normal constructor when we have data
  EncryptionParams(EncryptionType type, const std::string& data);

  /// Tests equality
  bool operator==(const EncryptionParams& that) const;

  /// Erase (clears the object - call as soon as you don't need secret anymore)
  void erase();
  /// Will also erase for safety against accidental memory recycling
  ~EncryptionParams();

  /// Reconstructs an EncryptionParams object from a previous getUrlSafeString()
  static ErrorCode unserialize(const std::string& urlSafeStr,
                               EncryptionParams& result);

 private:
  EncryptionType type_;
  std::string data_;
  std::string tag_;
};

/// base class to share code between encyptor and decryptor
class AESBase {
 protected:
  AESBase();
  virtual ~AESBase();

  /// evpCtx_ is copied into ctxOut
  /// @return     whether the cloning was successful
  bool cloneCtx(EVP_CIPHER_CTX* const ctxOut) const;

  using CipherCtxDeleter =
      folly::static_function_deleter<EVP_CIPHER_CTX, &EVP_CIPHER_CTX_free>;

  /// @return   cipher for a encryption type
  const EVP_CIPHER* getCipher(const EncryptionType encryptionType);

  static void resetCipherCtx(EVP_CIPHER_CTX* const ctx);

  EncryptionType type_;
  std::unique_ptr<EVP_CIPHER_CTX, CipherCtxDeleter> evpCtx_;
  bool started_;
};

/// encryptor class
class AESEncryptor : public AESBase {
 public:
  /**
   * should be called before starting to encrypt
   *
   * @param encryptionData  encryption info
   * @param ivOut           this is set to generated initialization vector
   *
   * @return    whether start was successful
   */
  bool start(const EncryptionParams& encryptionData, std::string& ivOut);

  /**
   * encrypts data. should be called after start
   *
   * @param in        data ptr to encrypt
   * @param inLength  input data length
   * @param out       encrypted string is written here
   *
   * @return    whether the string was successfully encrypted
   */
  bool encrypt(const char* in, int inLength, char* out);

  /**
   * should be called after all the encryption is done. After this call,
   * encryptor object can be reused. tagOut is set to the generated tag
   *
   * @return    whether the finish was successfully
   */
  bool finish(std::string& tagOut);

  /// return current tag
  std::string computeCurrentTag();

  /// destructor
  virtual ~AESEncryptor();

 private:
  static bool finishInternal(EVP_CIPHER_CTX* const ctx, EncryptionType type,
                             std::string& tagOut);
};

/// decryptor class
class AESDecryptor : public AESBase {
 public:
  /**
   * should be called before starting to decrypt
   *
   * @param encryptionData  encryption info
   * @param iv              initialization vector
   *
   * @return    whether start was successful
   */
  bool start(const EncryptionParams& encryptionData, const std::string& iv);

  /**
   * decrypts data. should be called after start
   *
   * @param in        data ptr to decrypt
   * @param inLength  input data length
   * @param out       decrypted string is written here
   *
   * @return    whether the string was successfully decrypted
   */
  bool decrypt(const char* in, int inLength, char* out);

  /**
   * should be called after all the decryption is done. After this call,
   * decryptor object can be reused.
   *
   * @return    whether the finish was successfully
   */
  bool finish(const std::string& tag);

  /// saves current cipher context
  bool saveContext();

  /// verify whether the given tag matches previously saved context
  bool verifyTag(const std::string& tag);

  AESDecryptor();
  /// destructor
  virtual ~AESDecryptor();

 private:
  static bool finishInternal(EVP_CIPHER_CTX* const ctx, EncryptionType type,
                             const std::string& tag);

  /// saved cipher ctx
  std::unique_ptr<EVP_CIPHER_CTX, CipherCtxDeleter> savedCtx_;

  /// whether ctx has been saved
  bool ctxSaved_{false};
};
}
}  // End of namespaces
