/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <string>
#include <wdt/ErrorCodes.h>

namespace facebook {
namespace wdt {

enum EncryptionType { ENC_NONE, ENC_AES128_CTR, NUM_ENC_TYPES };

class EncryptionParams {
 public:
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
  const std::string &getSecret() const {
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
  EncryptionParams(EncryptionType type, const std::string &data);

  /// Tests equality
  bool operator==(const EncryptionParams &that) const;

  /// Erase (clears the object - call as soon as you don't need secret anymore)
  void erase();
  /// Will also erase for safety against accidental memory recycling
  ~EncryptionParams();

  /// Reconstructs an EncryptionParams object from a previous getUrlSafeString()
  static ErrorCode unserialize(const std::string &urlSafeStr,
                               EncryptionParams &result);

 private:
  EncryptionType type_;
  std::string data_;
};
}
}  // End of namespaces
