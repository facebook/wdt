/**
 * Copyright (c) 2015-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/util/EncryptionUtils.h>

#include <string.h>  // for memset

namespace facebook {
namespace wdt {

using std::string;

// When we get more than 15 types we need to start encoding with more
// than 1 hex character, the decoding already support more than 1
static_assert(NUM_ENC_TYPES <= 16, "need to change encoding for types");

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
}
}  // end of namespaces
