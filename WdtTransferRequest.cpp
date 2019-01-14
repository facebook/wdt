/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtTransferRequest.h>
#include <folly/Conv.h>
#include <folly/Range.h>
#include <ctime>

using namespace std;
using folly::StringPiece;

namespace facebook {
namespace wdt {

WdtUri::WdtUri(const string& url) {
  errorCode_ = process(url);
}

void WdtUri::setHostName(const string& hostName) {
  hostName_ = hostName;
}

void WdtUri::setPort(int32_t port) {
  port_ = port;
}

void WdtUri::setQueryParam(const string& key, const string& value) {
  queryParams_[key] = value;
}

ErrorCode WdtUri::getErrorCode() const {
  return errorCode_;
}

/* static */
char WdtUri::toHex(unsigned char v) {
  WDT_CHECK_LT(v, 16);
  if (v <= 9) {
    return '0' + v;
  }
  return 'a' + v - 10;
}
/* static */
int WdtUri::fromHex(char c) {
  if (c < '0' || (c > '9' && (c < 'a' || c > 'f'))) {
    return -1;  // invalid not 0-9a-f hex char
  }
  if (c <= '9') {
    return c - '0';
  }
  return c - 'a' + 10;
}

/* static */
string WdtUri::escape(const string& binaryStr) {
  string res;
  res.reserve(binaryStr.length());  // most time nothing to escape
  for (unsigned char c : binaryStr) {
    // Allow 0-9 A-Z a-z (alphanum) and , : . _ + - (note that : is arguable)
    // (and we could use an array lookup instead of a bunch of ||s)
    if (c == ',' || c == ':' || c == '.' || c == '_' || c == '+' || c == '-' ||
        (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
        (c >= '0' && c <= '9')) {
      res.push_back(c);
    } else {
      res.push_back('%');
      res.push_back(toHex(c >> 4));
      res.push_back(toHex(c & 0xf));
    }
  }
  return res;
}

/* static */
bool WdtUri::unescape(string& res, StringPiece escapedValue) {
  res.reserve(res.length() + escapedValue.size());
  for (size_t i = 0; i < escapedValue.size(); ++i) {
    char c = escapedValue[i];
    if (c != '%') {
      res.push_back(c);
      continue;
    }
    i += 2;
    // Make sure there room for both hex
    if (i >= escapedValue.size()) {
      WLOG(ERROR) << "Can't decode \"" << escapedValue
                  << "\" end with unfinished % sequence";
      return false;
    }
    const int msb = fromHex(escapedValue[i - 1]);
    const int lsb = fromHex(escapedValue[i]);
    if (msb < 0 || lsb < 0) {
      WLOG(ERROR) << "Can't decode \"" << escapedValue
                  << "\" % sequence with non 0-9a-f characters";
      return false;
    }
    res.push_back(msb << 4 | lsb);
  }
  return true;
}

string WdtUri::generateUrl() const {
  string url = WDT_URL_PREFIX;
  if (hostName_.find(':') != string::npos) {
    // Enclosing ipv6 address by [] so that it can be escaped
    folly::toAppend('[', hostName_, ']', &url);
  } else {
    folly::toAppend(hostName_, &url);
  }
  if (port_ > 0) {
    // Must have positive port value
    folly::toAppend(":", port_, &url);
  }
  char prefix = '?';
  for (const auto& pair : queryParams_) {
    if (!pair.second.empty()) {
      string value = WdtUri::escape(pair.second);
      folly::toAppend(prefix, pair.first, "=", value, &url);
      prefix = '&';
    }
  }
  return url;
}

ErrorCode WdtUri::process(const string& url) {
  if (url.size() < WDT_URL_PREFIX.size()) {
    WLOG(ERROR) << "Url doesn't specify wdt protocol";
    return URI_PARSE_ERROR;
  }
  StringPiece urlPiece(url, 0, WDT_URL_PREFIX.size());
  StringPiece wdtPrefix(WDT_URL_PREFIX);
  if (urlPiece != wdtPrefix) {
    WLOG(ERROR) << "Url does not specify wdt protocol " << url;
    return URI_PARSE_ERROR;
  }
  urlPiece = StringPiece(url, WDT_URL_PREFIX.size());
  if (urlPiece.empty()) {
    WLOG(ERROR) << "Empty host name " << url;
    return URI_PARSE_ERROR;
  }
  ErrorCode status = OK;
  // Parse hostname
  if (urlPiece[0] == '[') {
    urlPiece.advance(1);
    size_t hostNameEnd = urlPiece.find(']');
    if (hostNameEnd == string::npos) {
      WLOG(ERROR) << "Didn't find ] for ipv6 address " << url;
      return URI_PARSE_ERROR;
    }
    hostName_.assign(urlPiece.data(), 0, hostNameEnd);
    urlPiece.advance(hostNameEnd + 1);
  } else {
    size_t urlIndex = 0;
    for (; urlIndex < urlPiece.size(); ++urlIndex) {
      if (urlPiece[urlIndex] == ':') {
        break;
      }
      if (urlPiece[urlIndex] == '?') {
        break;
      }
    }
    hostName_.assign(urlPiece.data(), 0, urlIndex);
    urlPiece.advance(urlIndex);
  }

  if (hostName_.empty()) {
    status = URI_PARSE_ERROR;
    WLOG(ERROR) << "Empty hostname " << url;
  }

  if (urlPiece.empty()) {
    return status;
  }
  // TODO: allow for '/' like wdt://[::1]:1234/?num_ports=3
  // parse port number
  if (urlPiece[0] == ':') {
    urlPiece.advance(1);
    size_t paramsIndex = urlPiece.find('?');
    if (paramsIndex == string::npos) {
      paramsIndex = urlPiece.size();
    }
    try {
      string portStr;
      portStr.assign(urlPiece.data(), 0, paramsIndex);
      port_ = folly::to<int32_t>(portStr);
    } catch (std::exception& e) {
      WLOG(ERROR) << "Invalid port, can't be parsed " << url;
      status = URI_PARSE_ERROR;
    }
    urlPiece.advance(paramsIndex);
  }

  if (urlPiece.empty()) {
    return status;
  }

  if (urlPiece[0] != '?') {
    WLOG(ERROR) << "Unexpected delimiter for params " << urlPiece[0];
    return URI_PARSE_ERROR;
  }
  urlPiece.advance(1);
  // parse params
  while (!urlPiece.empty()) {
    StringPiece keyValuePair = urlPiece.split_step('&');
    if (keyValuePair.empty()) {
      // Last key value pair
      keyValuePair = urlPiece;
      urlPiece.advance(urlPiece.size());
    }
    StringPiece key = keyValuePair.split_step('=');
    StringPiece value = keyValuePair;
    if (key.empty()) {
      // Value can be empty but key can't be empty
      WLOG(ERROR) << "Errors parsing params, url = " << url;
      status = URI_PARSE_ERROR;
      break;
    }
    string unescapedValue;
    if (!unescape(unescapedValue, value)) {
      status = URI_PARSE_ERROR;
    }
    queryParams_[key.toString()] = unescapedValue;
  }
  return status;
}

string WdtUri::getHostName() const {
  return hostName_;
}

int32_t WdtUri::getPort() const {
  return port_;
}

string WdtUri::getQueryParam(const string& key) const {
  auto it = queryParams_.find(key);
  if (it == queryParams_.end()) {
    WVLOG(1) << "Couldn't find query param " << key;
    return "";
  }
  return it->second;
}

const map<string, string>& WdtUri::getQueryParams() const {
  return queryParams_;
}

void WdtUri::clear() {
  hostName_.clear();
  port_ = -1;
  queryParams_.clear();
}

WdtUri& WdtUri::operator=(const string& url) {
  clear();
  errorCode_ = process(url);
  return *this;
}

/* static */
std::vector<int32_t> WdtTransferRequest::genPortsVector(int32_t startPort,
                                                        int32_t numPorts) {
  std::vector<int32_t> ports;
  for (int32_t i = 0; i < numPorts; i++) {
    ports.push_back(startPort + i);
  }
  return ports;
}

const string WdtTransferRequest::TRANSFER_ID_PARAM{"id"};
/** RECeiver's Protocol Version */
const string WdtTransferRequest::RECEIVER_PROTOCOL_VERSION_PARAM{"recpv"};
const string WdtTransferRequest::DIRECTORY_PARAM{"dir"};
const string WdtTransferRequest::PORTS_PARAM{"ports"};
const string WdtTransferRequest::START_PORT_PARAM{"start_port"};
const string WdtTransferRequest::NUM_PORTS_PARAM{"num_ports"};
const string WdtTransferRequest::ENCRYPTION_PARAM{"Enc"};
const string WdtTransferRequest::TLS_PARAM{"tls"};
const string WdtTransferRequest::NAMESPACE_PARAM{"ns"};
const string WdtTransferRequest::DEST_IDENTIFIER_PARAM{"dstid"};
const string WdtTransferRequest::DOWNLOAD_RESUMPTION_PARAM{"dr"};
const string WdtTransferRequest::IV_CHANGE_INTERVAL_PARAM{"iv_change_int"};

WdtTransferRequest::WdtTransferRequest(int startPort, int numPorts,
                                       const string& directory) {
  this->directory = directory;
  int portNum = startPort;
  for (int i = 0; i < numPorts; i++) {
    ports.push_back(portNum);
    if (startPort) {
      ++portNum;
    }
  }
}

WdtTransferRequest::WdtTransferRequest(const string& uriString) {
  WdtUri wdtUri(uriString);
  errorCode = wdtUri.getErrorCode();
  hostName = wdtUri.getHostName();
  transferId = wdtUri.getQueryParam(TRANSFER_ID_PARAM);
  wdtNamespace = wdtUri.getQueryParam(NAMESPACE_PARAM);
  destIdentifier = wdtUri.getQueryParam(DEST_IDENTIFIER_PARAM);
  directory = wdtUri.getQueryParam(DIRECTORY_PARAM);
  string encStr = wdtUri.getQueryParam(ENCRYPTION_PARAM);
  if (!encStr.empty()) {
    ErrorCode code = EncryptionParams::unserialize(encStr, encryptionData);
    if (code != OK) {
      WLOG(ERROR) << "Unable to parse encryption data from \"" << encStr
                  << "\" " << errorCodeToStr(code);
      errorCode = getMoreInterestingError(code, errorCode);
    }
  }

  string tlsEnabled = wdtUri.getQueryParam(TLS_PARAM);
  try {
    if (!tlsEnabled.empty()) {
      tls = folly::to<bool>(tlsEnabled);
    }
  } catch (std::exception& e) {
    WLOG(ERROR) << "Error parsing tls " << tlsEnabled << " " << e.what();
    errorCode = URI_PARSE_ERROR;
  }

  string downloadResume = wdtUri.getQueryParam(DOWNLOAD_RESUMPTION_PARAM);
  try {
    if (!downloadResume.empty()) {
      downloadResumptionEnabled = folly::to<bool>(downloadResume);
    }
  } catch (std::exception& e) {
    WLOG(ERROR) << "Error parsing download resume " << downloadResume << " "
                << e.what();
    errorCode = URI_PARSE_ERROR;
  }

  const string recpv = wdtUri.getQueryParam(RECEIVER_PROTOCOL_VERSION_PARAM);
  if (recpv.empty()) {
    WLOG(WARNING) << RECEIVER_PROTOCOL_VERSION_PARAM << " not specified in URI";
  } else {
    try {
      protocolVersion = folly::to<int64_t>(recpv);
    } catch (std::exception& e) {
      WLOG(ERROR) << "Error parsing protocol version "
                  << wdtUri.getQueryParam(RECEIVER_PROTOCOL_VERSION_PARAM)
                  << " " << e.what();
      errorCode = URI_PARSE_ERROR;
    }
  }

  const string ivChangeIntervalStr =
      wdtUri.getQueryParam(IV_CHANGE_INTERVAL_PARAM);
  if (ivChangeIntervalStr.empty()) {
    WLOG(WARNING) << IV_CHANGE_INTERVAL_PARAM << " not specified in URI";
  } else {
    try {
      ivChangeInterval = folly::to<int64_t>(ivChangeIntervalStr);
    } catch (std::exception& e) {
      WLOG(ERROR) << "Error parsing iv change interval " << ivChangeIntervalStr
                  << " " << e.what();
      errorCode = URI_PARSE_ERROR;
    }
  }

  string portsStr(wdtUri.getQueryParam(PORTS_PARAM));
  StringPiece portsList(portsStr);  // pointers into portsStr
  do {
    StringPiece portNum = portsList.split_step(',');
    int port;
    if (!portNum.empty()) {
      try {
        port = folly::to<int32_t>(portNum);
        ports.push_back(port);
      } catch (std::exception& e) {
        WLOG(ERROR) << "Couldn't convert " << portNum
                    << " to valid port number";
        errorCode = URI_PARSE_ERROR;
      }
    }
  } while (!portsList.empty());
  if (!ports.empty()) {
    // Done with ports - rest of the function is alternative way to set ports
    return;
  }
  // Figure out ports using other params only if there was no port list
  string startPortStr = wdtUri.getQueryParam(START_PORT_PARAM);
  string numPortsStr = wdtUri.getQueryParam(NUM_PORTS_PARAM);
  int32_t startPort = wdtUri.getPort();
  if (startPort <= 0) {
    if (startPortStr.empty()) {
      WLOG(ERROR) << "URI should have port or " << START_PORT_PARAM;
      errorCode = INVALID_REQUEST;
    } else {
      try {
        startPort = folly::to<int32_t>(startPortStr);
      } catch (std::exception& e) {
        WLOG(ERROR) << "Couldn't convert start port " << startPortStr;
        errorCode = URI_PARSE_ERROR;
      }
    }
  }
  int32_t numPorts = 0;
  if (numPortsStr.empty()) {
    WLOG(ERROR) << "URI should have " << NUM_PORTS_PARAM;
    errorCode = INVALID_REQUEST;
  } else {
    try {
      numPorts = folly::to<int32_t>(numPortsStr);
    } catch (std::exception& e) {
      WLOG(ERROR) << "Couldn't convert num ports " << numPortsStr;
      errorCode = URI_PARSE_ERROR;
    }
  }
  if (errorCode == OK) {
    ports = WdtTransferRequest::genPortsVector(startPort, numPorts);
  }
  // beware of the return above, add future params processing above the return
}

string WdtTransferRequest::genWdtUrlWithSecret() const {
  return generateUrlInternal(false, false);
}

string WdtTransferRequest::getLogSafeString() const {
  return generateUrlInternal(true, true);
}

string WdtTransferRequest::generateUrlInternal(bool genFull,
                                               bool forLogging) const {
  if (errorCode != OK) {
    const string msg = errorCodeToStr(errorCode);
    WLOG(ERROR) << "Transfer request has " << msg;
    return msg;
  }
  WdtUri wdtUri;
  wdtUri.setHostName(hostName);
  wdtUri.setQueryParam(TRANSFER_ID_PARAM, transferId);
  wdtUri.setQueryParam(NAMESPACE_PARAM, wdtNamespace);
  wdtUri.setQueryParam(DEST_IDENTIFIER_PARAM, destIdentifier);
  wdtUri.setQueryParam(RECEIVER_PROTOCOL_VERSION_PARAM,
                       folly::to<string>(protocolVersion));
  wdtUri.setQueryParam(IV_CHANGE_INTERVAL_PARAM,
                       folly::to<string>(ivChangeInterval));
  if (downloadResumptionEnabled) {
    wdtUri.setQueryParam(DOWNLOAD_RESUMPTION_PARAM,
                         folly::to<string>(downloadResumptionEnabled));
  }
  serializePorts(wdtUri);
  if (genFull) {
    wdtUri.setQueryParam(DIRECTORY_PARAM, directory);
  }
  if (encryptionData.isSet()) {
    WVLOG(1) << "Encryption data is set " << encryptionData.getLogSafeString();
    wdtUri.setQueryParam(ENCRYPTION_PARAM,
                         forLogging ? encryptionData.getLogSafeString()
                                    : encryptionData.getUrlSafeString());
  }
  wdtUri.setQueryParam(TLS_PARAM, folly::to<string>(tls));
  return wdtUri.generateUrl();
}

void WdtTransferRequest::serializePorts(WdtUri& wdtUri) const {
  // Serialize to a port list if the ports are not
  // contigous sequence else wdt://hostname:port
  if (ports.size() == 0) {
    return;
  }
  int32_t prevPort = ports[0];
  bool hasHoles = false;
  for (size_t i = 1; i < ports.size(); i++) {
    if (ports[i] != prevPort + 1) {
      hasHoles = true;
      break;
    }
    prevPort = ports[i];
  }
  if (hasHoles) {
    wdtUri.setQueryParam(PORTS_PARAM, getSerializedPortsList());
  } else {
    wdtUri.setPort(ports[0]);
    wdtUri.setQueryParam(NUM_PORTS_PARAM, folly::to<string>(ports.size()));
  }
}

string WdtTransferRequest::getSerializedPortsList() const {
  string portsList = "";
  for (size_t i = 0; i < ports.size(); i++) {
    if (i != 0) {
      folly::toAppend(",", &portsList);
    }
    auto port = ports[i];
    folly::toAppend(port, &portsList);
  }
  return portsList;
}

bool WdtTransferRequest::operator==(const WdtTransferRequest& that) const {
  bool result = (transferId == that.transferId) &&
                (protocolVersion == that.protocolVersion) &&
                (directory == that.directory) && (hostName == that.hostName) &&
                (ports == that.ports) &&
                (encryptionData == that.encryptionData) &&
                (destIdentifier == that.destIdentifier) &&
                (wdtNamespace == that.wdtNamespace);
  // No need to check the file info, simply checking whether two objects
  // are same with respect to the wdt settings
  return result;
}

std::ostream& operator<<(std::ostream& os, const WdtTransferRequest& req) {
  // getLogSafeString() returns a url string containing all fields. So should be
  // enough for logging purpose.
  os << "WdtTransferRequest represented by url: " << req.getLogSafeString();
  return os;
}
}
}
