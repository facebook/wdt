/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once
#include <wdt/ErrorCodes.h>
#include <wdt/Protocol.h>
#include <wdt/WdtOptions.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

namespace facebook {
namespace wdt {
/**
 * Users of wdt apis can provide a list of info
 * for files. A info represents a file name with
 * information such as size, and flags
 * to read the file with
 */
struct WdtFileInfo {
  /**
   * Name of the file to be read, generally as relative path
   */
  std::string fileName;
  /// Size of the file to be read, default is -1
  int64_t fileSize;
  /// File descriptor. If this is not -1, then wdt uses this to read
  int fd{-1};
  /// Whether read should be done using o_direct. If fd is set, this flag will
  /// be set automatically to match the fd open mode
  bool directReads{false};
  /// Constructor for file info with name, size and odirect request
  WdtFileInfo(const std::string& name, int64_t size, bool directReads);
  /**
   * Constructor with name, size and fd
   * If this constructor is used, then whether to do direct reads is decided
   * by fd flags.
   * Attempt to disambiguate the 2 constructors by having the fd first
   * and string last in this one.
   */
  WdtFileInfo(int fd, int64_t size, const std::string& name);
  /// Verify that we can align for reading in O_DIRECT and
  /// the flags make sense
  void verifyAndFixFlags();
};

/**
 * Basic Uri class to parse and get information from wdt url
 * This class can be used in two ways :
 * 1. Construct the class with a url and get fields like
 *    hostname, and different get parameters
 * 2. Construct an empty object and set the fields, and
 *    generate a url
 *
 * Example of a url :
 * wdt://localhost?dir=/tmp/wdt&ports=22356,22357
 */
class WdtUri {
 public:
  /// Empty Uri object
  WdtUri() = default;

  /// Construct the uri object using a string url
  explicit WdtUri(const std::string& url);

  /// Get the host name of the url
  std::string getHostName() const;

  /// Get the port number
  int32_t getPort() const;

  /// Get the query param by key
  std::string getQueryParam(const std::string& key) const;

  /// Get all the query params
  const std::map<std::string, std::string>& getQueryParams() const;

  /// Sets hostname to generate a url
  void setHostName(const std::string& hostName);

  /// Set the port for the uri
  void setPort(int32_t port);

  /// Sets a query param in the query params map
  void setQueryParam(const std::string& key, const std::string& value);

  /// Generate url by serializing the members of this struct
  std::string generateUrl() const;

  /// Url escape a value (which can be binary)
  static std::string escape(const std::string& binaryStr);
  /// Url unescape a value (escaped by escape()) - returns true if successful,
  /// false if there is a malformed % sequence in the string
  static bool unescape(std::string& result, folly::StringPiece escapedValue);
  /// 0-16 -> '0'-'f'
  static char toHex(unsigned char v);
  /// '0'-'f' -> 0-16
  static int fromHex(char c);

  /// Assignment operator to convert string to wdt uri object
  WdtUri& operator=(const std::string& url);

  /// Clears the field of the uri
  void clear();

  /// Get the error code if any during parsing
  ErrorCode getErrorCode() const;

 private:
  /**
   * Returns whether the url could be processed successfully. Populates
   * the values on a best effort basis.
   */
  ErrorCode process(const std::string& url);

  // TODO: use a vector instead, we don't really need to search...
  /**
   * Map of get parameters of the url. Key and value
   * of the map are the name and value of get parameter respectively
   */
  std::map<std::string, std::string> queryParams_;

  /// Prefix of the wdt url
  const std::string WDT_URL_PREFIX{"wdt://"};

  /// Hostname/ip address in the uri
  std::string hostName_{""};

  /// Port of the uri
  int32_t port_{-1};

  /// Error code that reflects that status of parsing url
  ErrorCode errorCode_{OK};
};

/**
 * Basic request for creating wdt objects
 * This request can be used for creating receivers and the
 * counter part sender or vice versa
 */
struct WdtTransferRequest {
  /**
   * Transfer Id for the transfer. It has to be same
   * on both sender and receiver
   */
  std::string transferId;

  /// Encryption protocol:sessionKey / secret (not printed), empty = clear text
  EncryptionParams encryptionData;

  /// Protocol version on sender and receiver
  int64_t protocolVersion{Protocol::protocol_version};

  /// Ports on which receiver is listening / sender is sending to
  std::vector<int32_t> ports;

  /// Address on which receiver binded the ports / sender is sending data to
  std::string hostName;

  /// Unique identifier for destination. This is useful to distinguish multiple
  /// destinations in the same host
  std::string destIdentifier;

  /// Namespace for the transfer
  std::string wdtNamespace;

  /// Directory to write the data to / read the data from
  std::string directory;

  /// Only used for the sender and when not using directory discovery
  std::vector<WdtFileInfo> fileInfo;

  /// Use fileInfo even if empty (don't use the directory exploring)
  bool disableDirectoryTraversal{false};

  // download resumption is enabled on the receiver side
  // and is requested from the sender
  bool downloadResumptionEnabled{false};

  /// TLS is enabled on the receiver side
  bool tls{false};

  /// Number of GBytes after iv is changed
  int64_t ivChangeInterval{0};

  /// Any error associated with this transfer request upon processing
  ErrorCode errorCode{OK};

  /// Empty constructor
  WdtTransferRequest() {
  }

  /**
   * Constructor with start port and num ports. Fills the vector with
   * ports from [startPort, startPort + numPorts)
   */
  WdtTransferRequest(int startPort, int numPorts, const std::string& directory);

  /// Constructor to construct the request object from a url string
  explicit WdtTransferRequest(const std::string& uriString);

  /// @return    generates wdt connection url and has encryption secret.
  ///            Returned secret should not be logged
  std::string genWdtUrlWithSecret() const;

  /// @return    returns a string describing this request. This string can be
  ///            logged
  std::string getLogSafeString() const;

  /// Serialize the ports into uri
  void serializePorts(WdtUri& wdtUri) const;

  /// Get stringified port list
  std::string getSerializedPortsList() const;

  /// Operator for finding if two request objects are equal
  bool operator==(const WdtTransferRequest& that) const;

  /// Overloaded operator for printing request info
  friend std::ostream& operator<<(std::ostream& os,
                                  const WdtTransferRequest& req);

  const static int LEGACY_PROTCOL_VERSION;

  /// Names of the get parameters for different fields
  const static std::string TRANSFER_ID_PARAM;
  /** Constant for for the protocol version get parameter in uri */
  const static std::string RECEIVER_PROTOCOL_VERSION_PARAM;
  const static std::string DIRECTORY_PARAM;
  const static std::string PORTS_PARAM;
  const static std::string START_PORT_PARAM;
  const static std::string NUM_PORTS_PARAM;
  /// Encryption parameters (proto:key for now, certificate,... potentially)
  const static std::string ENCRYPTION_PARAM;
  const static std::string TLS_PARAM;
  const static std::string NAMESPACE_PARAM;
  const static std::string DEST_IDENTIFIER_PARAM;
  const static std::string DOWNLOAD_RESUMPTION_PARAM;
  const static std::string IV_CHANGE_INTERVAL_PARAM;

  /// Get ports vector from startPort and numPorts
  static std::vector<int32_t> genPortsVector(int32_t startPort,
                                             int32_t numPorts);

 private:
  /**
   * Serialize this structure into a url string containing all fields
   * Will only put the real encoded secret if forLogging is set to false
   */
  std::string generateUrlInternal(bool genFull, bool forLogging) const;
};
}
}
