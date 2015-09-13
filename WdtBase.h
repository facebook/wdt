/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include "ErrorCodes.h"
#include "AbortChecker.h"
#include "DirectorySourceQueue.h"
#include "WdtOptions.h"
#include "Reporting.h"
#include "Throttler.h"
#include "Protocol.h"
#include <memory>
#include <string>
#include <vector>
#include <folly/RWSpinLock.h>
#include <unordered_map>
namespace facebook {
namespace wdt {

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

  /// Get the query param by key
  std::string getQueryParam(const std::string& key) const;

  /// Get all the query params
  const std::unordered_map<std::string, std::string>& getQueryParams() const;

  /// Sets hostname to generate a url
  void setHostName(const std::string& hostName);

  /// Sets a query param in the query params map
  void setQueryParam(const std::string& key, const std::string& value);

  /// Generate url by serializing the members of this struct
  std::string generateUrl() const;

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

  /**
   * Map of get parameters of the url. Key and value
   * of the map are the name and value of get parameter respectively
   */
  std::unordered_map<std::string, std::string> queryParams_;

  /// Prefix of the wdt url
  const std::string WDT_URL_PREFIX{"wdt://"};

  /// Hostname where the receiver is running
  std::string hostName_{""};

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

  /// Protocol version on sender and receiver
  int64_t protocolVersion{Protocol::protocol_version};

  /// Ports on which receiver is listening / sender is sending to
  std::vector<int32_t> ports;

  /// Address on which receiver binded the ports / sender is sending data to
  std::string hostName;

  /// Directory to write the data to / read the data from
  std::string directory;

  /// Only required for the sender
  std::vector<FileInfo> fileInfo;

  /// Any error associated with this transfer request upon processing
  ErrorCode errorCode{OK};

  /// Constructor with list of ports
  explicit WdtTransferRequest(const std::vector<int32_t>& ports);

  /**
   * Constructor with start port and num ports. Fills the vector with
   * ports from [startPort, startPort + numPorts)
   */
  WdtTransferRequest(int startPort, int numPorts, const std::string& directory);

  /// Constructor to construct the request object from a url string
  explicit WdtTransferRequest(const std::string& uriString);

  /// Serialize this structure into a url string containing all fields
  std::string generateUrl(bool genFull = false) const;

  /// Serialize the ports into uri
  void serializePorts(WdtUri& wdtUri) const;

  /// Get stringified port list
  std::string getSerializedPortsList() const;

  /// Operator for finding if two request objects are equal
  bool operator==(const WdtTransferRequest& that) const;

  /// Names of the get parameters for different fields
  const static std::string TRANSFER_ID_PARAM;
  const static std::string PROTOCOL_VERSION_PARAM;
  const static std::string DIRECTORY_PARAM;
  const static std::string PORTS_PARAM;
  const static std::string START_PORT_PARAM;
  const static std::string NUM_PORTS_PARAM;
};

/**
 * Shared code/functionality between Receiver and Sender
 * TODO: a lot more code from sender/receiver should move here
 */
class WdtBase {
 public:
  /// Constructor
  WdtBase();

  /**
   * Does the setup before start, returns the transfer request
   * that corresponds to the information relating to the sender
   * The transfer request has error code set should there be an error
   */
  virtual WdtTransferRequest init() = 0;

  /// Destructor
  virtual ~WdtBase();

  /// Transfer can be marked to abort and threads will eventually
  /// get aborted after this method has been called based on
  /// whether they are doing read/write on the socket and the timeout for the
  /// socket. Push mode for abort.
  void abort(const ErrorCode abortCode);

  /// clears abort flag
  void clearAbort();

  /**
   * sets an extra external call back to check for abort
   * can be for instance extending IAbortChecker with
   * bool checkAbort() {return atomicBool->load();}
   * see wdtCmdLine.cpp for an example.
   */
  void setAbortChecker(const std::shared_ptr<IAbortChecker>& checker);

  /// threads can call this method to find out
  /// whether transfer has been marked from abort
  ErrorCode getCurAbortCode();

  /// Wdt objects can report progress. Setter for progress reporter
  /// defined in Reporting.h
  void setProgressReporter(std::unique_ptr<ProgressReporter>& progressReporter);

  /// Set throttler externally. Should be set before any transfer calls
  void setThrottler(std::shared_ptr<Throttler> throttler);

  /// Sets the transferId for this transfer
  void setTransferId(const std::string& transferId);

  /// Sets the protocol version for the transfer
  void setProtocolVersion(int64_t protocolVersion);

  /// Get the transfer id of the object
  std::string getTransferId();

  /// Finishes the wdt object and returns a report
  virtual std::unique_ptr<TransferReport> finish() = 0;

  /// Method to transfer the data. Doesn't block and
  /// returns with the status
  virtual ErrorCode transferAsync() = 0;

  /// Basic setup for throttler using options
  void configureThrottler();

  /// Utility to generate a random transfer id
  static std::string generateTransferId();

 protected:
  /// Global throttler across all threads
  std::shared_ptr<Throttler> throttler_;

  /// Holds the instance of the progress reporter default or customized
  std::unique_ptr<ProgressReporter> progressReporter_;

  /// Unique id for the transfer
  std::string transferId_;

  /// protocol version to use, this is determined by negotiating protocol
  /// version with the other side
  int protocolVersion_{Protocol::protocol_version};

  /// abort checker class passed to socket functions
  class AbortChecker : public IAbortChecker {
   public:
    explicit AbortChecker(WdtBase* wdtBase) : wdtBase_(wdtBase) {
    }

    bool shouldAbort() const {
      return wdtBase_->getCurAbortCode() != OK;
    }

   private:
    WdtBase* wdtBase_;
  };

  /// abort checker passed to socket functions
  AbortChecker abortCheckerCallback_;

 private:
  folly::RWSpinLock abortCodeLock_;
  /// Internal and default abort code
  ErrorCode abortCode_{OK};
  /// Additional external source of check for abort requested
  std::shared_ptr<IAbortChecker> abortChecker_{nullptr};
};
}
}  // namespace facebook::wdt
