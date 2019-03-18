/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#pragma once

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <wdt/WdtConfig.h>
#include <iomanip>
#include <string>

#define WDT_DOUBLE_FORMATTING \
  std::fixed << std::setprecision(FLAGS_wdt_double_precision)

#define WDT_LOGGING_ENABLED FLAGS_wdt_logging_enabled

DECLARE_int32(wdt_double_precision);
DECLARE_bool(wdt_logging_enabled);

namespace facebook {
namespace wdt {

// Call regular google log but prefix with wdt for easier extraction later
#define WDT_LOG_PREFIX "wdt>\t"
#define WLOG(X) LOG_IF(X, WDT_LOGGING_ENABLED) << WDT_LOG_PREFIX << \
WDT_DOUBLE_FORMATTING
#define WVLOG(X) VLOG_IF(X, WDT_LOGGING_ENABLED) << WDT_LOG_PREFIX << \
WDT_DOUBLE_FORMATTING
#define WPLOG(X) PLOG_IF(X, WDT_LOGGING_ENABLED) << WDT_LOG_PREFIX << \
WDT_DOUBLE_FORMATTING
#define WLOG_IF(X, Y) LOG_IF(X, WDT_LOGGING_ENABLED && (Y)) << WDT_LOG_PREFIX \
<< WDT_DOUBLE_FORMATTING
#define WVLOG_IF(X, Y) VLOG_IF(X, WDT_LOGGING_ENABLED && (Y)) << \
WDT_LOG_PREFIX << WDT_DOUBLE_FORMATTING
#define WTLOG(X) WLOG(X) << *this << " "
#define WTVLOG(X) WVLOG(X) << *this << " "
#define WTPLOG(X) WPLOG(X) << *this << " "

// For now just does regular check, for some library embedding may consider
// skipping or being DCHECK
#define WDT_CHECK CHECK
#define WDT_CHECK_EQ CHECK_EQ
#define WDT_CHECK_NE CHECK_NE
#define WDT_CHECK_LE CHECK_LE
#define WDT_CHECK_LT CHECK_LT
#define WDT_CHECK_GE CHECK_GE
#define WDT_CHECK_GT CHECK_GT
// Note : All the new errors should be defined at the end - but see also
// getMoreInterestingError implementation for error priorities for reporting
#define ERRORS                                                                 \
  X(OK)                      /** No error  */                                  \
  X(ERROR)                   /** Generic error  */                             \
  X(ABORT)                   /** Abort */                                      \
  X(CONN_ERROR)              /** Connection Error */                           \
  X(CONN_ERROR_RETRYABLE)    /** Retryable connection error  */                \
  X(SOCKET_READ_ERROR)       /** Socket read error  */                         \
  X(SOCKET_WRITE_ERROR)      /** Socket write error  */                        \
  X(BYTE_SOURCE_READ_ERROR)  /** Byte source(file) read error  */              \
  X(FILE_WRITE_ERROR)        /** file write error  */                          \
  X(MEMORY_ALLOCATION_ERROR) /** Memory allocation failure  */                 \
  X(PROTOCOL_ERROR)          /** WDT protocol error  */                        \
  X(VERSION_MISMATCH)        /** Sender and Receiver version mismatch */       \
  X(ID_MISMATCH)             /** Sender and Receiver id mismatch*/             \
  X(CHECKSUM_MISMATCH)       /** Checksums do not match */                     \
  X(RESOURCE_NOT_FOUND)      /** Not found in the resource controller */       \
  X(ABORTED_BY_APPLICATION)  /** Transfer was aborted by application */        \
  X(VERSION_INCOMPATIBLE)    /** Sender/receiver version incompatible */       \
  X(NOT_FOUND)               /** Not found in the resource controller */       \
  X(QUOTA_EXCEEDED)          /** Quota exceeded in resource controller */      \
  X(FEWER_PORTS)             /** Couldn't listen on all the ports */           \
  X(URI_PARSE_ERROR)         /** Wdt uri passed couldn't be parsed */          \
  X(INCONSISTENT_DIRECTORY)  /** Destination directory is not consistent with  \
                                transfer log */                                \
  X(INVALID_LOG)             /** Transfer log invalid */                       \
  X(INVALID_CHECKPOINT)      /** Received checkpoint is invalid */             \
  X(NO_PROGRESS)             /** Transfer has not progressed */                \
  X(TRANSFER_LOG_ACQUIRE_ERROR) /** Failed to acquire lock for transfer log */ \
  X(WDT_TIMEOUT)                /** Socket operation timed out  */             \
  X(UNEXPECTED_CMD_ERROR)       /** Unexpected cmd received */                 \
  X(ENCRYPTION_ERROR)           /** Error related to encryption */             \
  X(ALREADY_EXISTS)             /** Create attempt for existing id */          \
  X(GLOBAL_CHECKPOINT_ABORT)    /** Abort due to global checkpoint */          \
  X(INVALID_REQUEST) /** Request for creation of wdt object invalid */         \
  X(SENDER_START_TIMED_OUT) /** Sender start timed out */

enum ErrorCode {
#define X(A) A,
  ERRORS
#undef X
};

std::string const kErrorToStr[] = {
#define X(A) #A,
    ERRORS
#undef X
};

/**
 * returns string description of an error code
 *
 * @param code  error-code
 * @return      string representation
 */
std::string errorCodeToStr(ErrorCode code);

/**
 * returns more interesting of two errors
 */
ErrorCode getMoreInterestingError(ErrorCode err1, ErrorCode err2);

/**
 * Thread safe version of strerror(), easier than strerror_r
 * (similar to folly::errnoStr() but without pulling in all the dependencies)
 */
std::string strerrorStr(int errnum);
}
}
#undef ERRORS
