#pragma once
#include <string>

namespace facebook {
namespace wdt {

// For now just does regular check, for some library embedding may consider
// skipping or being DCHECK
#define WDT_CHECK CHECK

#define ERRORS                                                          \
  X(OK)                      /** No error  */                           \
  X(ERROR)                   /** Generic error  */                      \
  X(ABORT)                   /** Abort */                               \
  X(CONN_ERROR)              /** Connection Error */                    \
  X(CONN_ERROR_RETRYABLE)    /** Retryable connection error  */         \
  X(SOCKET_READ_ERROR)       /** Socket read error  */                  \
  X(SOCKET_WRITE_ERROR)      /** Socket write error  */                 \
  X(BYTE_SOURCE_READ_ERROR)  /** Byte source(file) read error  */       \
  X(FILE_WRITE_ERROR)        /** file write error  */                   \
  X(MEMORY_ALLOCATION_ERROR) /** Memory allocation failure  */          \
  X(PROTOCOL_ERROR)          /** WDT protocol error  */                 \
  X(VERSION_MISMATCH)        /** Sender and Receiver version mimatch */ \
  X(ID_MISMATCH)             /** Sender and Receiver id mismatch*/      \
  X(CHECKSUM_MISMATCH)       /** Checksums do not match */

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
 * Thread safe version of strerror(), easier than strerror_r
 * (similar to folly::errnoStr() but without pulling in all the dependencies)
 */
std::string strerrorStr(int errnum);
}
}
#undef ERRORS
