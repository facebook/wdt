#pragma once
#include <string>

namespace facebook {
namespace wdt {

// For now just does regular check, for some library embedding may consider
// skipping or being DCHECK
#define WDT_CHECK CHECK

#define ERRORS                                                   \
  X(OK)                      /** No error  */                                         \
  X(CONN_ERROR)              /** Connection Error */                          \
  X(CONN_ERROR_RETRYABLE)    /** Retryable connection error  */     \
  X(SOCKET_READ_ERROR)       /** Socket read error  */                 \
  X(SOCKET_WRITE_ERROR)      /** Socket write error  */               \
  X(BYTE_SOURCE_READ_ERROR)  /** Byte source(file) read error  */ \
  X(MEMORY_ALLOCATION_ERROR) /** Memory allocation failure  */   \
  X(PROTOCOL_ERROR)          /** WDT protocol error  */

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
}
}
#undef ERRORS
