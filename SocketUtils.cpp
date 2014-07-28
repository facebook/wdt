#include "SocketUtils.h"

#include <sys/types.h>
#include <sys/socket.h>

using namespace facebook::wdt;

/* static */
int SocketUtils::getReceiveBufferSize(int fd) {
  int size;
  socklen_t sizeSize = sizeof(size);
  getsockopt(fd, SOL_SOCKET, SO_RCVBUF, (void*)&size, &sizeSize);
  return size;
}
