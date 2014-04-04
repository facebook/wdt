/*
 * Copyright 2014 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "wdtlib.h"

#include "ClientSocket.h"
#include "DirectorySourceQueue.h"
#include "ServerSocket.h"
#include "Protocol.h"

#include "folly/Conv.h"

#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>


using namespace facebook::wdt;

using std::string;

DEFINE_int32(backlog, 1, "Accept backlog");


size_t readAtLeast(int fd, char *buf, size_t max, size_t atLeast) {
  ssize_t len = 0;
  int count = 0;
  while (len < atLeast) {
    ssize_t n = read(fd, buf+len, max-len);
    if (n < 0) {
      PLOG(ERROR) << "Read error on " << fd << " after " << count;
      if (len) {
        return len;
      } else {
        return n;
      }
    }
    if (n == 0) {
      LOG(WARNING) << "Eof on " << fd << " after " << count << " read " << len;
      return len;
    }
    len += n;
    count++;
  }
  LOG(INFO) << "took " << count << " read to get " << len << " from " << fd;
  return len;
}

size_t readAtMost(int fd, char *buf, size_t max, size_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  ssize_t n = read(fd, buf, target);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << fd << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(ERROR) << "Eof on " << fd;
    return n;
  }
  LOG(INFO) << "readAtMost " << n << " from " << fd;
  return n;
}




void wdtServerOne(int port, int backlog, string destDirectory) {
  LOG(INFO) << "Server Thread for port " << port << " with backlog " << backlog
            << " on " << destDirectory;
  ServerSocket s(folly::to<string>(port), backlog);
  s.listen();
  while (true) {
    int fd = s.getNextFd();
    // test with sending bytes 1 by 1 and id len > 1024 (!)
    char buf[128*1024];
    ssize_t l;
    LOG(INFO) << "Reading from " << fd;
    while (true) {
      l = readAtLeast(fd, buf, sizeof(buf), 256);
      if (l <= 0) {
        break;
      }
      size_t off = 0;
      string id;
      int64_t size;
      bool success = Protocol::decode(buf, off, sizeof(buf),
                                      id, size);
      LOG(INFO) << "Read id:" << id << " size:" << size;
      // TODO: create directories...
      // TODO: check for weird id / properly concatenate destDir and id
      string path = folly::to<string>(destDirectory, id);
      int dest = open(path.c_str(), O_CREAT|O_WRONLY|O_TRUNC, 0644);
      if (dest == -1) {
        PLOG(ERROR) << "Unable to open " << id << " in " << destDirectory;
      }
      LOG(INFO) << "Opened " << path;
      // TODO: Doesn't work if more than 1 file fits in first read
      size_t toWrite = l - off;
      if (toWrite > size) {
        LOG(FATAL) << "Doesn't deal with tiny files yet " << toWrite
                   << " read vs " << size;
        toWrite = size;
      }
      // write rest of stuff
      int64_t wres = write(dest, buf + off, l - off);
      if (wres != l-off) {
        PLOG(ERROR) << "Write error/mismatch " << wres << " " << off << " " <<l;
      }
      while (wres < size) {
        int64_t nres = readAtMost(fd, buf, sizeof(buf), wres-size);
        if (nres <= 0) {
          break;
        }
        int64_t nwres = write(dest, buf, nres);
        if (nwres != nres) {
          PLOG(ERROR) << "Write error/mismatch " << nwres << " " << nres;
        }
        wres += nwres;
      }
      LOG(INFO) << "completed " << id << " " << dest;
      close(dest);
    }
    LOG(INFO) << "Done with " << fd;
    close(fd);
  }
}

void wdtServer(int port, int num_sockets, string destDirectory) {
  LOG(INFO) << "Starting (receiving) server on " << port
            << " with " << num_sockets << " target dir " << destDirectory;

  if (destDirectory.back() != '/') {
    destDirectory.push_back('/');
    LOG(VERBOSE) << "Added missing trailing / to " << destDirectory;
  }
  std::thread vt[num_sockets];
  for (int i=0; i < num_sockets; i++) {
    vt[i] = std::thread(wdtServerOne, port + i, FLAGS_backlog, destDirectory);
  }
  // will never exit
  for (int i=0; i < num_sockets; i++) {
    vt[i].join();
  }
}

void wdtClientOne(
  const string& destHost,
  int port,
  DirectorySourceQueue* queue
) {
  ClientSocket s(destHost, folly::to<string>(port));
  s.connect();
  int fd = s.getFd();
  char buf[1024];
  size_t off = 0;
  std::unique_ptr<ByteSource> source;
  while (source = queue->getNextSource()) {
    bool success = Protocol::encode(
      buf, off, sizeof(buf), source->getIdentifier(), source->getSize()
    );
    CHECK(success);
    ssize_t written = write(fd, buf, off);
    if (written != off) {
      PLOG(FATAL) << "Write error/mismatch " << written << " " << off;
    }
    while (!source->finished()) {
      size_t size;
      char* buffer = source->read(size);
      if (source->hasError()) {
        LOG(ERROR) << "failed reading file";
        break;
      }
      CHECK(buffer && size > 0);
      written = write(fd, buffer, size);
      LOG(INFO) << "wrote " << written << " on " << fd;
      if (written != size) {
        PLOG(FATAL) << "Write error/mismatch " << written << " " << size;
      }
    }
  }
}

void wdtClient(string destHost, int port, int num_sockets,
               string srcDirectory) {
  if (srcDirectory.back() != '/') {
    srcDirectory.push_back('/');
    LOG(VERBOSE) << "Added missing trailing / to " << srcDirectory;
  }
  LOG(INFO) << "Client (sending) to " << destHost << " port " << port
            << " with " << num_sockets << " source dir " << srcDirectory;
  DirectorySourceQueue queue(srcDirectory);
  std::thread vt[num_sockets];
  for (int i=0; i < num_sockets; i++) {
    vt[i] = std::thread(wdtClientOne, destHost, port + i, &queue);
  }
  queue.init();
  for (int i=0; i < num_sockets; i++) {
    vt[i].join();
  }
}
