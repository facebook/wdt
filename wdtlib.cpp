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
#include "FileCreator.h"
#include "ServerSocket.h"
#include "Protocol.h"

#include "folly/Conv.h"
#include "folly/String.h"

#include <iostream>
#include <thread>

#include <gflags/gflags.h>
#include <glog/logging.h>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <limits.h>
#include <chrono>

using namespace facebook::wdt;

using std::string;

/// Max size of filename + 2 max varints
size_t kMaxHeader = PATH_MAX + 10 + 10;


DEFINE_int32(backlog, 1, "Accept backlog");
// 256k is fastest for test on localhost and shm : > 5 Gbytes/sec
DEFINE_int32(buffer_size, 256*1024, "Buffer size (per thread/socket)");
DEFINE_int32(max_retries, 20, "how many attempts to connect/listen");
DEFINE_int32(sleep_ms, 50, "how many ms to wait between attempts");


size_t kBufferSize;

std::unique_ptr<FileCreator> gFileCreator;

/// Both version, magic number and command byte
enum CMD_MAGIC {
  FILE_CMD = 0x44,
  DONE_CMD = 0x4C
};


/// len is initial/already read len
size_t readAtLeast(int fd, char *buf, size_t max, ssize_t atLeast,
                   ssize_t len=0) {
  LOG(VERBOSE) << "readAtLeast len " << len
               << " max " << max
               << " atLeast " << atLeast
               << " from " << fd;
  CHECK(len >= 0)     << "negative len " << len;
  CHECK(atLeast >= 0) << "negative atLeast " << atLeast;
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
      LOG(VERBOSE) << "Eof on " << fd << " after " << count << " read " << len;
      return len;
    }
    len += n;
    count++;
  }
  LOG(VERBOSE) << "took " << count << " read to get " << len << " from " << fd;
  return len;
}

size_t readAtMost(int fd, char *buf, size_t max, size_t atMost) {
  const int64_t target = atMost < max ? atMost : max;
  LOG(VERBOSE) << "readAtMost target " << target;
  ssize_t n = read(fd, buf, target);
  if (n < 0) {
    PLOG(ERROR) << "Read error on " << fd << " with target " << target;
    return n;
  }
  if (n == 0) {
    LOG(WARNING) << "Eof on " << fd;
    return n;
  }
  LOG(VERBOSE) << "readAtMost " << n << " / " << atMost << " from " << fd;
  return n;
}


void wdtServerOne(int port, int backlog, string destDirectory) {
  LOG(INFO) << "Server Thread for port " << port << " with backlog " << backlog
            << " on " << destDirectory;

  ServerSocket s(folly::to<string>(port), backlog);
  for (int i = 1; i < FLAGS_max_retries; ++i) {
    if (s.listen()) {
      break;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(FLAGS_sleep_ms * 1000);
  }
  // one more/last try (stays true if it worked above)
  CHECK(s.listen()) << "Unable to listen/bind despite retries";
  char *buf = (char *)malloc(kBufferSize);
  CHECK(buf) << "error allocating " << kBufferSize;
  while (true) {
    int fd = s.getNextFd();
    // TODO test with sending bytes 1 by 1 and id len at max
    ssize_t numRead = 0;
    size_t off = 0;
    LOG(VERBOSE) << "Reading from " << fd;
    while (true) {
      numRead = readAtLeast(fd, buf + off, kBufferSize - off,
                            kMaxHeader, numRead);
      if (numRead <= 0) {
        break;
      }
      string id;
      int64_t size;
      const ssize_t oldOffset = off;
      enum CMD_MAGIC cmd = (enum CMD_MAGIC)buf[off++];
      if (cmd == DONE_CMD) {
        LOG(INFO) << "Got done command for " << fd;
        CHECK(numRead == 1) << "Unexpected state for done command"
                            << " off: " << off
                            << " numRead: " << numRead;
        write(fd, buf+off-1, 1);
        break;
      }
      CHECK(cmd == FILE_CMD) << "Unexpected magic/cmd byte " << cmd;
      bool success = Protocol::decode(buf, off, numRead + oldOffset,
                                      id, size);
      CHECK(success) << "Error decoding at"
                     << " ooff:" << oldOffset << " off: " << off
                     << " numRead: " << numRead;
      LOG(INFO) << "Read id:" << id << " size:" << size
                << " ooff:" << oldOffset << " off: " << off
                << " numRead: " << numRead;
      int dest = gFileCreator->createFile(id);
      if (dest == -1) {
        LOG(ERROR) << "Unable to open " << id << " in " << destDirectory;
      }
      ssize_t remainingData = numRead + oldOffset - off;
      ssize_t toWrite = remainingData;
      if (remainingData >= size) {
        toWrite = size;
      }
      // write rest of stuff
      int64_t wres = write(dest, buf + off, toWrite);
      if (wres != toWrite) {
        PLOG(ERROR) << "Write error/mismatch " << wres
                    << " " << off << " " << toWrite;
      } else {
        LOG(VERBOSE) << "Wrote intial " << wres << " / " << size
                     << " off: " << off
                     << " numRead: " << numRead
                     << " on " << dest;
      }
      off += wres;
      remainingData -= wres;
      // also means no leftOver so it's ok we use buf from start
      while (wres < size) {
        int64_t nres = readAtMost(fd, buf, kBufferSize, size-wres);
        if (nres <= 0) {
          break;
        }
        int64_t nwres = write(dest, buf, nres);
        if (nwres != nres) {
          PLOG(ERROR) << "Write error/mismatch " << nwres << " " << nres;
        }
        wres += nwres;
      }
      LOG(INFO) << "completed " << id
                     << " off: " << off
                     << " numRead: " << numRead
                     << " on " << dest;
      close(dest);
      CHECK(remainingData >= 0) "Negative remainingData " << remainingData;
      if (remainingData > 0) {
        // if we need to read more anyway, let's move the data
        numRead = remainingData;
        if ((remainingData < kMaxHeader) && (off > (kBufferSize/2))) {
          // rare so inneficient is ok
          LOG(VERBOSE) << "copying extra " << remainingData
                       << " leftover bytes @ " << off;
          memmove(/* dst      */ buf,
                  /* from     */ buf+off,
                  /* how much */ remainingData);
          off = 0;
        } else {
          // otherwise just change the offset
          LOG(VERBOSE) << "will use remaining extra " << remainingData
                       << " leftover bytes @ " << off;
        }
      } else {
        numRead = off = 0;
      }
    }
    LOG(INFO) << "Done with " << fd;
    close(fd);
  }
  free(buf);
}

void wdtServer(int port, int num_sockets, string destDirectory) {
  // TODO: do that automatically ? why do I have to call this...
  FileCreator::addTrailingSlash(destDirectory);
  LOG(INFO) << "Starting (receiving) server on " << port
            << " : " << num_sockets << " sockets, target dir " << destDirectory;
  kBufferSize = FLAGS_buffer_size;
  if (kBufferSize < kMaxHeader) {
    kBufferSize = 2*1024*((kMaxHeader-1)/(2*1024) + 1); // round up to even k
    LOG(INFO) << "Specified -buffer_size " << FLAGS_buffer_size
              << " smaller than " << kMaxHeader
              << " using " << kBufferSize << " instead";
  }
  gFileCreator.reset(new FileCreator(destDirectory));
  std::vector<std::thread> vt;
  for (int i=0; i < num_sockets; i++) {
    vt.emplace_back(wdtServerOne,
                    port + i,
                    FLAGS_backlog,
                    destDirectory);
  }
  // will never exit
  for (int i=0; i < num_sockets; i++) {
    vt[i].join();
  }
}

typedef std::chrono::high_resolution_clock Clock;
using std::chrono::nanoseconds;
using std::chrono::duration_cast;
using std::chrono::duration;

template<typename T>
double durationSeconds(T d) {
  return duration_cast<duration<double>>(d).count();
}

void wdtClientOne(
  Clock::time_point startTime,
  const string& destHost,
  int port,
  DirectorySourceQueue* queue,
  size_t *pHeaderBytes,
  size_t *pDataBytes
) {
  size_t headerBytes = 0, dataBytes = 0;
  size_t numFiles = 0;
  ClientSocket s(destHost, folly::to<string>(port));
  for (int i = 1; i < FLAGS_max_retries; ++i) {
    if (s.connect()) {
      break;
    }
    LOG(INFO) << "Sleeping after failed attempt " << i;
    usleep(FLAGS_sleep_ms * 1000);
  }
  // one more/last try (stays true if it worked above)
  CHECK(s.connect()) << "Unable to connect despite retries";
  int fd = s.getFd();
  char headerBuf[kMaxHeader];
  std::unique_ptr<ByteSource> source;

  double elapsedSecsConn = durationSeconds(Clock::now() - startTime);
  LOG(INFO) << "Connect took " << elapsedSecsConn;

  while ((source = queue->getNextSource())) {
    ++numFiles;
    size_t off = 0;
    headerBuf[off++] = FILE_CMD;
    const size_t expectedSize = source->getSize();
    size_t actualSize = 0;
    bool success = Protocol::encode(
      headerBuf, off, kMaxHeader, source->getIdentifier(), expectedSize
    );
    CHECK(success);
    ssize_t written = write(fd, headerBuf, off);
    headerBytes += written;
    if (written != off) {
      PLOG(FATAL) << "Write error/mismatch " << written << " " << off;
    }
    LOG(VERBOSE) << "Sent " << written << " on " << fd
                 << " : " << folly::humanify(string(headerBuf, off));
    while (!source->finished()) {
      size_t size;
      char* buffer = source->read(size);
      if (source->hasError()) {
        LOG(ERROR) << "failed reading file";
        break;
      }
      CHECK(buffer && size > 0);
      written = write(fd, buffer, size);
      actualSize += written;
      LOG(VERBOSE) << "wrote " << written << " on " << fd;
      if (written != size) {
        PLOG(FATAL) << "Write error/mismatch " << written << " " << size;
      }
    }
    dataBytes += actualSize;
    if (actualSize != expectedSize) {
      LOG(FATAL) << "UGH " << source->getIdentifier()
                 << " " << expectedSize << " " << actualSize;
    }
  }
  headerBuf[0] = DONE_CMD;
  write(fd, headerBuf, 1); //< TODO check for status/succes
  ++headerBytes;
  shutdown(fd, SHUT_WR);   //< TODO check for status/succes
  LOG(INFO) << "Wrote done cmd on " << fd << " waiting for reply...";
  ssize_t numRead = read(fd, headerBuf, 1);
  CHECK(numRead == 1) << "READ unexpected " << numRead << ":"
                      << folly::humanify(string(headerBuf, numRead));
  CHECK(headerBuf[0] == DONE_CMD) << "Unexpected reply " << headerBuf[0];
  numRead = read(fd, headerBuf, kMaxHeader);
  CHECK(numRead == 0) << "EOF not found when expected " << numRead << ":"
                      << folly::humanify(string(headerBuf, numRead));
  double totalTime = durationSeconds(Clock::now() - startTime);
  size_t totalBytes = headerBytes + dataBytes;
  LOG(WARNING) << "Got reply - all done for fd:" << fd
               << ". Number of files = " << numFiles
               << ". Total time = " << totalTime
               << " (" << elapsedSecsConn << " in connection)"
               << ". Avg file size = " << 1.*dataBytes/numFiles
               << ". Data bytes = " << dataBytes
               << ". Header bytes = " << headerBytes
               << " ( " << 100.*headerBytes/totalBytes << " % overhead)"
               << ". Total bytes = " << totalBytes
               << ". Total throughput = " << totalBytes/totalTime/1024./1024.
               << " Mbytes/sec";
  *pHeaderBytes = headerBytes;
  *pDataBytes = dataBytes;
}

void wdtClient(string destHost, int port, int num_sockets,
               string srcDirectory) {
  auto startTime = Clock::now();
  kBufferSize = FLAGS_buffer_size;
  FileCreator::addTrailingSlash(srcDirectory);
  LOG(INFO) << "Client (sending) to " << destHost << " port " << port
            << " : " << num_sockets << " sockets, source dir " << srcDirectory;
  DirectorySourceQueue queue(srcDirectory, kBufferSize);
  std::vector<std::thread> vt;
  size_t headerBytes[num_sockets];
  size_t dataBytes[num_sockets];
  for (int i=0; i < num_sockets; i++) {
    dataBytes[i] = 0;
    headerBytes[i] = 0;
    vt.emplace_back(wdtClientOne,
                    startTime,
                    destHost,
                    port + i,
                    &queue,
                    &headerBytes[i],
                    &dataBytes[i]);
  }
  queue.init();
  size_t totalDataBytes = 0;
  size_t totalHeaderBytes = 0;
  for (int i=0; i < num_sockets; i++) {
    vt[i].join();
    totalHeaderBytes += headerBytes[i];
    totalDataBytes += dataBytes[i];
  }
  size_t totalBytes = totalDataBytes + totalHeaderBytes;
  double totalTime = durationSeconds(Clock::now() - startTime);
  LOG(WARNING) << "All data transfered in " << totalTime
               << " seconds. Data Mbytes = " << totalDataBytes/1024./1024.
               << ". Header kbytes = " << totalHeaderBytes/1024.
               << " ( " << 100.*totalHeaderBytes/totalBytes << " % overhead)"
               << ". Total bytes = " << totalBytes
               << ". Total throughput = " << totalBytes/totalTime/1024./1024.
               << " Mbytes/sec";
}
