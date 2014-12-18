#include "Sender.h"
#include "FileByteSource.h"
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include "Protocol.h"

using namespace testing;

DEFINE_int32(num_sockets, 8, "");
DEFINE_int32(port, 22356, "");
DEFINE_bool(follow_symlinks, false, "");
DEFINE_int32(max_retries, 2, "");
DEFINE_int32(sleep_ms, 50, "");
DEFINE_int32(buffer_size, 256 * 1024, "");

namespace facebook {
namespace wdt {

using std::string;
using std::unique_ptr;

class MockThrottler : public Throttler {
 public:
  MockThrottler() : Throttler(Clock::now(), 0, 0, 0) {
  }

  MOCK_METHOD1(limit, void(double));
};

class MockByteSource : public FileByteSource {
 public:
  MockByteSource() : FileByteSource("", "", 0, 0) {
  }

  MOCK_CONST_METHOD0(getSize, uint64_t(void));
  MOCK_CONST_METHOD0(finished, bool(void));
  MOCK_CONST_METHOD0(hasError, bool(void));
  MOCK_METHOD1(read, char *(size_t &));
};

class MockClientSocket : public ClientSocket {
 public:
  MockClientSocket() : ClientSocket("", "") {
  }

  MOCK_METHOD0(connect, ErrorCode(void));
  MOCK_CONST_METHOD2(read, int(char *, int));
  MOCK_CONST_METHOD2(write, int(char *, int));
};

class SimpleSender : public Sender {
 public:
  SimpleSender() : Sender("localhost", "22000") {
  }

  SendStats sendOneByteSource_(ClientSocket *s, Throttler *throttler,
                               ByteSource *source, const bool doThrottling,
                               const size_t totalBytes) {
    const unique_ptr<Throttler> throttlerPtr(throttler);
    const unique_ptr<ByteSource> sourcePtr(source);
    const unique_ptr<ClientSocket> socketPtr(s);
    return sendOneByteSource(socketPtr, throttlerPtr, sourcePtr, doThrottling,
                             totalBytes);
  }
};

class MockDirectorySourceQueue : public DirectorySourceQueue {
 public:
  MockDirectorySourceQueue() : DirectorySourceQueue("dummy") {
  }

  MOCK_METHOD0(getNextSource_, ByteSource *(void));

  unique_ptr<ByteSource> getNextSource() {
    return unique_ptr<ByteSource>(getNextSource_());
  }
};

class MockSender : public Sender {
 public:
  MockSender() : Sender("localhost", "22000") {
  }

  MOCK_METHOD0(makeSocket_, ClientSocket *(void));

  unique_ptr<ClientSocket> makeSocket(const string &destHost,
                                      int port) override {
    return unique_ptr<ClientSocket>(makeSocket_());
  }

  MOCK_METHOD5(sendOneByteSource, SendStats(const unique_ptr<ClientSocket> &,
                                            const unique_ptr<Throttler> &,
                                            const unique_ptr<ByteSource> &,
                                            const bool, const size_t));
  void sendOneSimple(DirectorySourceQueue &queue, SendStats &stat) {
    sendOne(Clock::now(), "localhost", 220000, queue, 0, 0, 0, stat);
  }
};

TEST(SendOne, ConnectionError) {
  MockClientSocket *socket = new MockClientSocket;
  MockDirectorySourceQueue queue;
  MockSender sender;

  {
    InSequence s;
    EXPECT_CALL(sender, makeSocket_()).WillOnce(Return(socket));
    EXPECT_CALL(*socket, connect()).WillOnce(Return(CONN_ERROR));
  }

  Sender::SendStats stats;
  sender.sendOneSimple(queue, stats);
  EXPECT_EQ(stats.errCode, CONN_ERROR);

  socket = new MockClientSocket;
  {
    InSequence s;
    EXPECT_CALL(sender, makeSocket_()).WillOnce(Return(socket));
    EXPECT_CALL(*socket, connect()).Times(2).WillRepeatedly(
        Return(CONN_ERROR_RETRYABLE));
  }
  sender.sendOneSimple(queue, stats);
  EXPECT_EQ(stats.errCode, CONN_ERROR);
}

TEST(SendOne, ByteSourceSendError) {
  MockClientSocket *socket = new MockClientSocket;
  MockDirectorySourceQueue queue;
  MockByteSource *source = new MockByteSource;
  MockSender sender;

  {
    InSequence s;
    EXPECT_CALL(sender, makeSocket_()).WillOnce(Return(socket));
    EXPECT_CALL(*socket, connect()).Times(1).WillRepeatedly(Return(OK));
    EXPECT_CALL(queue, getNextSource_()).WillOnce(Return(source));
    Sender::SendStats stats;
    stats.errCode = SOCKET_WRITE_ERROR;
    EXPECT_CALL(sender, sendOneByteSource(_, _, _, _, _))
        .WillOnce(Return(stats));
  }

  Sender::SendStats stats;
  sender.sendOneSimple(queue, stats);
  EXPECT_EQ(stats.errCode, SOCKET_WRITE_ERROR);
}

TEST(SendOne, Success) {
  MockClientSocket *socket = new MockClientSocket;
  MockDirectorySourceQueue queue;
  MockByteSource *source = new MockByteSource;
  MockSender sender;

  {
    InSequence s;
    EXPECT_CALL(sender, makeSocket_()).WillOnce(Return(socket));
    EXPECT_CALL(*socket, connect()).Times(1).WillRepeatedly(Return(OK));
    EXPECT_CALL(queue, getNextSource_()).WillOnce(Return(source));
    Sender::SendStats stats;
    stats.errCode = OK;
    stats.totalBytes = 10;
    stats.headerBytes = 3;
    stats.dataBytes = 7;
    EXPECT_CALL(sender, sendOneByteSource(_, _, _, _, _))
        .WillOnce(Return(stats));
    EXPECT_CALL(queue, getNextSource_()).WillOnce(Return(nullptr));
    EXPECT_CALL(*socket, write(_, 1)).WillOnce(Return(1));
    EXPECT_CALL(*socket, read(_, 1))
        .WillOnce(DoAll(SetArgPointee<0>(Protocol::DONE_CMD), Return(1)));
    EXPECT_CALL(*socket, read(_, _)).WillOnce(Return(0));
  }

  Sender::SendStats stats;
  sender.sendOneSimple(queue, stats);
  EXPECT_EQ(stats.errCode, OK);
  EXPECT_EQ(stats.headerBytes, 4);
  EXPECT_EQ(stats.dataBytes, 7);
}

TEST(SendOneByteSource, HeaderWriteFailure) {
  MockClientSocket *socket = new MockClientSocket;
  MockByteSource *source = new MockByteSource;
  MockThrottler *throttler = new MockThrottler;
  SimpleSender sender;

  EXPECT_CALL(*source, getSize()).WillOnce(Return(10));
  EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(0));
  auto stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, SOCKET_WRITE_ERROR);
}

TEST(SendOneByteSource, ByteSourceReadError) {
  MockClientSocket *socket = new MockClientSocket;
  MockByteSource *source = new MockByteSource;
  MockThrottler *throttler = new MockThrottler;
  SimpleSender sender;

  // TEST 1
  EXPECT_CALL(*source, getSize()).WillOnce(Return(10));
  EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
  EXPECT_CALL(*source, finished()).WillOnce(Return(false));
  char p[] = "abc";
  EXPECT_CALL(*source, read(_)).WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
  EXPECT_CALL(*source, hasError()).WillOnce(Return(true));
  auto stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, BYTE_SOURCE_READ_ERROR);
}

TEST(SendOneByteSource, SocketWriteError) {
  MockClientSocket *socket = new MockClientSocket;
  MockByteSource *source = new MockByteSource;
  MockThrottler *throttler = new MockThrottler;
  SimpleSender sender;

  // TEST 1
  EXPECT_CALL(*source, getSize()).WillOnce(Return(10));
  EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
  EXPECT_CALL(*source, finished()).WillOnce(Return(false));
  char p[] = "abc";
  EXPECT_CALL(*source, read(_)).WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
  EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
  EXPECT_CALL(*throttler, limit(_));
  EXPECT_CALL(*socket, write(p, 3)).WillOnce(Return(-1));

  auto stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, SOCKET_WRITE_ERROR);

  // TEST 2
  source = new MockByteSource;
  throttler = new MockThrottler;
  socket = new MockClientSocket;
  EXPECT_CALL(*source, getSize()).WillOnce(Return(10));
  EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
  EXPECT_CALL(*source, finished()).WillOnce(Return(false));
  EXPECT_CALL(*source, read(_)).WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
  EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
  EXPECT_CALL(*throttler, limit(_));
  EXPECT_CALL(*socket, write(p, 3)).WillOnce(Return(4));

  stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, SOCKET_WRITE_ERROR);
}

TEST(SendOneByteSource, SingleChunkSuccess) {
  MockClientSocket *socket = new MockClientSocket;
  MockByteSource *source = new MockByteSource;
  MockThrottler *throttler = new MockThrottler;
  SimpleSender sender;

  // TEST complete write
  {
    InSequence s;  // ordered expectations
    EXPECT_CALL(*source, getSize()).WillOnce(Return(3));
    EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
    EXPECT_CALL(*source, finished()).WillOnce(Return(false));
    char p[] = "abc";
    EXPECT_CALL(*source, read(_))
        .WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
    EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*throttler, limit(_));
    EXPECT_CALL(*socket, write(p, 3)).WillOnce(Return(3));
    EXPECT_CALL(*source, finished()).WillOnce(Return(true));
  }

  auto stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, OK);
  EXPECT_EQ(stats.dataBytes, 3);

  // TEST short write
  source = new MockByteSource;
  throttler = new MockThrottler;
  socket = new MockClientSocket;
  {
    InSequence s;  // ordered expectations
    EXPECT_CALL(*source, getSize()).WillOnce(Return(3));
    EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
    EXPECT_CALL(*source, finished()).WillOnce(Return(false));
    char p[] = "abc";
    EXPECT_CALL(*source, read(_))
        .WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
    EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*throttler, limit(_));
    EXPECT_CALL(*socket, write(p, 3)).WillOnce(Return(2));
    EXPECT_CALL(*socket, write(p + 2, 1)).WillOnce(Return(1));
    EXPECT_CALL(*source, finished()).WillOnce(Return(true));
  }

  stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, OK);
  EXPECT_EQ(stats.dataBytes, 3);
}

TEST(SendOneByteSource, MultiChunkSuccess) {
  MockClientSocket *socket = new MockClientSocket;
  MockByteSource *source = new MockByteSource;
  MockThrottler *throttler = new MockThrottler;
  SimpleSender sender;

  {
    InSequence s;  // ordered expectations
    EXPECT_CALL(*source, getSize()).WillOnce(Return(5));
    EXPECT_CALL(*socket, write(_, _)).WillOnce(Return(3));
    EXPECT_CALL(*source, finished()).WillOnce(Return(false));
    char p[] = "abc";
    EXPECT_CALL(*source, read(_))
        .WillOnce(DoAll(SetArgReferee<0>(3), Return(p)));
    EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*throttler, limit(_));
    EXPECT_CALL(*socket, write(p, 3)).WillOnce(Return(3));
    EXPECT_CALL(*source, finished()).WillOnce(Return(false));
    char q[] = "ab";
    EXPECT_CALL(*source, read(_))
        .WillOnce(DoAll(SetArgReferee<0>(2), Return(q)));
    EXPECT_CALL(*source, hasError()).WillOnce(Return(false));
    EXPECT_CALL(*throttler, limit(_));
    EXPECT_CALL(*socket, write(q, 2)).WillOnce(Return(2));
    EXPECT_CALL(*source, finished()).WillOnce(Return(true));
  }

  auto stats = sender.sendOneByteSource_(socket, throttler, source, true, 0);
  EXPECT_EQ(stats.errCode, OK);
  EXPECT_EQ(stats.dataBytes, 5);
}
}
}

int main(int argc, char *argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
