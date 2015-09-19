/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include "WdtResourceController.h"
#include "Protocol.h"
#include <folly/Random.h>
#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
using namespace std;
namespace facebook {
namespace wdt {
class WdtResourceControllerTest : public WdtResourceController {
 public:
  const int protocolVersion = Protocol::protocol_version;
  const int startPort = 24689;
  const int numPorts = 8;
  const string hostName = "localhost";
  const string directory = "/tmp/wdt_resoure_controller_test";
  const int numFiles = 10;
  typedef shared_ptr<Sender> SenderPtr;
  typedef shared_ptr<Receiver> ReceiverPtr;
  WdtResourceControllerTest() : WdtResourceController() {
  }
  void AddObjectsWithNoLimitsTest();
  void MultipleNamespacesTest();
  void AddObjectsWithLimitsTest();
  void InvalidNamespaceTest();
  void ReleaseStaleTest();
  void RequestSerializationTest();

 private:
  string getTransferId(const string &wdtNamespace, int index) {
    return wdtNamespace + to_string(index);
  }

  WdtTransferRequest makeTransferRequest(const string &transferId) {
    WdtTransferRequest request(startPort, numPorts, directory);
    request.hostName = hostName;
    request.transferId = transferId;
    request.protocolVersion = protocolVersion;
    return request;
  }
};

void WdtResourceControllerTest::AddObjectsWithNoLimitsTest() {
  string wdtNamespace = "test-namespace-1";
  registerWdtNamespace(wdtNamespace);
  int index = 0;
  string transferPrefix = "add-objects-transfer";
  for (; index < 3; index++) {
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(receiverPtr != nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
  }
  EXPECT_EQ(getAllReceivers(wdtNamespace).size(), index);
  EXPECT_EQ(getAllSenders(wdtNamespace).size(), index);

  int numSenders = index;
  int numReceivers = index;

  ErrorCode code =
      releaseSender(wdtNamespace, getTransferId(transferPrefix, 0));
  ASSERT_TRUE(code == OK);
  code = releaseReceiver(wdtNamespace, getTransferId(transferPrefix, 0));
  ASSERT_TRUE(code == OK);

  ASSERT_TRUE(numSenders_ == numSenders - 1);
  ASSERT_TRUE(numReceivers_ == numReceivers - 1);
  vector<string> erasedIds;
  code = releaseStaleSenders(wdtNamespace, erasedIds);
  EXPECT_EQ(code, OK);
  vector<string> expectedErasedIds;
  for (int i = 1; i <= 2; i++) {
    expectedErasedIds.push_back(getTransferId(transferPrefix, i));
  }
  sort(erasedIds.begin(), erasedIds.end());
  sort(expectedErasedIds.begin(), expectedErasedIds.end());
  EXPECT_EQ(erasedIds, expectedErasedIds);

  erasedIds.clear();
  code = releaseStaleReceivers(wdtNamespace, erasedIds);
  EXPECT_EQ(code, OK);
  sort(erasedIds.begin(), erasedIds.end());
  EXPECT_EQ(erasedIds, expectedErasedIds);
}

void WdtResourceControllerTest::MultipleNamespacesTest() {
  int namespaceNum = 0;
  int maxNamespaces = 5;
  int numSenders = 0;
  int numReceivers = 0;
  int numObjectsPerNamespace = 3;
  string transferPrefix = "add-objects-transfer";
  for (; namespaceNum < maxNamespaces; namespaceNum++) {
    string wdtNamespace = "test-namespace-" + to_string(namespaceNum);
    registerWdtNamespace(wdtNamespace);
    for (int index = 0; index < numObjectsPerNamespace; index++) {
      auto transferRequest =
          makeTransferRequest(getTransferId(transferPrefix, index));
      ReceiverPtr receiverPtr;
      ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                      transferRequest, receiverPtr);
      ASSERT_TRUE(code == OK);
      ASSERT_TRUE(receiverPtr != nullptr);
      SenderPtr senderPtr;
      code = createSender(wdtNamespace, transferRequest.transferId,
                          transferRequest, senderPtr);
      ASSERT_TRUE(senderPtr != nullptr);
      ASSERT_TRUE(code == OK);

      ++numSenders;
      ++numReceivers;
    }
  }
  ASSERT_TRUE(numSenders_ == numSenders);
  ASSERT_TRUE(numReceivers_ == numReceivers);
  deRegisterWdtNamespace("test-namespace-1");
  ASSERT_TRUE(numSenders_ == numSenders - numObjectsPerNamespace);
  ASSERT_TRUE(numReceivers_ == numReceivers - numObjectsPerNamespace);
}

void WdtResourceControllerTest::AddObjectsWithLimitsTest() {
  int maxNamespaces = 2;
  string transferPrefix = "add-objects-limit-transfer";
  for (int namespaceNum = 1; namespaceNum <= maxNamespaces; namespaceNum++) {
    string wdtNamespace = "test-namespace-" + to_string(namespaceNum);
    registerWdtNamespace(wdtNamespace);
  }

  int index = 0;
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(receiverPtr != nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == QUOTA_EXCEEDED);
    ASSERT_TRUE(receiverPtr == nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
  {
    string wdtNamespace = "test-namespace-2";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));

    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(receiverPtr != nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == QUOTA_EXCEEDED);
    ASSERT_TRUE(senderPtr == nullptr);
    index++;
  }
  deRegisterWdtNamespace("test-namespace-1");
  {
    string wdtNamespace = "test-namespace-2";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == QUOTA_EXCEEDED);
    ASSERT_TRUE(receiverPtr == nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
}

void WdtResourceControllerTest::InvalidNamespaceTest() {
  int index = 0;
  string wdtNamespace = "test-namespace-1";
  string transferPrefix = "invalid-namespace";
  auto transferRequest =
      makeTransferRequest(getTransferId(transferPrefix, index));
  ReceiverPtr receiverPtr;
  ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                  transferRequest, receiverPtr);
  EXPECT_EQ(NOT_FOUND, code);
  /// Receiver should not be added
  ASSERT_TRUE(receiverPtr == nullptr);
  EXPECT_EQ(deRegisterWdtNamespace(wdtNamespace), ERROR);
}

void WdtResourceControllerTest::ReleaseStaleTest() {
  int maxNamespaces = 2;
  string transferPrefix = "add-objects-limit-transfer";
  for (int namespaceNum = 1; namespaceNum <= maxNamespaces; namespaceNum++) {
    string wdtNamespace = "test-namespace-" + to_string(namespaceNum);
    registerWdtNamespace(wdtNamespace);
  }
  int index = 0;
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(receiverPtr != nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
  ASSERT_TRUE(numSenders_ == 1);
  ASSERT_TRUE(numReceivers_ == 1);
  releaseAllSenders("test-namespace-1");
  releaseAllReceivers("test-namespace-1");
  ASSERT_TRUE(numSenders_ == 0);
  ASSERT_TRUE(numReceivers_ == 0);
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    ReceiverPtr receiverPtr;
    ErrorCode code = createReceiver(wdtNamespace, transferRequest.transferId,
                                    transferRequest, receiverPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(receiverPtr != nullptr);
    SenderPtr senderPtr;
    code = createSender(wdtNamespace, transferRequest.transferId,
                        transferRequest, senderPtr);
    ASSERT_TRUE(code == OK);
    ASSERT_TRUE(senderPtr != nullptr);
    senderPtr = getSender(wdtNamespace, getTransferId(transferPrefix, index));
    receiverPtr =
        getReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    ASSERT_TRUE(senderPtr != nullptr);
    ASSERT_TRUE(receiverPtr != nullptr);
    code = releaseReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    EXPECT_EQ(code, OK);
    code = releaseReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    ASSERT_TRUE(code != OK);
    index++;
  }
}

void WdtResourceControllerTest::RequestSerializationTest() {
  {
    WdtUri uri("wdt://blah.com?k1=v1&k2=v2&k3=v3.1,v3.2");
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "blah.com");
    EXPECT_EQ(uri.getQueryParam("k1"), "v1");
    EXPECT_EQ(uri.getQueryParam("k2"), "v2");
    EXPECT_EQ(uri.getQueryParam("k3"), "v3.1,v3.2");
    EXPECT_EQ(uri.getQueryParams().size(), 3);

    uri = "wdt://blah?name=first,second";
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getQueryParam("name"), "first,second");
    uri = "http://blah.com?name=test";
    ASSERT_TRUE(uri.getHostName().empty());

    uri = "wdt://localhost";
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "localhost");

    uri = "wdt://localhost.facebook.com?key=value1,value2";
    EXPECT_EQ(uri.getHostName(), "localhost.facebook.com");
    EXPECT_EQ(uri.getQueryParam("key"), "value1,value2");

    uri = "wdt://127.0.0.1?";
    EXPECT_EQ(uri.getHostName(), "127.0.0.1");
    EXPECT_EQ(uri.getQueryParams().size(), 0);

    uri = "wdt://127.0.0.1?a";
    EXPECT_EQ(uri.getHostName(), "127.0.0.1");
    EXPECT_EQ(uri.getQueryParams().size(), 1);

    EXPECT_EQ(uri.generateUrl(), "wdt://127.0.0.1?a=");

    uri = "wdt://?a=10";
    EXPECT_NE(uri.getErrorCode(), OK);

    vector<string> keys;
    vector<string> values;
    WdtUri wdtUri;
    for (int i = 0; i < 100; i++) {
      keys.push_back(to_string(folly::Random::rand32()));
      values.push_back(to_string(folly::Random::rand32()));
    }
    for (int i = 0; i < keys.size(); i++) {
      wdtUri.setQueryParam(keys[i], values[i]);
    }
    uri = wdtUri.generateUrl();
    EXPECT_NE(uri.getErrorCode(), OK);
    ASSERT_TRUE(uri.getHostName().empty());
    for (int i = 0; i < keys.size(); i++) {
      EXPECT_EQ(uri.getQueryParam(keys[i]), values[i]);
    }
  }
  {
    int index = 0;
    string wdtNamespace = "test-namespace-1";
    string transferPrefix = "invalid-namespace";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    string serialized = transferRequest.generateUrl(true);
    WdtTransferRequest dummy(serialized);
    EXPECT_EQ(dummy.errorCode, OK);
    EXPECT_EQ(dummy.generateUrl(true), serialized);
    EXPECT_EQ(dummy, transferRequest);
  }
  {
    WdtTransferRequest transferRequest(0, 1, "dir1/dir2");
    // Lets not populate anything else
    transferRequest.hostName = "localhost";
    string serializedString = transferRequest.generateUrl(true);
    LOG(INFO) << serializedString;
    WdtTransferRequest dummy(serializedString);
    LOG(INFO) << dummy.generateUrl();
    EXPECT_EQ(transferRequest, dummy);
  }
  {
    WdtTransferRequest transferRequest(0, 8, "/dir3/dir4");
    Receiver receiver(transferRequest);
    transferRequest = receiver.init();
    EXPECT_EQ(transferRequest.errorCode, OK);
    ASSERT_TRUE(transferRequest.ports.size() != 0);
    for (auto port : transferRequest.ports) {
      ASSERT_TRUE(port != 0);
    }
    LOG(INFO) << transferRequest.hostName;
    ASSERT_TRUE(!transferRequest.hostName.empty());
  }
  {
    string uri = "wdt://localhost?ports=1,2,3,10&dir=test&recpv=100&id=111";
    WdtTransferRequest transferRequest(uri);
    EXPECT_EQ(transferRequest.errorCode, OK);
    EXPECT_EQ(transferRequest.hostName, "localhost");
    EXPECT_EQ(transferRequest.directory, "test");
    EXPECT_EQ(transferRequest.protocolVersion, 100);
    EXPECT_EQ(transferRequest.transferId, "111");
    vector<int32_t> expectedPorts;
    for (int i = 0; i < 3; i++) {
      expectedPorts.push_back(i + 1);
    }
    expectedPorts.push_back(10);
    EXPECT_EQ(transferRequest.ports, expectedPorts);
  }
  {
    string uri =
        "wdt://localhost?ports=123*,*,*,*&dir=test&recpv=100&id=111";
    WdtTransferRequest transferRequest(uri);
    vector<int32_t> expectedPorts;
    EXPECT_EQ(transferRequest.ports, expectedPorts);
    EXPECT_EQ(transferRequest.errorCode, URI_PARSE_ERROR);
    EXPECT_EQ(transferRequest.generateUrl(), "URI_PARSE_ERROR");
  }
  {
    string url = "wdt://";
    WdtTransferRequest transferRequest(url);
    EXPECT_EQ(transferRequest.errorCode, URI_PARSE_ERROR);
    EXPECT_EQ(transferRequest.generateUrl(), "URI_PARSE_ERROR");
  }
}

TEST(WdtResourceController, AddObjectsWithNoLimits) {
  auto &options = WdtOptions::getMutable();
  options.namespace_receiver_limit = 0;
  WdtResourceControllerTest t;
  t.AddObjectsWithNoLimitsTest();
}

TEST(WdtResourceController, MultipleNamespacesTest) {
  auto &options = WdtOptions::getMutable();
  options.namespace_receiver_limit = 0;
  WdtResourceControllerTest t;
  t.MultipleNamespacesTest();
}

TEST(WdtResourceController, AddObjectsWithLimitsTest) {
  auto &options = WdtOptions::getMutable();
  options.global_sender_limit = 2;
  options.global_receiver_limit = 2;
  options.namespace_receiver_limit = 1;
  WdtResourceControllerTest t;
  t.AddObjectsWithLimitsTest();
}

TEST(WdtResourceController, InvalidNamespaceTest) {
  WdtResourceControllerTest t;
  t.InvalidNamespaceTest();
}

TEST(WdtResourceControllerTest, ReleaseStaleTest) {
  auto &options = WdtOptions::getMutable();
  options.global_sender_limit = 2;
  options.global_receiver_limit = 2;
  WdtResourceControllerTest t;
  t.ReleaseStaleTest();
}

TEST(WdtResourceControllerTest, RequestSerializationTest) {
  WdtResourceControllerTest t;
  t.RequestSerializationTest();
}

TEST(WdtResourceControllerTest, TransferIdGenerationTest) {
  string transferId1 = WdtBase::generateTransferId();
  string transferId2 = WdtBase::generateTransferId();
  EXPECT_NE(transferId1, transferId2);
}
}
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
