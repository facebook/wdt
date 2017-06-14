/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/WdtResourceController.h>

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
    if (index == 1) {
      SenderPtr oldSender;
      code = createSender(wdtNamespace, transferRequest.transferId,
                          transferRequest, oldSender);
      EXPECT_EQ(code, ALREADY_EXISTS);
      EXPECT_EQ(senderPtr, oldSender);
      ReceiverPtr oldReceiver;
      code = createReceiver(wdtNamespace, transferRequest.transferId,
                            transferRequest, oldReceiver);
      EXPECT_EQ(code, ALREADY_EXISTS);
      EXPECT_EQ(receiverPtr, oldReceiver);
    }
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

  EXPECT_EQ(numSenders_, numSenders - 1);
  EXPECT_EQ(numReceivers_, numReceivers - 1);
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
  requireRegistration(true);
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
}
}

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  GFLAGS_NAMESPACE::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
