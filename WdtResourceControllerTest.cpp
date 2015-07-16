#include "WdtResourceController.h"
#include "Protocol.h"
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
  WdtResourceControllerTest() : WdtResourceController() {
  }
  void AddObjectsWithNoLimitsTest();
  void MultipleNamespacesTest();
  void AddObjectsWithLimitsTest();
  void InvalidNamespaceTest();
  void ReleaseStaleTest();

 private:
  string getTransferId(const string& wdtNamespace, int index) {
    return wdtNamespace + to_string(index);
  }

  WdtTransferRequest makeTransferRequest(const string& transferId) {
    WdtTransferRequest request(startPort, numPorts);
    request.transferId = transferId;
    request.protocolVersion = protocolVersion;
    request.directory = directory;
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
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    ASSERT_TRUE(receiverPtr != nullptr);
    auto senderPtr = addSender(wdtNamespace, transferRequest);
    ASSERT_TRUE(senderPtr != nullptr);
  }
  int numSenders = index;
  int numReceivers = index;

  ErrorCode code =
      releaseSender(wdtNamespace, getTransferId(transferPrefix, 0));
  ASSERT_TRUE(code == OK);
  code = releaseReceiver(wdtNamespace, getTransferId(transferPrefix, 0));
  ASSERT_TRUE(code == OK);

  ASSERT_TRUE(numSenders_ == numSenders - 1);
  ASSERT_TRUE(numReceivers_ == numReceivers - 1);
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
      auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
      ASSERT_TRUE(receiverPtr != nullptr);

      auto senderPtr = addSender(wdtNamespace, transferRequest);
      ASSERT_TRUE(senderPtr != nullptr);

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

  updateMaxReceiversLimit(2);
  updateMaxSendersLimit(2);
  updateMaxReceiversLimit("test-namespace-1", 1);
  updateMaxReceiversLimit("test-namespace-2", 1);
  int index = 0;
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    ASSERT_TRUE(receiverPtr != nullptr);

    auto senderPtr = addSender(wdtNamespace, transferRequest);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    // Receiver shouldn't be added
    ASSERT_TRUE(receiverPtr == nullptr);

    auto senderPtr = addSender(wdtNamespace, transferRequest);
    ASSERT_TRUE(senderPtr != nullptr);
    index++;
  }
  {
    string wdtNamespace = "test-namespace-2";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));

    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    /// Receiver should be added
    ASSERT_TRUE(receiverPtr != nullptr);

    auto senderPtr = addSender(wdtNamespace, transferRequest);
    /// Sender should not be added
    ASSERT_TRUE(senderPtr == nullptr);
    index++;
  }
  deRegisterWdtNamespace("test-namespace-1");
  {
    string wdtNamespace = "test-namespace-2";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    /// Receiver shouldn't be added
    ASSERT_TRUE(receiverPtr == nullptr);
    auto senderPtr = addSender(wdtNamespace, transferRequest);
    /// Sender should not be added
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
  auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
  /// Sender should not be added
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
  updateMaxReceiversLimit(2);
  updateMaxSendersLimit(2);
  int index = 0;
  {
    string wdtNamespace = "test-namespace-1";
    auto transferRequest =
        makeTransferRequest(getTransferId(transferPrefix, index));
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    ASSERT_TRUE(receiverPtr != nullptr);
    auto senderPtr = addSender(wdtNamespace, transferRequest);
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
    auto receiverPtr = addReceiver(wdtNamespace, transferRequest);
    ASSERT_TRUE(receiverPtr != nullptr);
    auto senderPtr = addSender(wdtNamespace, transferRequest);
    ASSERT_TRUE(senderPtr != nullptr);
    senderPtr = getSender(wdtNamespace, getTransferId(transferPrefix, index));
    receiverPtr =
        getReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    ASSERT_TRUE(senderPtr != nullptr);
    ASSERT_TRUE(receiverPtr != nullptr);
    ErrorCode code =
        releaseReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    EXPECT_EQ(code, OK);
    code = releaseReceiver(wdtNamespace, getTransferId(transferPrefix, index));
    ASSERT_TRUE(code != OK);
    index++;
  }
}

TEST(WdtResourceController, AddObjectsWithNoLimits) {
  WdtResourceControllerTest t;
  t.AddObjectsWithNoLimitsTest();
}

TEST(WdtResourceController, MultipleNamespacesTest) {
  WdtResourceControllerTest t;
  t.MultipleNamespacesTest();
}

TEST(WdtResourceController, AddObjectsWithLimitsTest) {
  WdtResourceControllerTest t;
  t.AddObjectsWithLimitsTest();
}

TEST(WdtResourceController, InvalidNamespaceTest) {
  WdtResourceControllerTest t;
  t.InvalidNamespaceTest();
}

TEST(WdtResourceControllerTest, ReleaseStaleTest) {
  WdtResourceControllerTest t;
  t.ReleaseStaleTest();
}
}
}

int main(int argc, char* argv[]) {
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
