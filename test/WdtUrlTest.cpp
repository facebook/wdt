/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Receiver.h>
#include <wdt/Sender.h>
#include <wdt/test/TestCommon.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>

using namespace std;

namespace facebook {
namespace wdt {

TEST(RequestSerializationTest, UrlTests) {
  const auto &options = WdtOptions::get();
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
      keys.push_back(to_string(rand32()));
      values.push_back(to_string(rand32()));
    }
    for (size_t i = 0; i < keys.size(); i++) {
      wdtUri.setQueryParam(keys[i], values[i]);
    }
    uri = wdtUri.generateUrl();
    EXPECT_NE(uri.getErrorCode(), OK);
    ASSERT_TRUE(uri.getHostName().empty());
    for (size_t i = 0; i < keys.size(); i++) {
      EXPECT_EQ(uri.getQueryParam(keys[i]), values[i]);
    }
  }
  {
    // ipv6 uri test
    WdtUri uri("wdt://[::1]:22356?k1=v1");
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "::1");
    EXPECT_EQ(uri.getPort(), 22356);

    uri = "wdt://[::1]:";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "::1");
    EXPECT_EQ(uri.getPort(), -1);

    uri = "wdt://[12::12:1]:1";
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "12::12:1");
    EXPECT_EQ(uri.getPort(), 1);

    uri = "wdt://123.4.5.6:22356";
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "123.4.5.6");
    EXPECT_EQ(uri.getPort(), 22356);

    uri = "wdt://[";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "");

    uri = "wdt://[121";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "");

    uri = "wdt://[]";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "");

    uri = "wdt://[]:22356";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "");
    EXPECT_EQ(uri.getPort(), 22356);

    uri = "wdt://[::1]:*";
    EXPECT_EQ(uri.getErrorCode(), URI_PARSE_ERROR);
    EXPECT_EQ(uri.getHostName(), "::1");
    EXPECT_EQ(uri.getPort(), -1);

    uri = "wdt://]:22356";
    EXPECT_EQ(uri.getErrorCode(), OK);
    EXPECT_EQ(uri.getHostName(), "]");
    EXPECT_EQ(uri.getPort(), 22356);

    {
      uri = "wdt://[::1]:24689";
      EXPECT_EQ(uri.getPort(), 24689);
      WdtTransferRequest transferRequest(uri.generateUrl());
      EXPECT_EQ(uri.getErrorCode(), OK);
      EXPECT_EQ(transferRequest.hostName, "::1");
      EXPECT_EQ(transferRequest.ports,
                WdtTransferRequest::genPortsVector(24689, options.num_ports));
    }
    {
      uri = "wdt://[::1]?num_ports=10";
      WdtTransferRequest transferRequest(uri.generateUrl());
      EXPECT_EQ(uri.getErrorCode(), OK);
      EXPECT_EQ(transferRequest.hostName, "::1");
      EXPECT_EQ(transferRequest.ports,
                WdtTransferRequest::genPortsVector(options.start_port, 10));
    }
    {
      uri = "wdt://[::1]:24689?start_port=22356&ports=1,2,3,4";
      WdtTransferRequest transferRequest(uri.generateUrl());
      EXPECT_EQ(uri.getErrorCode(), OK);
      EXPECT_EQ(transferRequest.hostName, "::1");
      EXPECT_EQ(transferRequest.ports,
                WdtTransferRequest::genPortsVector(1, 4));
    }
  }
  {
    WdtTransferRequest request(123, 5, "");
    request.hostName = "host1";
    request.transferId = "tid1";
    request.protocolVersion = 753;
    string serialized = request.genWdtUrlWithSecret();
    LOG(INFO) << "Serialized " << serialized;
    WdtTransferRequest deser(serialized);
    EXPECT_EQ(deser.hostName, "host1");
    EXPECT_EQ(deser.transferId, "tid1");
    EXPECT_EQ(deser.protocolVersion, 753);
    EXPECT_EQ(deser.ports.size(), 5);
    for (int i = 0; i < 5; ++i) {
      EXPECT_EQ(deser.ports[i], 123 + i);
    }
    EXPECT_EQ(deser.errorCode, OK);
    EXPECT_EQ(deser.genWdtUrlWithSecret(), serialized);
    EXPECT_EQ(deser, request);
  }
  {
    WdtTransferRequest transferRequest(24689, 1, "");
    // Lets not populate anything else
    transferRequest.hostName = "localhost";
    string serializedString = transferRequest.genWdtUrlWithSecret();
    LOG(INFO) << serializedString;
    WdtTransferRequest dummy(serializedString);
    LOG(INFO) << dummy.getLogSafeString();
    EXPECT_EQ(transferRequest, dummy);
  }
  {
    string uri = "wdt://localhost?ports=1&recpv=10";
    WdtTransferRequest transferRequest(uri);
    EXPECT_EQ(transferRequest.errorCode, OK);
    ASSERT_EQ(transferRequest.ports.size(), 1);
    EXPECT_EQ(transferRequest.ports[0], 1);
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
    string uri = "wdt://localhost?ports=123*,*,*,*&dir=test&recpv=100&id=111";
    WdtTransferRequest transferRequest(uri);
    vector<int32_t> expectedPorts;
    // transfer request will fill ports according to the
    // default values in the wdt options
    int32_t startPort = options.start_port;
    int32_t numPorts = options.num_ports;
    for (int32_t i = 0; i < numPorts; i++) {
      expectedPorts.push_back(startPort + i);
    }
    EXPECT_EQ(transferRequest.ports, expectedPorts);
    EXPECT_EQ(transferRequest.errorCode, URI_PARSE_ERROR);
    EXPECT_EQ(transferRequest.genWdtUrlWithSecret(), "URI_PARSE_ERROR");
  }
  {
    string url = "wdt://";
    WdtTransferRequest transferRequest(url);
    EXPECT_EQ(transferRequest.errorCode, URI_PARSE_ERROR);
    EXPECT_EQ(transferRequest.genWdtUrlWithSecret(), "URI_PARSE_ERROR");
  }
  {
    string url = "wdt://localhost:22355?num_ports=3";
    WdtTransferRequest transferRequest(url);
    Receiver receiver(transferRequest);
    auto retTransferRequest = receiver.init();
    EXPECT_EQ(retTransferRequest.errorCode, INVALID_REQUEST);
  }
  {
    string url = "wdt://localhost:22355";
    WdtTransferRequest transferRequest(url);
    Sender sender(transferRequest);
    auto retTransferRequest = sender.init();
    EXPECT_EQ(retTransferRequest.errorCode, INVALID_REQUEST);
  }
  {
    string url = "wdt://localhost:22355?num_ports=3";
    WdtTransferRequest transferRequest(url);
    EXPECT_EQ(transferRequest.errorCode, OK);
    transferRequest.directory = "blah";
    // Artificial error
    transferRequest.errorCode = ERROR;
    Receiver receiver(transferRequest);
    auto retTransferRequest = receiver.init();
    EXPECT_EQ(retTransferRequest.errorCode, ERROR);
  }
}

TEST(RequestSerializationTest, TransferIdGenerationTest) {
  string transferId1 = WdtBase::generateTransferId();
  string transferId2 = WdtBase::generateTransferId();
  EXPECT_NE(transferId1, transferId2);
}

TEST(TransferRequestTest, Encryption1) {
  {
    WdtTransferRequest req("");
    EXPECT_FALSE(req.encryptionData.isSet());
  }
  {
    WdtTransferRequest req(123, 3, "/foo/bar");
    LOG(INFO) << "Url without encr= " << req.getLogSafeString();
    WdtTransferRequest req2(123, 3, "/foo/ba2");
    EXPECT_FALSE(req2 == req);
    req2.directory = "/foo/bar";
    EXPECT_EQ(req, req2);
    req.encryptionData = EncryptionParams(ENC_AES128_CTR, "data1");
    EXPECT_FALSE(req == req2);
  }
  {
    // TODO: hostname is mandatory in url yet not in constructor,
    // while directory isn't and yet is
    WdtTransferRequest req(123, 3, "");
    req.hostName = "host1";
    EXPECT_EQ(req.errorCode, OK);
    req.encryptionData = EncryptionParams(ENC_AES128_CTR, "barFOO");
    EXPECT_EQ(req.encryptionData.getType(), ENC_AES128_CTR);
    string binary("FOObar56");
    binary.push_back(0);
    binary.push_back(1);
    binary.push_back(0xff);
    binary.push_back(0xfe);
    const string secret(binary);
    req.encryptionData = EncryptionParams(ENC_AES128_CTR, secret);
    string ser = req.genWdtUrlWithSecret();
    LOG(INFO) << "Url with e= " << ser;
    EXPECT_EQ(ser,
              "wdt://host1:123?enc=1:464f4f62617235360001fffe"
              "&id=&num_ports=3&recpv=" +
                  std::to_string(Protocol::protocol_version));
    WdtTransferRequest unser(ser);
    EXPECT_EQ(unser.errorCode, OK);
    EXPECT_EQ(unser.encryptionData.getType(), ENC_AES128_CTR);
    EXPECT_EQ(unser.encryptionData.getSecret(), secret);
    EXPECT_EQ(req, unser);
  }
}
}
}  // namespace end

int main(int argc, char *argv[]) {
  FLAGS_logtostderr = true;
  testing::InitGoogleTest(&argc, argv);
  google::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  int ret = RUN_ALL_TESTS();
  return ret;
}
