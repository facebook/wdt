/**
 * Copyright (c) 2014-present, Facebook, Inc.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree. An additional grant
 * of patent rights can be found in the PATENTS file in the same directory.
 */
#include <wdt/Receiver.h>

#include <folly/Random.h>
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
      keys.push_back(to_string(folly::Random::rand32()));
      values.push_back(to_string(folly::Random::rand32()));
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
                WdtBase::genPortsVector(24689, options.num_ports));
    }
    {
      uri = "wdt://[::1]?num_ports=10";
      WdtTransferRequest transferRequest(uri.generateUrl());
      EXPECT_EQ(uri.getErrorCode(), OK);
      EXPECT_EQ(transferRequest.hostName, "::1");
      EXPECT_EQ(transferRequest.ports,
                WdtBase::genPortsVector(options.start_port, 10));
    }
    {
      uri = "wdt://[::1]:24689?start_port=22356&ports=1,2,3,4";
      WdtTransferRequest transferRequest(uri.generateUrl());
      EXPECT_EQ(uri.getErrorCode(), OK);
      EXPECT_EQ(transferRequest.hostName, "::1");
      EXPECT_EQ(transferRequest.ports, WdtBase::genPortsVector(1, 4));
    }
  }
  {
    WdtTransferRequest request(123, 5, "/my/dir");
    request.hostName = "host1";
    request.transferId = "tid1";
    request.protocolVersion = 753;
    string serialized = request.generateUrl(true);
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
    EXPECT_EQ(deser.generateUrl(true), serialized);
    EXPECT_EQ(deser, request);
  }
  {
    WdtTransferRequest transferRequest(24689, 1, "dir1/dir2");
    // Lets not populate anything else
    transferRequest.hostName = "localhost";
    string serializedString = transferRequest.generateUrl(true);
    LOG(INFO) << serializedString;
    WdtTransferRequest dummy(serializedString);
    LOG(INFO) << dummy.generateUrl();
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
    EXPECT_EQ(transferRequest.generateUrl(), "URI_PARSE_ERROR");
  }
  {
    string url = "wdt://";
    WdtTransferRequest transferRequest(url);
    EXPECT_EQ(transferRequest.errorCode, URI_PARSE_ERROR);
    EXPECT_EQ(transferRequest.generateUrl(), "URI_PARSE_ERROR");
  }
}

TEST(RequestSerializationTest, TransferIdGenerationTest) {
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
