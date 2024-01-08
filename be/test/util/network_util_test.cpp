// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "util/network_util.h"

#include <gtest/gtest.h>

namespace doris {

TEST(NetworkUtilTest, ParseEndpoint) {
    struct TestCase {
        std::string endpoint;
        std::string host;
        uint16_t port;
    };

    std::vector<TestCase> cases {{"localhost:80", "localhost", 80},
                                 {"127.0.0.1:123", "127.0.0.1", 123},
                                 {"[::FFFF:204.152.189.116]:444", "::FFFF:204.152.189.116", 444},
                                 {"voce-0679a55f02f4419a9-v609v81w.vpce-svc-023de1c5d795efa0a.us-"
                                  "east-1.voce.amazonaws.com:5000",
                                  "voce-0679a55f02f4419a9-v609v81w.vpce-svc-023de1c5d795efa0a.us-"
                                  "east-1.voce.amazonaws.com",
                                  5000}};

    for (auto& [endpoint, expect_host, expect_port] : cases) {
        std::string host;
        uint16_t port;
        ASSERT_TRUE(parse_endpoint(endpoint, &host, &port));
        ASSERT_EQ(host, expect_host);
        ASSERT_EQ(port, expect_port);
    }
}

TEST(NetworkUtilTest, ParseInvalidEndpoint) {
    std::vector<std::string> endpoints {
            "", ":", "127.0.0.1", "localhost", "localhost:", ":123", "localhost:65536",
    };

    for (auto endpoint : endpoints) {
        std::string host;
        uint16_t port;
        ASSERT_FALSE(parse_endpoint(endpoint, &host, &port));
    }
}

TEST(NetworkUtilTest, DISABLED_ResolveLongEndpoint) {
    // ATTN: Append below line into /etc/hosts before run this test.
    //
    // 127.0.0.1 voce-0679a55f02f4419a9-v609v81w.vpce-svc-023de1c5d795efa0a.us-east-1.voce.amazonaws.com
    std::string endpoint =
            "voce-0679a55f02f4419a9-v609v81w.vpce-svc-023de1c5d795efa0a.us-east-1.voce.amazonaws."
            "com:5000";

    std::string host;
    uint16_t port;
    ASSERT_TRUE(parse_endpoint(endpoint, &host, &port));

    std::string ip;
    ASSERT_TRUE(hostname_to_ip(host, ip).ok());
    ASSERT_EQ(ip, "127.0.0.1");
}

} // namespace doris

#if 0
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
#endif
