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

#include "util/s3_rate_limiter.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

namespace doris {

class S3RateLimiterTest : public testing::Test {
public:
    static void SetUpTestSuite() {}

    static void TearDownTestSuite() {}
};

TEST_F(S3RateLimiterTest, normal) {
    auto rate_limiter = S3RateLimiter(1, 5, 10);
    std::atomic_int64_t failed;
    std::atomic_int64_t succ;
    std::atomic_int64_t sleep_thread_num;
    std::atomic_int64_t sleep;
    auto request_thread = [&]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        auto ms = rate_limiter.add(1);
        if (ms < 0) {
            failed++;
        } else if (ms == 0) {
            succ++;
        } else {
            sleep += ms;
            sleep_thread_num++;
        }
    };
    {
        std::vector<std::jthread> threads;
        for (size_t i = 0; i < 20; i++) {
            threads.emplace_back(std::jthread(request_thread));
        }
    }
    ASSERT_EQ(failed, 10);
    ASSERT_EQ(succ, 6);
    ASSERT_EQ(sleep_thread_num, 4);
}

} // namespace doris