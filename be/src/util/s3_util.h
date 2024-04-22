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

#pragma once

#include <aws/core/Aws.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Errors.h>
#include <bvar/bvar.h>
#include <fmt/format.h>
#include <stdint.h>

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "common/status.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "gutil/hash/hash.h"
#include "util/s3_rate_limiter.h"
namespace Aws {
namespace S3 {
class S3Client;
} // namespace S3
} // namespace Aws

namespace bvar {
template <typename T>
class Adder;
}
namespace doris {

namespace s3_bvar {
extern bvar::LatencyRecorder s3_get_latency;
extern bvar::LatencyRecorder s3_put_latency;
extern bvar::LatencyRecorder s3_delete_latency;
extern bvar::LatencyRecorder s3_head_latency;
extern bvar::LatencyRecorder s3_multi_part_upload_latency;
extern bvar::LatencyRecorder s3_list_latency;
extern bvar::LatencyRecorder s3_list_object_versions_latency;
extern bvar::LatencyRecorder s3_get_bucket_version_latency;
extern bvar::LatencyRecorder s3_copy_object_latency;
} // namespace s3_bvar

class S3URI;

inline Aws::Client::AWSError<Aws::S3::S3Errors> s3_error_factory() {
    return Aws::Client::AWSError<Aws::S3::S3Errors>(Aws::S3::S3Errors::INTERNAL_FAILURE,
                                                    "exceeds limit", "exceeds limit", false);
}

#define DO_S3_RATE_LIMIT(op, code)                                                  \
    [&]() mutable {                                                                 \
        if (!config::enable_s3_rate_limiter) {                                      \
            return (code);                                                          \
        }                                                                           \
        auto sleep_duration = S3ClientFactory::instance().rate_limiter(op)->add(1); \
        if (sleep_duration < 0) {                                                   \
            using T = decltype((code));                                             \
            return T(s3_error_factory());                                           \
        }                                                                           \
        return (code);                                                              \
    }()
#define DO_S3_PUT_RATE_LIMIT(code) DO_S3_RATE_LIMIT(S3RateLimitType::PUT, code)

#define DO_S3_GET_RATE_LIMIT(code) DO_S3_RATE_LIMIT(S3RateLimitType::GET, code)

const static std::string S3_AK = "AWS_ACCESS_KEY";
const static std::string S3_SK = "AWS_SECRET_KEY";
const static std::string S3_ENDPOINT = "AWS_ENDPOINT";
const static std::string S3_REGION = "AWS_REGION";
const static std::string S3_TOKEN = "AWS_TOKEN";
const static std::string S3_MAX_CONN_SIZE = "AWS_MAX_CONN_SIZE";
const static std::string S3_REQUEST_TIMEOUT_MS = "AWS_REQUEST_TIMEOUT_MS";
const static std::string S3_CONN_TIMEOUT_MS = "AWS_CONNECTION_TIMEOUT_MS";

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string token;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
    int max_connections = -1;
    int request_timeout_ms = -1;
    int connect_timeout_ms = -1;

    bool sse_enabled = false;
    selectdb::ObjectStoreInfoPB::Provider provider;

    bool use_virtual_addressing = true;

    std::string to_string() const {
        std::string masked_sk;
        constexpr size_t HEAD_LEN = 4;
        constexpr size_t TAIL_LEN = 4;
        masked_sk.reserve(HEAD_LEN + 1 + TAIL_LEN);
        if (sk.length() < HEAD_LEN + 1 + TAIL_LEN) {
            masked_sk = "*";
        } else {
            auto sk_view = std::string_view(sk);
            masked_sk += sk_view.substr(0, HEAD_LEN);
            masked_sk += '*';
            masked_sk += sk_view.substr(sk_view.size() - TAIL_LEN, TAIL_LEN);
        }
        return fmt::format(
                "(ak={}, sk={}, token={}, endpoint={}, region={}, bucket={}, prefix={}, "
                "max_connections={}, request_timeout_ms={}, connect_timeout_ms={}, "
                "use_virtual_addressing={})",
                ak, masked_sk, token, endpoint, region, bucket, prefix, max_connections,
                request_timeout_ms, connect_timeout_ms, use_virtual_addressing);
    }

    uint64_t get_hash() const {
        uint64_t hash_code = 0;
        hash_code += Fingerprint(ak);
        hash_code += Fingerprint(sk);
        hash_code += Fingerprint(token);
        hash_code += Fingerprint(endpoint);
        hash_code += Fingerprint(region);
        hash_code += Fingerprint(bucket);
        hash_code += Fingerprint(prefix);
        hash_code += Fingerprint(max_connections);
        hash_code += Fingerprint(request_timeout_ms);
        hash_code += Fingerprint(connect_timeout_ms);
        hash_code += Fingerprint(use_virtual_addressing);
        return hash_code;
    }
};

class S3ClientFactory {
public:
    ~S3ClientFactory();

    static S3ClientFactory& instance();

    std::shared_ptr<Aws::S3::S3Client> create(const S3Conf& s3_conf);

    static bool is_s3_conf_valid(const std::map<std::string, std::string>& prop);

    static bool is_s3_conf_valid(const S3Conf& s3_conf);

    static Status convert_properties_to_s3_conf(const std::map<std::string, std::string>& prop,
                                                const S3URI& s3_uri, S3Conf* s3_conf);

    static Aws::Client::ClientConfiguration& getClientConfiguration() {
        // The default constructor of ClientConfiguration will do some http call
        // such as Aws::Internal::GetEC2MetadataClient and other init operation,
        // which is unnecessary.
        // So here we use a static instance, and deep copy every time
        // to avoid unnecessary operations.
        static Aws::Client::ClientConfiguration instance;
        return instance;
    }

    S3RateLimiterHolder* rate_limiter(S3RateLimitType type);

private:
    S3ClientFactory();
    static std::string get_valid_ca_cert_path();

    Aws::SDKOptions _aws_options;
    std::mutex _lock;
    std::unordered_map<uint64_t, std::shared_ptr<Aws::S3::S3Client>> _cache;
    std::array<std::unique_ptr<S3RateLimiterHolder>, 2> _rate_limiters;
    std::string _ca_cert_file_path;
};

} // end namespace doris
