#pragma once
#include <bthread/mutex.h>

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>

#include "brpc/server.h"
#include "common/config.h"
namespace selectdb {

class RpcRateLimiter;

class RateLimiter {
public:
    RateLimiter() = default;
    ~RateLimiter() = default;
    void init(google::protobuf::Service* service);
    std::shared_ptr<RpcRateLimiter> get_rpc_rate_limiter(const std::string& rpc_name);

private:
    // rpc_name -> RpcRateLimiter
    std::unordered_map<std::string, std::shared_ptr<RpcRateLimiter>> limiters_;
};

class RpcRateLimiter {
public:
    RpcRateLimiter(const std::string rpc_name, const int64_t max_qps_limit)
            : rpc_name_(rpc_name), max_qps_limit_(max_qps_limit) {}

    ~RpcRateLimiter() = default;

    /**
     * @brief Get the qps token by instance_id
     *
     * @param instance_id
     * @param get_bvar_qps a function that cat get the qps
     */
    bool get_qps_token(const std::string& instance_id, std::function<int()>& get_bvar_qps);

    // Todo: Recycle outdated instance_id

private:
    class QpsToken {
    public:
        QpsToken(const int64_t max_qps_limit) : max_qps_limit_(max_qps_limit) {}

        bool get_token(std::function<int()>& get_bvar_qps);

    private:
        bthread::Mutex mutex_;
        std::chrono::steady_clock::time_point last_update_time_;
        int64_t access_count_ {0};
        int64_t current_qps_ {0};
        int64_t max_qps_limit_;
    };

private:
    bthread::Mutex mutex_;
    // instance_id -> QpsToken
    std::unordered_map<std::string, std::shared_ptr<QpsToken>> qps_limiter_;
    std::string rpc_name_;
    int64_t max_qps_limit_;
};
} // namespace selectdb