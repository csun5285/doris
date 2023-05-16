
#include "recycler/checker.h"

#include <butil/endpoint.h>
#include <fmt/core.h>
#include <glog/logging.h>

#include <chrono>
#include <mutex>
#include <sstream>
#include <string_view>

#include "common/encryption_util.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "recycler/s3_accessor.h"
#ifdef UNIT_TEST
#include "../test/mock_accessor.h"
#endif
#include "recycler/util.h"

namespace selectdb {
namespace config {
extern int32_t brpc_listen_port;
extern int32_t check_object_interval_seconds;
extern int32_t recycle_job_lease_expired_ms;
extern int32_t recycle_concurrency;
extern std::vector<std::string> recycle_whitelist;
extern std::vector<std::string> recycle_blacklist;
} // namespace config

Checker::Checker(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(std::move(txn_kv)) {
    ip_port_ = std::string(butil::my_ip_cstr()) + ":" + std::to_string(config::brpc_listen_port);
}

Checker::~Checker() {
    if (!stopped()) {
        stop();
    }
}

int Checker::start() {
    DCHECK(txn_kv_);
    instance_filter_.reset(config::recycle_whitelist, config::recycle_blacklist);

    // launch instance scanner
    auto scanner_func = [this]() {
        while (!stopped()) {
            std::vector<InstanceInfoPB> instances;
            get_all_instances(txn_kv_.get(), instances);
            LOG(INFO) << "Checker get instances: " << [&instances] {
                std::stringstream ss;
                for (auto& i : instances) ss << ' ' << i.instance_id();
                return ss.str();
            }();
            if (!instances.empty()) {
                // enqueue instances
                std::lock_guard lock(mtx_);
                for (auto& instance : instances) {
                    if (instance_filter_.filter_out(instance.instance_id())) continue;
                    auto [_, success] = pending_instance_set_.insert(instance.instance_id());
                    // skip instance already in pending queue
                    if (success) {
                        pending_instance_queue_.push_back(std::move(instance));
                    }
                }
                pending_instance_cond_.notify_all();
            }
            {
                std::unique_lock lock(mtx_);
                notifier_.wait_for(lock,
                                   std::chrono::seconds(config::check_object_interval_seconds),
                                   [&]() { return stopped(); });
            }
        }
    };
    workers_.push_back(std::thread(scanner_func));
    // Launch lease thread
    workers_.push_back(std::thread([this] { lease_check_jobs(); }));

    // launch check workers
    auto checker_func = [this]() {
        while (!stopped()) {
            // fetch instance to check
            InstanceInfoPB instance;
            {
                std::unique_lock lock(mtx_);
                pending_instance_cond_.wait(
                        lock, [&]() { return !pending_instance_queue_.empty() || stopped(); });
                if (stopped()) {
                    return;
                }
                instance = std::move(pending_instance_queue_.front());
                pending_instance_queue_.pop_front();
                pending_instance_set_.erase(instance.instance_id());
            }
            auto& instance_id = instance.instance_id();
            {
                std::lock_guard lock(mtx_);
                // skip instance in recycling
                if (working_instance_map_.count(instance_id)) continue;
            }
            auto checker = std::make_shared<InstanceChecker>(txn_kv_, instance.instance_id());
            if (checker->init(instance) != 0) {
                LOG(WARNING) << "failed to init instance checker, instance_id="
                             << instance.instance_id();
                continue;
            }
            std::string check_job_key;
            job_check_key({instance.instance_id()}, &check_job_key);
            int ret = prepare_instance_recycle_job(txn_kv_.get(), check_job_key,
                                                   instance.instance_id(), ip_port_,
                                                   config::check_object_interval_seconds * 1000);
            if (ret != 0) { // Prepare failed
                continue;
            } else {
                std::lock_guard lock(mtx_);
                working_instance_map_.emplace(instance_id, checker);
            }
            if (stopped()) return;
            ret = checker->do_check();
            // If instance checker has been aborted, don't finish this job
            if (!checker->stopped()) {
                finish_instance_recycle_job(txn_kv_.get(), check_job_key, instance.instance_id(),
                                            ip_port_, ret == 0);
            }
            {
                std::lock_guard lock(mtx_);
                working_instance_map_.erase(instance.instance_id());
            }
        }
    };
    int num_threads = config::recycle_concurrency; // FIXME: use a new config entry?
    for (int i = 0; i < num_threads; ++i) {
        workers_.push_back(std::thread(checker_func));
    }
    return 0;
}

void Checker::stop() {
    stopped_ = true;
    notifier_.notify_all();
    pending_instance_cond_.notify_all();
    {
        std::lock_guard lock(mtx_);
        for (auto& [_, checker] : working_instance_map_) {
            checker->stop();
        }
    }
    for (auto& w : workers_) {
        if (w.joinable()) w.join();
    }
}

void Checker::lease_check_jobs() {
    while (!stopped()) {
        std::vector<std::string> instances;
        instances.reserve(working_instance_map_.size());
        {
            std::lock_guard lock(mtx_);
            for (auto& [id, _] : working_instance_map_) {
                instances.push_back(id);
            }
        }
        for (auto& i : instances) {
            std::string check_job_key;
            job_check_key({i}, &check_job_key);
            int ret = lease_instance_recycle_job(txn_kv_.get(), check_job_key, i, ip_port_);
            if (ret == 1) {
                std::lock_guard lock(mtx_);
                if (auto it = working_instance_map_.find(i); it != working_instance_map_.end()) {
                    it->second->stop();
                }
            }
        }
        {
            std::unique_lock lock(mtx_);
            notifier_.wait_for(lock,
                               std::chrono::milliseconds(config::recycle_job_lease_expired_ms / 3),
                               [&]() { return stopped(); });
        }
    }
}

// return 0 for success get a key, 1 for key not found, negative for error
int key_exist(TxnKv* txn_kv, std::string_view key) {
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    if (ret != 0) {
        LOG(WARNING) << "failed to init txn, ret=" << ret;
        return -1;
    }
    std::string val;
    return txn->get(key, &val);
}

InstanceChecker::InstanceChecker(std::shared_ptr<TxnKv> txn_kv, const std::string& instance_id)
        : txn_kv_(std::move(txn_kv)), instance_id_(instance_id) {}

int InstanceChecker::init(const InstanceInfoPB& instance) {
    for (auto& obj_info : instance.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_info.ak();
        s3_conf.sk = obj_info.sk();
        if (obj_info.has_encryption_info()) {
            AkSkPair plain_ak_sk_pair;
            int ret = decrypt_ak_sk_helper(obj_info.ak(), obj_info.sk(), obj_info.encryption_info(),
                                           &plain_ak_sk_pair);
            if (ret != 0) {
                LOG(WARNING) << "fail to decrypt ak sk. instance_id: " << instance_id_
                             << " obj_info: " << proto_to_json(obj_info);
            } else {
                s3_conf.ak = std::move(plain_ak_sk_pair.first);
                s3_conf.sk = std::move(plain_ak_sk_pair.second);
            }
        }
        s3_conf.endpoint = obj_info.endpoint();
        s3_conf.region = obj_info.region();
        s3_conf.bucket = obj_info.bucket();
        s3_conf.prefix = obj_info.prefix();
#ifdef UNIT_TEST
        auto accessor = std::make_shared<MockAccessor>(s3_conf);
#else
        auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
#endif
        if (accessor->init() != 0) [[unlikely]] {
            LOG(WARNING) << "failed to init s3 accessor, instance_id=" << instance.instance_id();
            return -1;
        }
        accessor_map_.emplace(obj_info.id(), std::move(accessor));
    }
    return 0;
}

int InstanceChecker::do_check() {
    TEST_SYNC_POINT("InstanceChecker.do_check");
    LOG(INFO) << "begin to check instance objects instance_id=" << instance_id_;
    long num_scanned = 0;
    long num_check_failed = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance_id_ << " num_scanned=" << num_scanned
                  << " num_check_failed=" << num_check_failed;
    });

    auto check_rowset_objects = [&](const doris::RowsetMetaPB& rs_meta, std::string_view key) {
        if (rs_meta.num_segments() == 0) return;
        auto it = accessor_map_.find(rs_meta.resource_id());
        if (it == accessor_map_.end()) [[unlikely]] {
            ++num_check_failed;
            LOG(WARNING) << "get s3 accessor failed. instance_id=" << instance_id_
                         << " tablet_id=" << rs_meta.tablet_id()
                         << " rowset_id=" << rs_meta.rowset_id_v2() << " key=" << key;
            return;
        }
        auto& accessor = it->second;
        int ret = 0;
        if (rs_meta.num_segments() == 1) {
            auto path = segment_path(rs_meta.tablet_id(), rs_meta.rowset_id_v2(), 0);
            ret = accessor->exist(path);
            if (ret == 0) { // exist
            } else if (ret == 1) {
                // if rowset kv has been deleted, no data loss
                ret = key_exist(txn_kv_.get(), key);
                if (ret != 1) {
                    ++num_check_failed;
                    TEST_SYNC_POINT_CALLBACK("InstanceChecker.do_check1", &path);
                    LOG(WARNING) << "object not exist, path=" << accessor->path() << '/' << path
                                 << " key=" << hex(key);
                }
            } else {
                // other error, no need to log, because S3Accessor has logged this error
                ++num_check_failed;
            }
            return;
        }
        std::vector<std::string> paths;
        ret = accessor->list(rowset_path_prefix(rs_meta.tablet_id(), rs_meta.rowset_id_v2()),
                             &paths);
        if (ret != 0) { // no need to log, because S3Accessor has logged this error
            ++num_check_failed;
            return;
        }
        std::unordered_set<std::string> path_set;
        for (auto& p : paths) {
            path_set.insert(std::move(p));
        }
        for (int i = 0; i < rs_meta.num_segments(); ++i) {
            auto path = segment_path(rs_meta.tablet_id(), rs_meta.rowset_id_v2(), i);
            if (!path_set.count(path)) {
                // if rowset kv has been deleted, no data loss
                ret = key_exist(txn_kv_.get(), key);
                if (ret != 1) {
                    TEST_SYNC_POINT_CALLBACK("InstanceChecker.do_check2", &path);
                    ++num_check_failed;
                    LOG(WARNING) << "object not exist, path=" << accessor->path() << '/' << path
                                 << " key=" << hex(key);
                }
                break;
            }
        }
    };

    // scan visible rowsets
    auto start_key = meta_rowset_key({instance_id_, 0, 0});
    auto end_key = meta_rowset_key({instance_id_, INT64_MAX, 0});

    std::unique_ptr<RangeGetIterator> it;
    do {
        std::unique_ptr<Transaction> txn;
        int ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to init txn, ret=" << ret;
            return -1;
        }

        ret = txn->get(start_key, end_key, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get rowset meta, ret=" << ret;
            return -1;
        }
        num_scanned += it->size();

        while (it->has_next() && !stopped()) {
            auto [k, v] = it->next();
            if (!it->has_next()) start_key = k;

            doris::RowsetMetaPB rs_meta;
            if (!rs_meta.ParseFromArray(v.data(), v.size())) {
                ++num_check_failed;
                LOG(WARNING) << "malformed rowset meta. key=" << hex(k) << " val=" << hex(v);
                continue;
            }
            check_rowset_objects(rs_meta, k);
        }
        start_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more() && !stopped());
    return num_check_failed == 0 ? 0 : -1;
}

} // namespace selectdb
