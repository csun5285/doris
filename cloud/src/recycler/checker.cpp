
#include "recycler/checker.h"

#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <fmt/core.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <sstream>
#include <string_view>
#include <unordered_set>
#include <vector>

#include "common/bvars.h"
#include "common/config.h"
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
extern int32_t scan_instances_interval_seconds;
extern int32_t recycle_job_lease_expired_ms;
extern int32_t recycle_concurrency;
extern std::vector<std::string> recycle_whitelist;
extern std::vector<std::string> recycle_blacklist;
extern bool enable_inverted_check;
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
                    if (instance.status() == InstanceInfoPB::DELETED) continue;
                    using namespace std::chrono;
                    auto enqueue_time_s =
                            duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
                    auto [_, success] =
                            pending_instance_map_.insert({instance.instance_id(), enqueue_time_s});
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
                                   std::chrono::seconds(config::scan_instances_interval_seconds),
                                   [&]() { return stopped(); });
            }
        }
    };
    workers_.push_back(std::thread(scanner_func));
    // Launch lease thread
    workers_.push_back(std::thread([this] { lease_check_jobs(); }));
    // Launch inspect thread
    workers_.push_back(std::thread([this] { inspect_instance_check_interval(); }));

    // launch check workers
    auto checker_func = [this]() {
        while (!stopped()) {
            // fetch instance to check
            InstanceInfoPB instance;
            long enqueue_time_s = 0;
            {
                std::unique_lock lock(mtx_);
                pending_instance_cond_.wait(
                        lock, [&]() { return !pending_instance_queue_.empty() || stopped(); });
                if (stopped()) {
                    return;
                }
                instance = std::move(pending_instance_queue_.front());
                pending_instance_queue_.pop_front();
                enqueue_time_s = pending_instance_map_[instance.instance_id()];
                pending_instance_map_.erase(instance.instance_id());
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
            using namespace std::chrono;
            auto ctime_ms = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
            g_bvar_checker_enqueue_cost_s.put(instance_id, ctime_ms / 1000 - enqueue_time_s);
            ret = checker->do_check();
            if (config::enable_inverted_check) {
                if (checker->do_inverted_check() != 0) ret = -1;
            }
            if (ret == -1) return;
            // If instance checker has been aborted, don't finish this job
            if (!checker->stopped()) {
                finish_instance_recycle_job(txn_kv_.get(), check_job_key, instance.instance_id(),
                                            ip_port_, ret == 0, ctime_ms);
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

#define LOG_CHECK_INTERVAL_ALARM LOG(WARNING) << "Err for check interval: "
void Checker::do_inspect(const InstanceInfoPB& instance) {
    std::string check_job_key = job_check_key({instance.instance_id()});
    std::unique_ptr<Transaction> txn;
    std::string val;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to create txn";
        return;
    }
    ret = txn->get(check_job_key, &val);
    if (ret < 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to get kv, ret=" << ret
                                 << " key=" << hex(check_job_key);
        return;
    }
    auto checker = InstanceChecker(txn_kv_, instance.instance_id());
    if (checker.init(instance) != 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to init instance checker, instance_id="
                                 << instance.instance_id();
        return;
    }
    int64_t bucket_lifecycle_days = 0;
    if (checker.get_bucket_lifecycle(&bucket_lifecycle_days) != 0) {
        LOG_CHECK_INTERVAL_ALARM << "failed to get bucket lifecycle, instance_id="
                                 << instance.instance_id();
        return;
    }
    DCHECK(bucket_lifecycle_days > 0);
    int64_t last_ctime_ms = -1;
    auto job_status = JobRecyclePB::IDLE;
    auto has_last_ctime = [&]() {
        JobRecyclePB job_info;
        if (!job_info.ParseFromString(val)) {
            LOG_CHECK_INTERVAL_ALARM << "failed to parse JobRecyclePB, key=" << hex(check_job_key);
        }
        DCHECK(job_info.instance_id() == instance.instance_id());
        if (!job_info.has_last_ctime_ms()) return false;
        last_ctime_ms = job_info.last_ctime_ms();
        job_status = job_info.status();
        g_bvar_checker_last_success_time_ms.put(instance.instance_id(),
                                                job_info.last_success_time_ms());
        return true;
    };
    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    if (ret == 1 || !has_last_ctime()) {
        // Use instance's ctime for instances that do not have job's last ctime
        last_ctime_ms = instance.ctime();
    }
    DCHECK(now - last_ctime_ms >= 0);
    int64_t expiration_ms =
            bucket_lifecycle_days > config::reserved_buffer_days
                    ? (bucket_lifecycle_days - config::reserved_buffer_days) * 86400000
                    : bucket_lifecycle_days * 86400000;
    TEST_SYNC_POINT_CALLBACK("Checker:do_inspect", &last_ctime_ms);
    if (now - last_ctime_ms >= expiration_ms) {
        TEST_SYNC_POINT("Checker.do_inspect1");
        LOG_CHECK_INTERVAL_ALARM << "check risks, instance_id: " << instance.instance_id()
                                 << " last_ctime_ms: " << last_ctime_ms
                                 << " job_status: " << job_status
                                 << " bucket_lifecycle_days: " << bucket_lifecycle_days
                                 << " reserved_buffer_days: " << config::reserved_buffer_days
                                 << " expiration_ms: " << expiration_ms;
    }
}
#undef LOG_CHECK_INTERVAL_ALARM
void Checker::inspect_instance_check_interval() {
    while (!stopped()) {
        LOG(INFO) << "start to inspect instance check interval";
        std::vector<InstanceInfoPB> instances;
        get_all_instances(txn_kv_.get(), instances);
        for (const auto& instance : instances) {
            if (instance_filter_.filter_out(instance.instance_id())) continue;
            if (stopped()) return;
            if (instance.status() == InstanceInfoPB::DELETED) continue;
            do_inspect(instance);
        }
        {
            std::unique_lock lock(mtx_);
            notifier_.wait_for(lock, std::chrono::seconds(config::scan_instances_interval_seconds),
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
    long num_scanned_with_segment = 0;
    long num_check_failed = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance_id_ << " num_scanned=" << num_scanned
                  << " num_scanned_with_segment=" << num_scanned_with_segment
                  << " num_check_failed=" << num_check_failed;
        g_bvar_checker_num_scanned.put(instance_id_, num_scanned);
        g_bvar_checker_num_scanned_with_segment.put(instance_id_, num_scanned_with_segment);
        g_bvar_checker_num_check_failed.put(instance_id_, num_check_failed);
        g_bvar_checker_check_cost_s.put(instance_id_, static_cast<long>(cost));
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
        ++num_scanned_with_segment;
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
    return num_check_failed == 0 ? 0 : -2;
}

int InstanceChecker::get_bucket_lifecycle(int64_t* lifecycle_days) {
    // If there are multiple buckets, return the minimum lifecycle.
    int64_t min_lifecycle_days = std::numeric_limits<int64_t>::max();
    int64_t tmp_liefcycle_days = 0;
    for (const auto& [obj_info, accessor] : accessor_map_) {
        if (accessor->check_bucket_versioning() != 0) { return -1; }
        if (accessor->get_bucket_lifecycle(&tmp_liefcycle_days) != 0) { return -1; }
        if (tmp_liefcycle_days < min_lifecycle_days) { min_lifecycle_days = tmp_liefcycle_days; }
    }
    *lifecycle_days = min_lifecycle_days;
    return 0;
}

int InstanceChecker::do_inverted_check() {
    LOG(INFO) << "begin to inverted check objects instance_id=" << instance_id_;
    long num_scanned = 0;
    long num_check_failed = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "inverted check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance_id_ << " num_scanned=" << num_scanned
                  << " num_check_failed=" << num_check_failed;
    });

    struct TabletRowsets {
        int64_t tablet_id {0};
        std::unordered_set<std::string> rowset_ids;
    };
    TabletRowsets tablet_rowsets_cache;

    auto check_object_key = [&](const std::string& obj_key) {
        std::vector<std::string> str;
        butil::SplitString(obj_key, '/', &str);
        // {prefix}/data/{tablet_id}/{rowset_id}_{seg_num}.dat
        if (str.size() < 4) {
            return -1;
        }
        int64_t tablet_id = atol((str.end() - 2)->c_str());
        if (tablet_id <= 0) {
            LOG(WARNING) << "failed to parse tablet_id, key=" << obj_key;
            return -1;
        }
        std::string rowset_id;
        if (auto pos = str.back().find('_'); pos != std::string::npos) {
            rowset_id = str.back().substr(0, pos);
        } else {
            LOG(WARNING) << "failed to parse rowset_id, key=" << obj_key;
            return -1;
        }
        if (tablet_rowsets_cache.tablet_id == tablet_id) {
            if (tablet_rowsets_cache.rowset_ids.count(rowset_id) > 0) {
                return 0;
            } else {
                LOG(WARNING) << "rowset not exists, key=" << obj_key;
                return -1;
            }
        }
        // Get all rowset id of this tablet
        tablet_rowsets_cache.tablet_id = tablet_id;
        tablet_rowsets_cache.rowset_ids.clear();
        std::unique_ptr<Transaction> txn;
        int ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to create txn";
            return -1;
        }
        std::unique_ptr<RangeGetIterator> it;
        auto begin = meta_rowset_key({instance_id_, tablet_id, 0});
        auto end = meta_rowset_key({instance_id_, tablet_id, INT64_MAX});
        do {
            ret = txn->get(begin, end, &it);
            if (ret != 0) {
                LOG(WARNING) << "failed to get rowset kv, ret=" << ret;
                return -1;
            }
            if (!it->has_next()) {
                break;
            }
            while (it->has_next()) {
                // recycle corresponding resources
                auto [k, v] = it->next();
                doris::RowsetMetaPB rowset;
                if (!rowset.ParseFromArray(v.data(), v.size())) {
                    LOG(WARNING) << "malformed rowset meta value, key=" << hex(k);
                    return -1;
                }
                tablet_rowsets_cache.rowset_ids.insert(rowset.rowset_id_v2());
                if (!it->has_next()) {
                    begin = k;
                    begin.push_back('\x00'); // Update to next smallest key for iteration
                    break;
                }
            }
        } while (it->more() && !stopped());
        if (tablet_rowsets_cache.rowset_ids.count(rowset_id) > 0) {
            return 0;
        } else {
            LOG(WARNING) << "rowset not exists, key=" << obj_key;
            return -1;
        }
        return 0;
    };

    for (auto& [_, accessor] : accessor_map_) {
        auto s3_accessor = static_cast<S3Accessor*>(accessor.get());
        auto client = s3_accessor->s3_client();
        auto& conf = s3_accessor->conf();
        Aws::S3::Model::ListObjectsV2Request request;
        request.WithBucket(conf.bucket).WithPrefix(conf.prefix + "/data/");
        bool is_trucated = false;
        do {
            auto outcome = client->ListObjectsV2(request);
            if (!outcome.IsSuccess()) {
                LOG(WARNING) << "failed to list objects, endpoint=" << conf.endpoint
                             << " bucket=" << conf.bucket << " prefix=" << request.GetPrefix();
                return -1;
            }
            LOG(INFO) << "get " << outcome.GetResult().GetContents().size()
                      << " objects, endpoint=" << conf.endpoint << " bucket=" << conf.bucket
                      << " prefix=" << request.GetPrefix();
            const auto& result = outcome.GetResult();
            num_scanned += result.GetContents().size();
            for (auto& obj : result.GetContents()) {
                if (check_object_key(obj.GetKey()) != 0) {
                    LOG(WARNING) << "failed to check object key, endpoint=" << conf.endpoint
                                 << " bucket=" << conf.bucket << " key=" << obj.GetKey();
                    ++num_check_failed;
                }
            }
            is_trucated = result.GetIsTruncated();
            request.SetContinuationToken(result.GetNextContinuationToken());
        } while (is_trucated && !stopped());
    }
    return num_check_failed == 0 ? 0 : -1;
}

} // namespace selectdb
