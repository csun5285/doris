
#include "recycler/checker.h"

#include <fmt/core.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <string_view>

#include "common/util.h"
#include "meta-service/keys.h"
#include "recycler/s3_accessor.h"

namespace selectdb {
namespace config {
extern int32_t check_object_interval_seconds;
extern int32_t recycle_concurrency;
} // namespace config

static std::atomic_bool s_is_working = false;

static bool is_working() {
    return s_is_working.load(std::memory_order_acquire);
}

int Checker::start() {
    DCHECK(txn_kv_);

    s_is_working = true;
    // launch instance scanner
    auto scanner_func = [this]() {
        while (is_working()) {
            auto instances = get_instances();
            if (!instances.empty()) {
                // enqueue instances
                std::lock_guard lock(pending_instance_mtx_);
                for (auto& instance : instances) {
                    auto [_, success] = pending_instance_set_.insert(instance.instance_id());
                    // skip instance already in pending queue
                    if (success) {
                        pending_instance_queue_.push_back(std::move(instance));
                    }
                }
                pending_instance_cond_.notify_all();
            }
            {
                std::unique_lock lock(instance_scanner_mtx_);
                instance_scanner_cond_.wait_for(
                        lock, std::chrono::seconds(config::check_object_interval_seconds),
                        [&]() { return !is_working(); });
            }
        }
    };
    workers_.push_back(std::thread(scanner_func));

    // launch check workers
    auto checker_func = [this]() {
        while (is_working()) {
            // fetch instance to check
            InstanceInfoPB instance;
            {
                std::unique_lock lock(pending_instance_mtx_);
                pending_instance_cond_.wait(
                        lock, [&]() { return !pending_instance_queue_.empty() || !is_working(); });
                if (!is_working()) {
                    return;
                }
                instance = std::move(pending_instance_queue_.front());
                pending_instance_queue_.pop_front();
                pending_instance_set_.erase(instance.instance_id());
            }
            {
                std::lock_guard lock(working_instance_set_mtx_);
                auto [_, success] = working_instance_set_.insert(instance.instance_id());
                if (!success) continue;
            }
            check_instance_objects(instance);
            {
                std::lock_guard lock(working_instance_set_mtx_);
                working_instance_set_.erase(instance.instance_id());
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
    s_is_working = false;
    instance_scanner_cond_.notify_all();
    pending_instance_cond_.notify_all();
    for (auto& w : workers_) {
        w.join();
    }
}

std::vector<InstanceInfoPB> Checker::get_instances() {
    std::vector<InstanceInfoPB> instances;
    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(INFO) << "failed to init txn, ret=" << ret;
        return instances;
    }

    std::unique_ptr<RangeGetIterator> it;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return instances;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;

            InstanceInfoPB instance_info;
            if (!instance_info.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance info, key=" << hex(k);
                return instances;
            }

            LOG(INFO) << "get an instance, instance_id=" << instance_info.instance_id();
            instances.push_back(std::move(instance_info));
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    return instances;
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

void Checker::check_instance_objects(const InstanceInfoPB& instance) {
    LOG(INFO) << "begin to check instance objects instance_id=" << instance.instance_id();
    // create s3 accessor
    std::map<std::string, std::shared_ptr<ObjStoreAccessor>> accessor_map;
    for (auto& obj_info : instance.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_info.ak();
        s3_conf.sk = obj_info.sk();
        s3_conf.endpoint = obj_info.endpoint();
        s3_conf.region = obj_info.region();
        s3_conf.bucket = obj_info.bucket();
        s3_conf.prefix = obj_info.prefix();
        auto accessor = std::make_shared<S3Accessor>(std::move(s3_conf));
        if (accessor->init() != 0) [[unlikely]] {
            LOG(WARNING) << "failed to init s3 accessor, instance_id=" << instance.instance_id();
            return;
        }
        accessor_map.emplace(obj_info.id(), std::move(accessor));
    }

    long num_scanned = 0;
    long num_check_failed = 0;
    using namespace std::chrono;
    auto start_time = steady_clock::now();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        auto cost = duration<float>(steady_clock::now() - start_time).count();
        LOG(INFO) << "check instance objects finished, cost=" << cost
                  << "s. instance_id=" << instance.instance_id() << " num_scanned=" << num_scanned
                  << " num_check_failed=" << num_check_failed;
    });

    auto check_rowset_objects = [&](const doris::RowsetMetaPB& rs_meta, std::string_view key) {
        if (rs_meta.num_segments() == 0) return;
        auto it = accessor_map.find(rs_meta.resource_id());
        if (it == accessor_map.end()) [[unlikely]] {
            LOG(WARNING) << "get s3 accessor failed. instance_id=" << instance.instance_id()
                         << " tablet_id=" << rs_meta.tablet_id()
                         << " rowset_id=" << rs_meta.rowset_id_v2() << " key=" << key;
            return;
        }
        auto& accessor = it->second;
        int ret = 0;
        if (rs_meta.num_segments() == 1) {
            auto path =
                    fmt::format("data/{}/{}_0.dat", rs_meta.tablet_id(), rs_meta.rowset_id_v2());
            ret = accessor->exist(path);
            if (ret == 0) { // exist
            } else if (ret == 1) {
                // if rowset kv has been deleted, no data loss
                ret = key_exist(txn_kv_.get(), key);
                if (ret != 1) {
                    ++num_check_failed;
                    LOG(WARNING) << "object not exist, path=" << accessor->path() << '/' << path
                                 << " key=" << key;
                }
            } else { // other error, no need to log, because S3Accessor has logged this error
            }
            return;
        }
        std::vector<std::string> keys;
        ret = accessor->list(fmt::format("data/{}/{}", rs_meta.tablet_id(), rs_meta.rowset_id_v2()),
                             &keys);
        if (ret != 0) { // no need to log, because S3Accessor has logged this error
            return;
        }
        std::unordered_set<std::string> filenames;
        for (auto& key : keys) {
            filenames.insert(key.substr(key.rfind('/') + 1));
        }
        for (int i = 0; i < rs_meta.num_segments(); ++i) {
            auto path = fmt::format("{}_{}.dat", rs_meta.rowset_id_v2(), i);
            if (!filenames.count(fmt::format("{}_{}.dat", rs_meta.rowset_id_v2(), i))) {
                // if rowset kv has been deleted, no data loss
                ret = key_exist(txn_kv_.get(), key);
                if (ret != 1) {
                    ++num_check_failed;
                    LOG(WARNING) << "object not exist, path=" << accessor->path() << '/' << path
                                 << " key=" << key;
                }
                break;
            }
        }
    };

    // scan visible rowsets
    auto start_key = meta_rowset_key({instance.instance_id(), 0, 0});
    auto end_key =
            meta_rowset_key({instance.instance_id(), std::numeric_limits<int64_t>::max(), 0});

    int ret = 0;
    std::unique_ptr<RangeGetIterator> it;
    do {
        std::unique_ptr<Transaction> txn;
        ret = txn_kv_->create_txn(&txn);
        if (ret != 0) {
            LOG(WARNING) << "failed to init txn, ret=" << ret;
            return;
        }

        ret = txn->get(start_key, end_key, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get rowset meta, ret=" << ret;
            return;
        }
        num_scanned += it->size();

        while (it->has_next() && is_working()) {
            auto [k, v] = it->next();
            if (!it->has_next()) start_key = k;

            doris::RowsetMetaPB rs_meta;
            if (!rs_meta.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed rowset meta. key=" << hex(k) << " val=" << hex(v);
                continue;
            }
            check_rowset_objects(rs_meta, k);
        }
        start_key.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more() && is_working());
}

} // namespace selectdb
