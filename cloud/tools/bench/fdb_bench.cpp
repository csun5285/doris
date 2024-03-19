#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <bthread/bthread.h>
#include <bthread/errno.h>
#include <fmt/core.h>
#include <gflags/gflags.h>
#include <google/protobuf/stubs/callback.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <memory>
#include <tuple>

#include "common/bvars.h"
#include "common/config.h"
#include "common/configbase.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/metric.h"
#include "common/util.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "rate-limiter/rate_limiter.h"
#include "resource-manager/resource_manager.h"

DEFINE_string(cmd, "load", "The command to execute. Support commands: load, verify");
DEFINE_string(cluster_file, "/etc/foundationdb/fdb.cluster",
              "The cluster file of the target fdb cluster");
DEFINE_uint64(begin_instance_id, 0, "The begin of instance id to load");
DEFINE_uint64(end_instance_id, 1000, "The end of instance id to load");
DEFINE_uint64(concurrency, 2, "The number of concurrency");
DEFINE_uint64(num_rowset, 1000, "The number of rowset for each tablet");
DEFINE_uint64(num_tablets, 5, "The number of tablets for each instance");
DEFINE_uint64(report_intervals, 10, "The interval to report progress, in secs");
DEFINE_uint32(payload_size, 1024, "The total bytes of payload of each rowset");
DEFINE_uint32(verify_samples, 100, "The sampling intervals");

struct LoadContext {
    std::shared_ptr<selectdb::TxnKv> txn_kv;
    selectdb::MetaServiceImpl* service;

    size_t num_instances;
    std::atomic_uint64_t next_instance_id;
    std::atomic_size_t num_finished;
    std::vector<std::unique_ptr<std::atomic_uint64_t>> finished;

    LoadContext(size_t concurrency) {
        for (size_t i = 0; i < concurrency; ++i) {
            finished.push_back(std::make_unique<std::atomic_uint64_t>(0));
        }
    }
};

void verify(std::shared_ptr<selectdb::TxnKv> kv);
void load(std::shared_ptr<selectdb::TxnKv> kv);

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!selectdb::config::init("/dev/null")) {
        std::cerr << "init config failed" << std::endl;
        return -1;
    }

    selectdb::config::log_level = "info";
    if (!selectdb::init_glog("fdb_bench")) {
        std::cerr << "init glog failed" << std::endl;
        return -1;
    }

    selectdb::config::fdb_cluster_file_path = FLAGS_cluster_file;
    std::shared_ptr<selectdb::TxnKv> txn_kv = std::make_shared<selectdb::FdbTxnKv>();
    if (int ret = txn_kv->init(); ret != 0) {
        LOG_FATAL("init fdb txn kv").tag("cluster-file", FLAGS_cluster_file).tag("ret", ret);
    }

    LOG_INFO("init global encryption key").tag("key", selectdb::config::encryption_key);
    if (selectdb::init_global_encryption_key_info_map(txn_kv.get()) != 0) {
        LOG_FATAL("init global encryption key info map");
    }

    if (FLAGS_cmd == "verify") {
        verify(txn_kv);
    } else if (FLAGS_cmd == "load") {
        load(txn_kv);
    } else {
        std::cerr << "Invalid command " << FLAGS_cmd << std::endl
                  << "Support commands: load, verify" << std::endl;
        return -1;
    }

    return 0;
}

static std::string rowset_payload(uint64_t rowset_id) {
    static const char PAYLOAD[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+";
    constexpr size_t NUM = sizeof(PAYLOAD);

    std::string content;
    content.reserve(FLAGS_payload_size + NUM);
    content.append(PAYLOAD + (rowset_id % NUM), PAYLOAD + NUM);
    while (content.size() < FLAGS_payload_size) {
        content.append(PAYLOAD, PAYLOAD + NUM);
    }
    content.resize(FLAGS_payload_size);
    return content;
}

static std::string cloud_unique_id(uint64_t instance_id) {
    return fmt::format("{}_cloud_unique_id", instance_id);
}

static selectdb::ObjectStoreInfoPB default_obj_info() {
    selectdb::ObjectStoreInfoPB obj_info;
    obj_info.set_user_id("default_user_id");
    obj_info.set_ak("default_access_key");
    obj_info.set_sk("default_secret_key");
    obj_info.set_bucket("selectdb_cloud");
    obj_info.set_endpoint("default_endpoint");
    obj_info.set_external_endpoint("default_external_endpoint");
    obj_info.set_id("default_id");
    obj_info.set_prefix("/selectdb_cloud/benchmark");
    obj_info.set_region("default_region");
    obj_info.set_provider(selectdb::ObjectStoreInfoPB_Provider::ObjectStoreInfoPB_Provider_S3);
    return obj_info;
}

static void load_instance(selectdb::MetaServiceImpl* service, uint64_t instance_id) {
    CHECK_NE(bthread_self(), 0);

    brpc::Controller ctrl;
    selectdb::CreateInstanceRequest req;
    selectdb::CreateInstanceResponse resp;

    req.set_instance_id(std::to_string(instance_id));
    req.set_name(fmt::format("instance_{}", instance_id));
    req.set_user_id("default_user_id");
    req.mutable_obj_info()->CopyFrom(default_obj_info());
    service->create_instance(&ctrl, &req, &resp, nullptr);
    auto code = resp.status().code();
    if (code != selectdb::MetaServiceCode::OK &&
        code != selectdb::MetaServiceCode::ALREADY_EXISTED) {
        LOG_FATAL("create instance").tag("instance_id", instance_id).tag("code", code);
    }
}

static void add_cluster(selectdb::MetaServiceImpl* service, uint64_t instance_id) {
    CHECK_NE(bthread_self(), 0);

    while (true) {
        brpc::Controller ctrl;
        selectdb::AlterClusterRequest req;
        selectdb::AlterClusterResponse resp;

        req.set_instance_id(std::to_string(instance_id));
        req.set_op(
                selectdb::AlterClusterRequest_Operation::AlterClusterRequest_Operation_ADD_CLUSTER);
        auto* cluster = req.mutable_cluster();
        auto name = fmt::format("instance-{}-cluster", instance_id);
        cluster->set_cluster_id(name);
        cluster->set_cluster_name(name);
        cluster->set_type(selectdb::ClusterPB_Type::ClusterPB_Type_SQL);
        cluster->set_desc("cluster description");
        auto* node = cluster->add_nodes();
        node->set_ip("0.0.0.0");
        node->set_node_type(selectdb::NodeInfoPB_NodeType::NodeInfoPB_NodeType_FE_MASTER);
        node->set_cloud_unique_id(cloud_unique_id(instance_id));
        node->set_edit_log_port(123);
        node->set_heartbeat_port(456);
        node->set_name("default_node");

        service->alter_cluster(&ctrl, &req, &resp, nullptr);
        auto code = resp.status().code();
        if (code == selectdb::MetaServiceCode::OK) {
            return;
        } else if (code == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            req.set_op(selectdb::AlterClusterRequest_Operation::
                               AlterClusterRequest_Operation_DROP_CLUSTER);
            service->alter_cluster(&ctrl, &req, &resp, nullptr);
        } else {
            LOG_FATAL("add default cluster")
                    .tag("instance_id", instance_id)
                    .tag("code", code)
                    .tag("msg", resp.status().msg());
        }
    }
}

static doris::TabletMetaPB add_tablet(int64_t table_id, int64_t index_id, int64_t partition_id,
                                      int64_t tablet_id) {
    doris::TabletMetaPB tablet;
    tablet.set_table_id(table_id);
    tablet.set_index_id(index_id);
    tablet.set_partition_id(partition_id);
    tablet.set_tablet_id(tablet_id);
    auto schema = tablet.mutable_schema();
    schema->set_schema_version(0);
    auto first_rowset = tablet.add_rs_metas();
    first_rowset->set_rowset_id(0); // required
    first_rowset->set_rowset_id_v2(std::to_string(1));
    first_rowset->set_start_version(0);
    first_rowset->set_end_version(1);
    first_rowset->mutable_tablet_schema()->CopyFrom(*schema);
    return tablet;
}

static void create_tablet(selectdb::MetaServiceImpl* meta_service, uint64_t instance_id,
                          int64_t tablet_id) {
    brpc::Controller cntl;
    selectdb::CreateTabletsRequest req;
    selectdb::CreateTabletsResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.add_tablet_metas()->CopyFrom(add_tablet(tablet_id, tablet_id, tablet_id, tablet_id));
    meta_service->create_tablets(&cntl, &req, &resp, nullptr);
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        LOG_FATAL("create tablet")
                .tag("tablet_id", tablet_id)
                .tag("code", resp.status().code())
                .tag("msg", resp.status().msg());
    }
}

static doris::RowsetMetaPB create_rowset(int64_t tablet_id, uint64_t rowset_id,
                                         int64_t version = -1, int num_rows = 100) {
    doris::RowsetMetaPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(std::to_string(rowset_id));
    rowset.set_tablet_id(tablet_id);
    rowset.set_txn_id(0);
    if (version >= 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.mutable_tablet_schema()->set_schema_version(0);

    auto* bound = rowset.add_segments_key_bounds();
    bound->set_min_key(rowset_payload(rowset_id));
    bound->set_max_key("");

    return rowset;
}

static void commit_rowset(selectdb::MetaServiceImpl* meta_service, uint64_t instance_id,
                          const doris::RowsetMetaPB& rowset) {
    brpc::Controller cntl;
    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse res;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_rowset_meta()->CopyFrom(rowset);
    LOG_INFO("each rowset meta size is").tag("size", rowset.ByteSizeLong());
    meta_service->commit_rowset(&cntl, &req, &res, nullptr);
    if (res.status().code() != selectdb::MetaServiceCode::OK) {
        LOG_FATAL("commit rowset")
                .tag("rowset_id", rowset.rowset_id_v2())
                .tag("code", res.status().code())
                .tag("msg", res.status().msg());
    }
}

static void insert_rowset(selectdb::MetaServiceImpl* meta_service, uint64_t instance_id,
                          int64_t tablet_id, uint64_t rowset_id) {
    auto rowset = create_rowset(tablet_id, rowset_id, rowset_id);
    // prepare_rowset(meta_service, instance_id, rowset);
    commit_rowset(meta_service, instance_id, rowset);
}

static void load(std::shared_ptr<LoadContext> ctx, size_t id) {
    selectdb::MetaServiceImpl* service = ctx->service;
    while (true) {
        uint64_t instance_id = ctx->next_instance_id.fetch_add(1, std::memory_order_relaxed);
        if (instance_id >= FLAGS_end_instance_id) {
            break;
        }

        load_instance(service, instance_id);
        add_cluster(service, instance_id);
        for (uint64_t i = 0; i < FLAGS_num_tablets; ++i) {
            int64_t tablet_id = static_cast<int64_t>(i);
            create_tablet(service, instance_id, tablet_id);
            for (uint64_t j = 0; j < FLAGS_num_rowset; ++j) {
                uint64_t rowset_id = j;
                insert_rowset(service, instance_id, tablet_id, rowset_id);
            }
        }

        ctx->finished[id]->store(instance_id, std::memory_order_relaxed);
        ctx->num_finished += 1;
    }
}

static void load_watcher(std::shared_ptr<LoadContext> ctx) {
    constexpr uint64_t SECONDS = 1000000;
    constexpr uint64_t MB = 1024 * 1024;

    uint64_t saved_total_finished = 0;
    auto start_at = std::chrono::high_resolution_clock::now();
    while (saved_total_finished < ctx->num_instances) {
        bthread_usleep(FLAGS_report_intervals * SECONDS);

        selectdb::FdbMetricExporter::export_fdb_metrics(ctx->txn_kv.get());

        // save the progress
        uint64_t min_finished_instance_id =
                **std::min_element(ctx->finished.begin(), ctx->finished.end());
        LOG_INFO("save progress").tag("instance_id", min_finished_instance_id);

        uint64_t total_finished = ctx->num_finished.load(std::memory_order_relaxed);
        auto now = std::chrono::high_resolution_clock::now();
        auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - start_at).count();
        auto duration_seconds = static_cast<double>(duration) / 1000.0;
        auto disk_used_bytes = g_bvar_fdb_data_total_disk_used_bytes.get_value();
        auto kv_size_bytes = g_bvar_fdb_data_total_kv_size_bytes.get_value();
        auto ips = static_cast<double>(total_finished - saved_total_finished) / duration_seconds;
        auto msg = fmt::format(
                "finished {} left {} cost {:.2}.s, load {:.3} ips, disk used {} MB, kv "
                "size {} MB, min instance id {}",
                total_finished, ctx->num_instances - total_finished, duration_seconds, ips,
                disk_used_bytes / MB, kv_size_bytes / MB, min_finished_instance_id);
        std::cout << msg << std::endl;

        saved_total_finished = total_finished;
        start_at = now;
    }
}

static bthread_t start_load_thread(std::shared_ptr<LoadContext> ctx, size_t id) {
    using TaskArg = std::tuple<std::shared_ptr<LoadContext>, size_t>;
    auto fn = +[](void* raw_arg) -> void* {
        std::unique_ptr<TaskArg> arg(reinterpret_cast<TaskArg*>(raw_arg));
        load(std::get<0>(*arg), std::get<1>(*arg));
        return nullptr;
    };

    bthread_t tid;
    auto arg = std::make_unique<std::tuple<std::shared_ptr<LoadContext>, size_t>>(ctx, id);
    if (int res = bthread_start_background(&tid, nullptr, fn, arg.get()); res != 0) {
        LOG_FATAL("setup load thread").tag("msg", std::strerror(res));
    }
    arg.release();
    return tid;
}

static bthread_t start_load_watcher(std::shared_ptr<LoadContext> ctx) {
    auto fn = +[](void* raw_arg) -> void* {
        std::unique_ptr<std::shared_ptr<LoadContext>> arg(
                reinterpret_cast<std::shared_ptr<LoadContext>*>(raw_arg));
        load_watcher(*arg);
        return nullptr;
    };

    bthread_t tid;
    auto arg = std::make_unique<std::shared_ptr<LoadContext>>(ctx);
    if (int res = bthread_start_background(&tid, nullptr, fn, arg.get()); res != 0) {
        LOG_FATAL("start load watcher").tag("msg", std::strerror(res));
    }
    arg.release();
    return tid;
}

static void verify_instance(std::shared_ptr<selectdb::TxnKv> kv, uint64_t instance_id) {
    std::unique_ptr<selectdb::Transaction> txn;
    CHECK_EQ(kv->create_txn(&txn), selectdb::TxnErrorCode::TXN_OK);
    selectdb::InstanceKeyInfo instance_key({std::to_string(instance_id)});
    std::string key;
    std::string value;
    selectdb::instance_key(instance_key, &key);
    selectdb::TxnErrorCode err = txn->get(key, &value, true);
    if (err == selectdb::TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_FATAL("instance key is not found")
                .tag("instance_id", instance_id)
                .tag("key", selectdb::hex(key));
    } else if (err == selectdb::TxnErrorCode::TXN_OK) {
        LOG_FATAL("read instance key").tag("err", err).tag("key", selectdb::hex(key));
    }

    LOG_INFO("read instance key")
            .tag("instance_id", instance_id)
            .tag("key", selectdb::hex(key))
            .tag("value_size", value.size());
}

static void verify_tablet(std::shared_ptr<selectdb::TxnKv> kv, uint64_t instance_id,
                          uint64_t tablet_id) {
    std::unique_ptr<selectdb::Transaction> txn;
    CHECK_EQ(kv->create_txn(&txn), selectdb::TxnErrorCode::TXN_OK);

    std::string key = selectdb::meta_tablet_key(
            {std::to_string(instance_id), tablet_id, tablet_id, tablet_id, tablet_id});
    std::string value;
    selectdb::TxnErrorCode err = txn->get(key, &value, true);
    if (err == selectdb::TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_FATAL("tablet key is not found")
                .tag("instance_id", instance_id)
                .tag("tablet", tablet_id)
                .tag("key", selectdb::hex(key));
    } else if (err == selectdb::TxnErrorCode::TXN_OK) {
        LOG_FATAL("read tablet key")
                .tag("instance_id", instance_id)
                .tag("tablet", tablet_id)
                .tag("key", selectdb::hex(key))
                .tag("err", err);
    }

    LOG_INFO("read tablet key")
            .tag("instance_id", instance_id)
            .tag("tablet", tablet_id)
            .tag("key", selectdb::hex(key))
            .tag("value_size", value.size());
}

static void verify_rowset(std::shared_ptr<selectdb::TxnKv> kv, uint64_t instance_id,
                          uint64_t tablet_id, uint64_t rowset_id) {
    std::unique_ptr<selectdb::Transaction> txn;
    CHECK_EQ(kv->create_txn(&txn), selectdb::TxnErrorCode::TXN_OK);

    std::string key =
            selectdb::meta_rowset_key({std::to_string(instance_id), tablet_id, rowset_id});
    std::string value;
    selectdb::TxnErrorCode err = txn->get(key, &value, true);
    if (err == selectdb::TxnErrorCode::TXN_KEY_NOT_FOUND) {
        LOG_FATAL("rowset key is not found")
                .tag("instance_id", instance_id)
                .tag("tablet", tablet_id)
                .tag("version", rowset_id)
                .tag("key", selectdb::hex(key));
    } else if (err != selectdb::TxnErrorCode::TXN_OK) {
        LOG_FATAL("read rowset key")
                .tag("instance_id", instance_id)
                .tag("tablet", tablet_id)
                .tag("version", rowset_id)
                .tag("key", selectdb::hex(key))
                .tag("err", err);
    }

    LOG_INFO("read rowset key")
            .tag("instance_id", instance_id)
            .tag("tablet", tablet_id)
            .tag("version", rowset_id)
            .tag("key", selectdb::hex(key))
            .tag("value_size", value.size());
}

void verify(std::shared_ptr<selectdb::TxnKv> kv) {
    if (FLAGS_verify_samples == 0) {
        FLAGS_verify_samples = 1;
    }
    for (uint64_t instance_id = FLAGS_begin_instance_id; instance_id < FLAGS_end_instance_id;
         instance_id += FLAGS_verify_samples) {
        // read instance kv.
        verify_instance(kv, instance_id);
        for (uint64_t i = 0; i < FLAGS_num_tablets; ++i) {
            uint64_t tablet_id = i;
            verify_tablet(kv, instance_id, tablet_id);
            for (uint64_t j = 1; j < FLAGS_num_rowset; ++j) {
                uint64_t rowset_id = j;
                verify_rowset(kv, instance_id, tablet_id, rowset_id);
            }
        }
    }
}

void load(std::shared_ptr<selectdb::TxnKv> txn_kv) {
    auto resource_mgr = std::make_shared<selectdb::ResourceManager>(txn_kv);
    auto rate_limiter = std::make_shared<selectdb::RateLimiter>();
    auto meta_service =
            std::make_unique<selectdb::MetaServiceImpl>(txn_kv, resource_mgr, rate_limiter);

    auto ctx = std::make_shared<LoadContext>(FLAGS_concurrency);
    ctx->txn_kv = txn_kv;
    ctx->service = meta_service.get();
    if (FLAGS_begin_instance_id >= FLAGS_end_instance_id) {
        LOG_FATAL("invalid instance id range")
                .tag("begin", FLAGS_begin_instance_id)
                .tag("end", FLAGS_end_instance_id);
    }
    ctx->num_finished = 0;
    ctx->next_instance_id = FLAGS_begin_instance_id;
    ctx->num_instances = FLAGS_end_instance_id - FLAGS_begin_instance_id;
    CHECK_GT(FLAGS_concurrency, 0);

    bthread_list_t bthread_list;
    CHECK_EQ(bthread_list_init(&bthread_list, 0, 0), 0);
    for (size_t i = 0; i < FLAGS_concurrency; ++i) {
        bthread_t tid = start_load_thread(ctx, i);
        CHECK_EQ(bthread_list_add(&bthread_list, tid), 0);
    }
    CHECK_EQ(bthread_list_add(&bthread_list, start_load_watcher(ctx)), 0);
    CHECK_EQ(bthread_list_join(&bthread_list), 0);
    bthread_list_destroy(&bthread_list);
}
