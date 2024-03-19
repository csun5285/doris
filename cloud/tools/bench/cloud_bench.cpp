#include <brpc/channel.h>
#include <brpc/controller.h>
#include <brpc/grpc.h>
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <butil/endpoint.h>
#include <butil/macros.h>
#include <butil/string_splitter.h>
#include <butil/strings/string_split.h>
#include <bvar/latency_recorder.h>
#include <bvar/status.h>
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <google/protobuf/service.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <iostream>
#include <memory>
#include <thread>
#include <type_traits>

#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"

DEFINE_string(cmd, "copy-into", "The command to execute, allowed values: copy-into, version, read");
DEFINE_string(endpoints, "127.0.0.1:5000", "The endpoints of meta service, seperated by comma");
DEFINE_uint64(concurrency, 0, "The number of concurrency");
DEFINE_uint64(prepare_concurrency, 0, "The number of concurrency during prepare");
DEFINE_uint64(instance_id, 1, "The first id of instances to bench");
DEFINE_uint64(report_intervals, 10, "The interval to report progress, in secs");
DEFINE_uint64(start_intervals, 0,
              "The interval between starting workers, in secs, set 0 to disable it");
DEFINE_uint64(workload_num_op, 1000, "The number of op to execute");
DEFINE_uint64(workload_num_rowset, 1000, "The number of rowset a txn to commit");
DEFINE_uint64(workload_num_version, 2000, "The number of rowsets to read in get_rowset");
DEFINE_uint32(workload_payload_size, 1024, "The total bytes of payload of each rowset");

using Status = selectdb::MetaServiceResponseStatus;

class ConcurrencyLimiter {
    DISALLOW_COPY_AND_ASSIGN(ConcurrencyLimiter);

public:
    ConcurrencyLimiter(size_t num) : num_(num) {}

    void Acquire() {
        std::unique_lock<bthread::Mutex> lock(mutex_);
        while (num_ == 0) {
            cv_.wait(lock);
        }
        num_ -= 1;
    }

    void Release() {
        std::unique_lock<bthread::Mutex> lock(mutex_);
        num_ += 1;
        cv_.notify_one();
    }

private:
    bthread::Mutex mutex_;
    bthread::ConditionVariable cv_;
    size_t num_;
};

static std::atomic<uint64_t> g_next(0);
static std::atomic<uint64_t> g_success(0);
static std::atomic<uint64_t> g_failure(0);
static std::atomic<uint64_t> g_concurrency(0);
static std::unique_ptr<ConcurrencyLimiter> g_limiter;
static bvar::LatencyRecorder g_bvar_op_latency("cloud_bench", "copy_into");

void run(selectdb::MetaService_Stub* service);

int main(int argc, char** argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (!selectdb::config::init("/dev/null")) {
        std::cerr << "init config failed" << std::endl;
        return -1;
    }

    selectdb::config::log_level = "info";
    if (!selectdb::init_glog("cloud_bench")) {
        std::cerr << "init glog failed" << std::endl;
        return -1;
    }

    if (FLAGS_prepare_concurrency == 0) {
        FLAGS_prepare_concurrency = std::thread::hardware_concurrency();
    }
    g_limiter = std::make_unique<ConcurrencyLimiter>(FLAGS_prepare_concurrency);

    // build brpc service stub by a list of endpoints and a round-robin selector.
    brpc::ChannelOptions options;
    options.connect_timeout_ms = 1000;
    options.timeout_ms = 60 * 1000;
    auto channel = std::make_unique<brpc::Channel>();
    auto endpoints = fmt::format("list://{}", FLAGS_endpoints);
    if (channel->Init(endpoints.c_str(), "rr", &options)) {
        LOG_FATAL("init brpc channel to meta service").tag("endpoint", FLAGS_endpoints);
    }
    auto meta_service = std::make_unique<selectdb::MetaService_Stub>(
            channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);

    if (FLAGS_concurrency == 0) {
        FLAGS_concurrency = std::thread::hardware_concurrency();
    }

    run(meta_service.get());

    return 0;
}

static std::string rowset_payload(uint64_t rowset_id) {
    static const char PAYLOAD[] =
            "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789!@#$%^&*()_+";
    constexpr size_t NUM = sizeof(PAYLOAD);

    std::string content;
    content.reserve(FLAGS_workload_payload_size + NUM);
    content.append(PAYLOAD + (rowset_id % NUM), PAYLOAD + NUM);
    while (content.size() < FLAGS_workload_payload_size) {
        content.append(PAYLOAD, PAYLOAD + NUM);
    }
    content.resize(FLAGS_workload_payload_size);
    return content;
}

static std::string cloud_unique_id(uint64_t instance_id) {
    // degraded format
    return fmt::format("1:{}:unique_id", instance_id);
}

static void create_instance(selectdb::MetaService_Stub* service, uint64_t instance_id) {
    selectdb::CreateInstanceRequest req;
    selectdb::CreateInstanceResponse resp;
    req.set_instance_id(std::to_string(instance_id));
    req.set_user_id("user_id");
    req.set_name(fmt::format("instance-{}", instance_id));
    req.set_sse_enabled(false);
    auto* obj_info = req.mutable_obj_info();
    obj_info->set_ak("access-key");
    obj_info->set_sk("secret-key");
    obj_info->set_bucket("selectdb-test-bucket");
    obj_info->set_prefix("selectdb-test");
    obj_info->set_endpoint("endpoint");
    obj_info->set_external_endpoint("endpoint");
    obj_info->set_region("region");
    obj_info->set_provider(selectdb::ObjectStoreInfoPB_Provider_BOS);

    brpc::Controller ctrl;
    service->create_instance(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("create instance")
                .tag("instance_id", instance_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    auto code = resp.status().code();
    if (code != selectdb::MetaServiceCode::OK &&
        code != selectdb::MetaServiceCode::ALREADY_EXISTED) {
        LOG_FATAL("create instance")
                .tag("instance_id", instance_id)
                .tag("code", code)
                .tag("msg", resp.status().msg());
    }
}

static void add_cluster(selectdb::MetaService_Stub* service, uint64_t instance_id) {
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
        if (ctrl.Failed()) {
            LOG_FATAL("alter cluster")
                    .tag("instance_id", instance_id)
                    .tag("code", ctrl.ErrorCode())
                    .tag("msg", ctrl.ErrorText());
        }

        auto code = resp.status().code();
        if (code == selectdb::MetaServiceCode::OK) {
            return;
        } else if (code == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            req.set_op(selectdb::AlterClusterRequest_Operation::
                               AlterClusterRequest_Operation_DROP_CLUSTER);
            ctrl.Reset();
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

static void create_tablet(selectdb::MetaService_Stub* meta_service, uint64_t instance_id,
                          int64_t tablet_id) {
    CHECK_GT(tablet_id, 0);
    while (true) {
        brpc::Controller ctrl;
        selectdb::CreateTabletsRequest req;
        selectdb::CreateTabletsResponse resp;
        req.set_cloud_unique_id(cloud_unique_id(instance_id));
        req.add_tablet_metas()->CopyFrom(add_tablet(tablet_id, tablet_id, tablet_id, tablet_id));
        meta_service->create_tablets(&ctrl, &req, &resp, nullptr);
        if (ctrl.Failed()) {
            LOG_FATAL("create_tablets")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("code", ctrl.ErrorCode())
                    .tag("msg", ctrl.ErrorText());
        }
        if (resp.status().code() != selectdb::MetaServiceCode::OK) {
            LOG_FATAL("create tablet")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", tablet_id)
                    .tag("code", resp.status().code())
                    .tag("msg", resp.status().msg());
        }
        return;
    }
}

static Status begin_txn(selectdb::MetaService_Stub* service, uint64_t instance_id,
                        std::string label, int64_t db_id, const std::vector<uint64_t>& tablet_ids,
                        int64_t* txn_id) {
    brpc::Controller ctrl;
    selectdb::BeginTxnRequest req;
    selectdb::BeginTxnResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    auto* txn_info = req.mutable_txn_info();
    txn_info->set_label(label);
    txn_info->set_db_id(db_id);
    txn_info->mutable_table_ids()->Add(tablet_ids.begin(), tablet_ids.end());
    txn_info->set_timeout_ms(1000 * 60 * 60);

    service->begin_txn(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("begin_txn")
                .tag("instance_id", instance_id)
                .tag("table_ids", fmt::format("{}", fmt::join(tablet_ids, ",")))
                .tag("label", label)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }
    *txn_id = resp.txn_id();
    return resp.status();
}

static Status commit_txn(selectdb::MetaService_Stub* service, uint64_t instance_id,
                         int64_t txn_id) {
    brpc::Controller ctrl;
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_txn_id(txn_id);

    service->commit_txn(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("commit_txn")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    return resp.status();
}

static doris::RowsetMetaPB create_rowset(int64_t tablet_id, std::string rowset_id, int64_t txn_id,
                                         int64_t version = -1, int num_rows = 100) {
    CHECK_GT(tablet_id, 0);

    doris::RowsetMetaPB rowset;
    rowset.set_rowset_id(0); // required
    rowset.set_rowset_id_v2(rowset_id);
    rowset.set_tablet_id(tablet_id);
    rowset.set_partition_id(tablet_id);
    rowset.set_txn_id(txn_id);
    if (version >= 0) {
        rowset.set_start_version(version);
        rowset.set_end_version(version);
    }
    rowset.set_num_segments(1);
    rowset.set_num_rows(num_rows);
    rowset.set_data_disk_size(num_rows * 100);
    rowset.mutable_tablet_schema()->set_schema_version(0);

    auto* bound = rowset.add_segments_key_bounds();
    bound->set_min_key(rowset_payload(txn_id));
    bound->set_max_key("");

    return rowset;
}

static Status prepare_rowset(selectdb::MetaService_Stub* service, uint64_t instance_id,
                             int64_t txn_id, int64_t tablet_id, std::string rowset_id) {
    brpc::Controller ctrl;
    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_rowset_meta()->CopyFrom(create_rowset(tablet_id, rowset_id, txn_id));

    service->prepare_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("prepare_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }
    return resp.status();
}

static Status commit_rowset(selectdb::MetaService_Stub* service, uint64_t instance_id,
                            int64_t txn_id, int64_t tablet_id, std::string rowset_id) {
    brpc::Controller ctrl;
    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_rowset_meta()->CopyFrom(create_rowset(tablet_id, rowset_id, txn_id));

    service->commit_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("commit_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }
    return resp.status();
}

static Status insert_rowset(selectdb::MetaService_Stub* service, uint64_t instance_id,
                            int64_t txn_id, int64_t tablet_id, std::string rowset_id) {
    auto status = prepare_rowset(service, instance_id, txn_id, tablet_id, rowset_id);
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("prepare_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        if (status.code() != selectdb::MetaServiceCode::ALREADY_EXISTED) {
            return status;
        }
    }

    status = commit_rowset(service, instance_id, txn_id, tablet_id, rowset_id);
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("commit_rowset")
                .tag("instance_id", instance_id)
                .tag("txn_id", txn_id)
                .tag("tablet_id", tablet_id)
                .tag("rowset_id", rowset_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status copy_into(selectdb::MetaService_Stub* service, uint64_t instance_id,
                        std::string label, const std::vector<uint64_t>& tablet_ids) {
    int64_t txn_id = 0;
    auto status = begin_txn(service, instance_id, label, 1, tablet_ids, &txn_id);
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("begin_txn")
                .tag("instance_id", instance_id)
                .tag("label", label)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    for (uint64_t tablet_id : tablet_ids) {
        auto rowset_id = fmt::format("rowset_{}_{}", label, tablet_id);
        status = insert_rowset(service, instance_id, txn_id, tablet_id, rowset_id);
        if (status.code() != selectdb::MetaServiceCode::OK) {
            return status;
        }
    }

    status = commit_txn(service, instance_id, txn_id);
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("commit_txn")
                .tag("instance_id", instance_id)
                .tag("label", label)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static std::vector<uint64_t> prepare_instance_and_tablets(selectdb::MetaService_Stub* service,
                                                          uint64_t instance_id) {
    create_instance(service, instance_id);
    add_cluster(service, instance_id);
    std::vector<uint64_t> tablet_ids;
    for (uint64_t i = 1; i <= FLAGS_workload_num_rowset; ++i) {
        create_tablet(service, instance_id, i);
        tablet_ids.push_back(i);
    }
    return tablet_ids;
}

static Status get_version(selectdb::MetaService_Stub* service, uint64_t instance_id,
                          int64_t tablet_id) {
    brpc::Controller ctrl;
    selectdb::GetVersionRequest req;
    selectdb::GetVersionResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_db_id(1);
    req.set_partition_id(tablet_id);
    req.set_table_id(tablet_id);

    service->get_version(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("get version")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    auto status = resp.status();
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("get version")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status get_tablet_meta(selectdb::MetaService_Stub* service, uint64_t instance_id,
                              int64_t tablet_id) {
    brpc::Controller ctrl;
    selectdb::GetTabletRequest req;
    selectdb::GetTabletResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.set_tablet_id(tablet_id);
    service->get_tablet(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("get_tablet")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    auto status = resp.status();
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("get_tablet")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
        return status;
    }

    return Status();
}

static Status get_tablet_stats(selectdb::MetaService_Stub* service, uint64_t instance_id,
                               int64_t tablet_id, selectdb::TabletStatsPB* stats) {
    brpc::Controller ctrl;
    selectdb::GetTabletStatsRequest req;
    selectdb::GetTabletStatsResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    auto* tablet_idx = req.mutable_tablet_idx()->Add();
    tablet_idx->set_tablet_id(tablet_id);
    service->get_tablet_stats(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("get tablet stats")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    auto status = resp.status();
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("get_tablet_stats")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
    }

    stats->CopyFrom(resp.tablet_stats().at(0));

    return Status();
}

static Status get_rowset_meta(selectdb::MetaService_Stub* service, uint64_t instance_id,
                              int64_t tablet_id, const selectdb::TabletStatsPB& stats) {
    CHECK_GT(tablet_id, 0);

    brpc::Controller ctrl;
    selectdb::GetRowsetRequest req;
    selectdb::GetRowsetResponse resp;
    req.set_cloud_unique_id(cloud_unique_id(instance_id));
    req.mutable_idx()->set_tablet_id(tablet_id);
    req.set_start_version(0);
    req.set_end_version(FLAGS_workload_num_version);
    req.set_base_compaction_cnt(stats.base_compaction_cnt());
    req.set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
    req.set_cumulative_point(stats.cumulative_point());
    service->get_rowset(&ctrl, &req, &resp, nullptr);
    if (ctrl.Failed()) {
        LOG_FATAL("get_rowset")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", ctrl.ErrorCode())
                .tag("msg", ctrl.ErrorText());
    }

    auto status = resp.status();
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_ERROR("get_rowset")
                .tag("instance_id", instance_id)
                .tag("tablet_id", tablet_id)
                .tag("code", status.code())
                .tag("msg", status.msg());
    }

    return Status();
}

static void run_copy_into(selectdb::MetaService_Stub* service, uint64_t id) {
    uint64_t instance_id = FLAGS_instance_id + id;
    auto tablet_ids = prepare_instance_and_tablets(service, instance_id);

    std::string ip = butil::my_ip_cstr();
    auto now = std::chrono::high_resolution_clock::now();
    auto epoch = now.time_since_epoch().count();
    while (true) {
        uint64_t index = g_next.fetch_add(1, std::memory_order_relaxed);
        if (index >= FLAGS_workload_num_op) {
            break;
        }

        auto label = fmt::format("{}-{}-{}-{}", ip, epoch, id, index);

        selectdb::StopWatch sw;
        auto status = copy_into(service, instance_id, label, tablet_ids);
        g_bvar_op_latency << sw.elapsed_us();

        if (status.code() == selectdb::MetaServiceCode::OK) {
            g_success.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_failure.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

static void reporter() {
    constexpr uint64_t SECONDS = 1000000;

    std::string cmd = FLAGS_cmd;
    std::transform(cmd.begin(), cmd.end(), cmd.begin(), ::toupper);

    uint64_t saved_success = 0, saved_failure = 0;
    auto start_at = std::chrono::high_resolution_clock::now();
    while (saved_failure + saved_success < FLAGS_workload_num_op) {
        bthread_usleep(FLAGS_report_intervals * SECONDS);

        auto now = std::chrono::high_resolution_clock::now();
        auto duration =
                std::chrono::duration_cast<std::chrono::milliseconds>(now - start_at).count();
        auto duration_seconds = static_cast<double>(duration) / 1000.0;
        auto success = g_success.load(std::memory_order_relaxed);
        auto failure = g_failure.load(std::memory_order_relaxed);

        auto concurrency = g_concurrency.load(std::memory_order_relaxed) - 1;
        auto ops = static_cast<double>(success - saved_success) / duration_seconds;
        auto p50 = g_bvar_op_latency.latency_percentile(0.5);
        auto p99 = g_bvar_op_latency.latency_percentile(0.99);
        auto p999 = g_bvar_op_latency.latency_percentile(0.999);
        auto pmax = g_bvar_op_latency.max_latency();

        auto msg = fmt::format(
                "{} {:8} OPS cost {:.2}s, P50 {}, P99 {}, P999 {}, MAX {}, CONCURRENCY {}", cmd,
                static_cast<uint64_t>(ops), duration_seconds, p50, p99, p999, pmax, concurrency);
        std::cout << msg << std::endl;

        saved_success = success;
        saved_failure = failure;
        start_at = now;
    }
}

static void prepare_version_workload(selectdb::MetaService_Stub* service, size_t id) {
    g_limiter->Acquire();

    uint64_t instance_id = FLAGS_instance_id + id;
    auto tablet_ids = prepare_instance_and_tablets(service, instance_id);

    // Put version_key.
    auto now = std::chrono::high_resolution_clock::now();
    auto epoch = now.time_since_epoch().count();
    auto label = fmt::format("version-{}-{}", epoch, id);
    auto status = copy_into(service, instance_id, label, tablet_ids);
    if (status.code() != selectdb::MetaServiceCode::OK) {
        LOG_FATAL("prepare version workload")
                .tag("instance_id", instance_id)
                .tag("label", label)
                .tag("code", status.code())
                .tag("msg", status.msg());
    }

    g_limiter->Release();
}

static void run_version(selectdb::MetaService_Stub* service, size_t id) {
    uint64_t instance_id = FLAGS_instance_id + id;
    size_t idx = 0;
    while (true) {
        uint64_t index = g_next.fetch_add(1, std::memory_order_relaxed);
        if (index >= FLAGS_workload_num_op) {
            break;
        }

        selectdb::StopWatch sw;
        int64_t tablet_id = idx++ % FLAGS_workload_num_rowset + 1;
        auto status = get_version(service, instance_id, tablet_id);
        g_bvar_op_latency << sw.elapsed_us();

        if (status.code() == selectdb::MetaServiceCode::OK) {
            g_success.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_failure.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

static void prepare_read_workload(selectdb::MetaService_Stub* service, size_t id) {
    g_limiter->Acquire();

    uint64_t instance_id = FLAGS_instance_id + id;
    auto tablet_ids = prepare_instance_and_tablets(service, instance_id);

    // Put tablet_meta and rowset metas
    auto now = std::chrono::high_resolution_clock::now();
    auto epoch = now.time_since_epoch().count();
    for (size_t i = 0; i < FLAGS_workload_num_version; ++i) {
        auto label = fmt::format("read-{}-{}-{}", epoch, id, i);
        auto status = copy_into(service, instance_id, label, tablet_ids);
        if (status.code() != selectdb::MetaServiceCode::OK) {
            LOG_FATAL("prepare read workload")
                    .tag("instance_id", instance_id)
                    .tag("index", i)
                    .tag("code", status.code())
                    .tag("msg", status.msg());
        }
    }

    g_limiter->Release();
}

static void read_tablet_meta(selectdb::MetaService_Stub* service, size_t id) {
    uint64_t instance_id = FLAGS_instance_id + id;

    std::vector<selectdb::TabletStatsPB> tablet_stats;
    for (int64_t i = 1; i <= FLAGS_workload_num_rowset; ++i) {
        selectdb::TabletStatsPB stats;
        auto status = get_tablet_stats(service, instance_id, i, &stats);
        if (status.code() != selectdb::MetaServiceCode::OK) {
            LOG_FATAL("read tablet meta: get tablet stats")
                    .tag("instance_id", instance_id)
                    .tag("tablet_id", i)
                    .tag("code", status.code())
                    .tag("msg", status.msg());
        }
        tablet_stats.push_back(stats);
    }

    size_t idx = 0;
    while (true) {
        uint64_t index = g_next.fetch_add(1, std::memory_order_relaxed);
        if (index >= FLAGS_workload_num_op) {
            break;
        }

        selectdb::StopWatch sw;
        int64_t tablet_id = idx++ % FLAGS_workload_num_rowset + 1;
        auto status = get_tablet_meta(service, instance_id, tablet_id);
        if (status.code() == selectdb::MetaServiceCode::OK) {
            const auto& stat = tablet_stats[tablet_id - 1];
            status = get_rowset_meta(service, instance_id, tablet_id, stat);
        }
        g_bvar_op_latency << sw.elapsed_us();

        if (status.code() == selectdb::MetaServiceCode::OK) {
            g_success.fetch_add(1, std::memory_order_relaxed);
        } else {
            g_failure.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

template <typename Fn, typename... Args>
static bthread_t start_bthread(Fn fn, Args... args) {
    using TaskArg = std::tuple<Args...>;
    struct Task {
        Fn fn;
        TaskArg args;
        Task(Fn f, TaskArg a) : fn(std::move(f)), args(std::move(a)) {}
    };
    auto task_fn = +[](void* raw_task) -> void* {
        std::unique_ptr<Task> task(reinterpret_cast<Task*>(raw_task));
        g_concurrency.fetch_add(1, std::memory_order_relaxed);
        std::apply(std::move(task->fn), std::move(task->args));
        g_concurrency.fetch_sub(1, std::memory_order_relaxed);
        return nullptr;
    };

    bthread_t tid;
    auto task = std::make_unique<Task>(fn, TaskArg(args...));
    if (int res = bthread_start_background(&tid, nullptr, task_fn, task.get()); res != 0) {
        LOG_FATAL("setup bthread").tag("msg", std::strerror(res));
    }
    task.release();
    return tid;
}

void run(selectdb::MetaService_Stub* service) {
    LOG_INFO("Prepare {} workloads", FLAGS_cmd);

    using TaskFn = void (*)(selectdb::MetaService_Stub*, size_t);
    TaskFn workload_fn = nullptr;
    if (FLAGS_cmd == "copy-into") {
        workload_fn = run_copy_into;
    } else if (FLAGS_cmd == "version") {
        bthread_list_t bthread_list;
        CHECK_EQ(bthread_list_init(&bthread_list, 0, 0), 0);
        for (size_t i = 0; i < FLAGS_concurrency; ++i) {
            auto tid = start_bthread(prepare_version_workload, service, i);
            CHECK_EQ(bthread_list_add(&bthread_list, tid), 0);
        }
        CHECK_EQ(bthread_list_join(&bthread_list), 0);
        bthread_list_destroy(&bthread_list);

        workload_fn = run_version;
    } else if (FLAGS_cmd == "read-meta") {
        bthread_list_t bthread_list;
        CHECK_EQ(bthread_list_init(&bthread_list, 0, 0), 0);
        for (size_t i = 0; i < FLAGS_concurrency; ++i) {
            auto tid = start_bthread(prepare_read_workload, service, i);
            CHECK_EQ(bthread_list_add(&bthread_list, tid), 0);
        }
        CHECK_EQ(bthread_list_join(&bthread_list), 0);
        bthread_list_destroy(&bthread_list);

        workload_fn = read_tablet_meta;
    } else {
        LOG_FATAL("UNKNOWN cmd").tag("cmd", FLAGS_cmd);
    }

    LOG_INFO("Run {} with {} concurrency, start intervals {} secs", FLAGS_cmd, FLAGS_concurrency,
             FLAGS_start_intervals);

    bthread_list_t bthread_list;
    CHECK_EQ(bthread_list_init(&bthread_list, 0, 0), 0);
    CHECK_EQ(bthread_list_add(&bthread_list, start_bthread(reporter)), 0);
    for (size_t i = 0;
         i < FLAGS_concurrency && g_next.load(std::memory_order_relaxed) < FLAGS_workload_num_op;
         ++i) {
        auto tid = start_bthread(workload_fn, service, i);
        CHECK_EQ(bthread_list_add(&bthread_list, tid), 0);
        if (FLAGS_start_intervals != 0) {
            std::this_thread::sleep_for(std::chrono::seconds(FLAGS_start_intervals));
        }
    }
    CHECK_EQ(bthread_list_join(&bthread_list), 0);
    bthread_list_destroy(&bthread_list);
}
