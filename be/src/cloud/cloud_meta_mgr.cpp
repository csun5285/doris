#include "cloud/cloud_meta_mgr.h"

#include <brpc/channel.h>
#include <brpc/controller.h>
#include <glog/logging.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <mutex>
#include <random>
#include <shared_mutex>
#include <vector>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "runtime/stream_load/stream_load_context.h"
#include "util/s3_util.h"

namespace doris::cloud {
using namespace ErrorCode;

bvar::LatencyRecorder g_get_rowset_latency("doris_CloudMetaMgr", "get_rowset");

static constexpr int BRPC_RETRY_TIMES = 3;

template <typename T, typename... Ts>
struct is_any : std::disjunction<std::is_same<T, Ts>...> {};

template <typename T, typename... Ts>
constexpr bool is_any_v = is_any<T, Ts...>::value;

template <class Req>
static std::string debug_info(const Req& req) {
    if constexpr (is_any_v<Req, selectdb::CommitTxnRequest, selectdb::AbortTxnRequest,
                           selectdb::PrecommitTxnRequest>) {
        return fmt::format(" txn_id={}", req.txn_id());
    } else if constexpr (is_any_v<Req, selectdb::StartTabletJobRequest,
                                  selectdb::FinishTabletJobRequest>) {
        return fmt::format(" tablet_id={}", req.job().idx().tablet_id());
    } else if constexpr (is_any_v<Req, selectdb::UpdateDeleteBitmapRequest>) {
        return fmt::format(" tablet_id={}, lock_id={}", req.tablet_id(), req.lock_id());
    } else if constexpr (is_any_v<Req, selectdb::GetDeleteBitmapUpdateLockRequest>) {
        return fmt::format(" partition_id={}, lock_id={}", req.partition_ids(0), req.lock_id());
    } else {
        static_assert(!sizeof(Req));
    }
}

class MetaServiceProxy {
public:
    static Status get_client(std::shared_ptr<selectdb::MetaService_Stub>* stub) {
        SYNC_POINT_RETURN_WITH_VALUE("MetaServiceProxy::get_client", Status::OK(), stub);
        return get_pooled_client(stub);
    }

private:
    static Status get_pooled_client(std::shared_ptr<selectdb::MetaService_Stub>* stub) {
        static std::once_flag proxies_flag;
        static size_t num_proxies = 1;
        static std::atomic<size_t> index(0);
        static std::unique_ptr<MetaServiceProxy[]> proxies;

        std::call_once(
                proxies_flag, +[]() {
                    if (config::meta_service_connection_pooled) {
                        num_proxies = config::meta_service_connection_pool_size;
                    }
                    proxies = std::make_unique<MetaServiceProxy[]>(num_proxies);
                });

        for (size_t i = 0; i + 1 < num_proxies; ++i) {
            size_t next_index = index.fetch_add(1, std::memory_order_relaxed) % num_proxies;
            Status s = proxies[next_index].get(stub);
            if (s.ok()) return Status::OK();
        }

        size_t next_index = index.fetch_add(1, std::memory_order_relaxed) % num_proxies;
        return proxies[next_index].get(stub);
    }

    static Status init_channel(brpc::Channel* channel) {
        static std::atomic<size_t> index = 1;

        size_t next_id = index.fetch_add(1, std::memory_order_relaxed);
        brpc::ChannelOptions options;
        options.connection_group = fmt::format("ms_{}", next_id);
        auto endpoint = config::meta_service_endpoint;
        int ret_code = 0;
        if (config::meta_service_use_load_balancer) {
            ret_code = channel->Init(endpoint.c_str(), config::rpc_load_balancer.c_str(), &options);
        } else {
            ret_code = channel->Init(endpoint.c_str(), &options);
        }
        if (ret_code != 0) {
            return Status::InternalError("fail to init brpc channel, endpoint: {}", endpoint);
        }
        return Status::OK();
    }

    Status get(std::shared_ptr<selectdb::MetaService_Stub>* stub) {
        using namespace std::chrono;

        auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
        {
            std::shared_lock lock(_mutex);
            if (_deadline_ms >= now) {
                *stub = _stub;
                return Status::OK();
            }
        }

        auto channel = std::make_unique<brpc::Channel>();
        RETURN_IF_ERROR(init_channel(channel.get()));
        *stub = std::make_shared<selectdb::MetaService_Stub>(
                channel.release(), google::protobuf::Service::STUB_OWNS_CHANNEL);

        long deadline = now;
        if (config::meta_service_connection_age_base_minutes > 0) {
            std::default_random_engine rng(static_cast<uint32_t>(now));
            std::uniform_int_distribution<> uni(
                    config::meta_service_connection_age_base_minutes,
                    config::meta_service_connection_age_base_minutes * 2);
            deadline = now + duration_cast<milliseconds>(minutes(uni(rng))).count();
        } else {
            deadline = LONG_MAX;
        }

        // Last one WIN
        std::unique_lock lock(_mutex);
        _deadline_ms = deadline;
        _stub = *stub;
        return Status::OK();
    }

    std::shared_mutex _mutex;
    long _deadline_ms {0};
    std::shared_ptr<selectdb::MetaService_Stub> _stub;
};

template <class Req, class Res, class RpcFn>
static Status retry_rpc(std::string_view op_name, const Req& req, Res& res, RpcFn rpc_fn) {
    int retry_times = 0;
    uint32_t duration_ms = 0;
    auto rng = std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<uint32_t> u(20, 200);
    std::uniform_int_distribution<uint32_t> u2(500, 1000);
    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        res.Clear();
        rpc_fn(stub.get(), &cntl, &req, &res, nullptr);
        if (cntl.Failed()) {
            if (retry_times >= config::meta_service_rpc_retry_times) {
                return Status::RpcError("failed to {}: {}", op_name, cntl.ErrorText());
            }
            duration_ms = retry_times <= 100 ? u(rng) : u2(rng);
            LOG(WARNING) << "failed to " << op_name << debug_info(req)
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << cntl.ErrorText();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        if (res.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (res.status().code() == selectdb::KV_TXN_CONFLICT) {
            duration_ms = retry_times <= 100 ? u(rng) : u2(rng);
            LOG(WARNING) << "failed to " << op_name << debug_info(req)
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << res.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        break;
    } while (++retry_times <= config::meta_service_rpc_retry_times);
    return Status::InternalError("failed to {}: {}", op_name, res.status().msg());
}

CloudMetaMgr::CloudMetaMgr() = default;

CloudMetaMgr::~CloudMetaMgr() = default;

Status CloudMetaMgr::open() {
    return Status::OK();
}

Status CloudMetaMgr::get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) {
    VLOG_DEBUG << "send GetTabletRequest, tablet_id: " << tablet_id;
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::get_tablet_meta", Status::OK(), tablet_id,
                                      tablet_meta);

    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int tried = 0;
TRY_AGAIN:
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetTabletRequest req;
    selectdb::GetTabletResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);
    stub->get_tablet(&cntl, &req, &resp, nullptr);
    int retry_times = config::meta_service_rpc_retry_times;
    if (cntl.Failed()) {
        if (tried++ < retry_times) {
            auto rng = std::default_random_engine(static_cast<uint32_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<uint32_t> u(20, 200);
            std::uniform_int_distribution<uint32_t> u1(500, 1000);
            uint32_t duration_ms = tried >= 100 ? u(rng) : u1(rng);
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
            LOG_INFO("failed to get tablet meta")
                    .tag("reason", cntl.ErrorText())
                    .tag("tablet_id", tablet_id)
                    .tag("tried", tried)
                    .tag("sleep", duration_ms);
            goto TRY_AGAIN;
        }
        return Status::RpcError("failed to get tablet meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
        return Status::NotFound("failed to get tablet meta: {}", resp.status().msg());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get tablet meta: {}", resp.status().msg());
    }
    *tablet_meta = std::make_shared<TabletMeta>();
    (*tablet_meta)->init_from_pb(resp.tablet_meta());
    VLOG_DEBUG << "get tablet meta, tablet_id: " << (*tablet_meta)->tablet_id();
    return Status::OK();
}

Status CloudMetaMgr::sync_tablet_rowsets(Tablet* tablet, bool need_download_data_async) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::sync_tablet_rowsets", Status::OK(), tablet);

    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int tried = 0;
TRY_AGAIN:

    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetRowsetRequest req;
    selectdb::GetRowsetResponse resp;

    int64_t tablet_id = tablet->tablet_id();
    int64_t table_id = tablet->table_id();
    int64_t index_id = tablet->index_id();
    req.set_cloud_unique_id(config::cloud_unique_id);
    auto idx = req.mutable_idx();
    idx->set_tablet_id(tablet_id);
    idx->set_table_id(table_id);
    idx->set_index_id(index_id);
    idx->set_partition_id(tablet->partition_id());
    {
        std::shared_lock rlock(tablet->get_header_lock());
        req.set_start_version(tablet->local_max_version() + 1);
        req.set_base_compaction_cnt(tablet->base_compaction_cnt());
        req.set_cumulative_compaction_cnt(tablet->cumulative_compaction_cnt());
        req.set_cumulative_point(tablet->cumulative_layer_point());
    }
    req.set_end_version(-1);
    VLOG_DEBUG << "send GetRowsetRequest: " << req.ShortDebugString();

    stub->get_rowset(&cntl, &req, &resp, nullptr);
    int64_t latency = cntl.latency_us();
    g_get_rowset_latency << latency;
    int retry_times = config::meta_service_rpc_retry_times;
    if (cntl.Failed()) {
        if (tried++ < retry_times) {
            auto rng = std::default_random_engine(static_cast<uint32_t>(
                    std::chrono::steady_clock::now().time_since_epoch().count()));
            std::uniform_int_distribution<uint32_t> u(20, 200);
            std::uniform_int_distribution<uint32_t> u1(500, 1000);
            uint32_t duration_ms = tried >= 100 ? u(rng) : u1(rng);
            std::this_thread::sleep_for(std::chrono::milliseconds(duration_ms));
            LOG_INFO("failed to get rowset meta")
                    .tag("reason", cntl.ErrorText())
                    .tag("tablet_id", tablet_id)
                    .tag("table_id", table_id)
                    .tag("index_id", index_id)
                    .tag("partition_id", tablet->partition_id())
                    .tag("tried", tried)
                    .tag("sleep", duration_ms);
            goto TRY_AGAIN;
        }
        return Status::RpcError("failed to get rowset meta: {}", cntl.ErrorText());
    }
    if (resp.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
        return Status::NotFound("failed to get rowset meta: {}", resp.status().msg());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get rowset meta: {}", resp.status().msg());
    }
    if (latency > 100 * 1000) { // 100ms
        LOG(INFO) << "finish get_rowset rpc. rowset_meta.size()=" << resp.rowset_meta().size()
                  << ", latency=" << latency << "us";
    } else {
        LOG_EVERY_N(INFO, 100) << "finish get_rowset rpc. rowset_meta.size()="
                               << resp.rowset_meta().size() << ", latency=" << latency << "us";
    }

    using namespace std::chrono;
    int64_t now = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();
    tablet->set_last_sync_time(now);

    if (tablet->enable_unique_key_merge_on_write()) {
        DeleteBitmap delete_bitmap(tablet_id);
        int64_t old_max_version = req.start_version() - 1;
        RETURN_IF_ERROR(sync_tablet_delete_bitmap(tablet, old_max_version, resp.rowset_meta(),
                                                  &delete_bitmap));
        tablet->tablet_meta()->delete_bitmap().merge(delete_bitmap);
    }

    {
        auto& stats = resp.stats();
        std::lock_guard wlock(tablet->get_header_lock());

        // ATTN: we are facing following data race
        //
        // resp_base_compaction_cnt=0|base_compaction_cnt=0|resp_cumulative_compaction_cnt=0|cumulative_compaction_cnt=1|resp_max_version=11|max_version=8
        //
        //   BE-compaction-thread                 meta-service                                     BE-query-thread
        //            |                                |                                                |
        //    local   |    commit cumu-compaction      |                                                |
        //   cc_cnt=0 |  --------------------------->  |     sync rowset (long rpc, local cc_cnt=0 )    |   local
        //            |                                |  <-----------------------------------------    |  cc_cnt=0
        //            |                                |  -.                                            |
        //    local   |       done cc_cnt=1            |    \                                           |
        //   cc_cnt=1 |  <---------------------------  |     \                                          |
        //            |                                |      \  returned with resp cc_cnt=0 (snapshot) |
        //            |                                |       '------------------------------------>   |   local
        //            |                                |                                                |  cc_cnt=1
        //            |                                |                                                |
        //            |                                |                                                |  CHECK FAIL
        //            |                                |                                                |  need retry
        // To get rid of just retry syncing tablet
        if (stats.base_compaction_cnt() < tablet->base_compaction_cnt() ||
            stats.cumulative_compaction_cnt() < tablet->cumulative_compaction_cnt()) [[unlikely]] {
            // stale request, ignore
            LOG_WARNING("stale get rowset meta request")
                    .tag("resp_base_compaction_cnt", stats.base_compaction_cnt())
                    .tag("base_compaction_cnt", tablet->base_compaction_cnt())
                    .tag("resp_cumulative_compaction_cnt", stats.cumulative_compaction_cnt())
                    .tag("cumulative_compaction_cnt", tablet->cumulative_compaction_cnt())
                    .tag("tried", tried);
            if (tried++ < 10) goto TRY_AGAIN;
            return Status::OK();
        }
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.reserve(resp.rowset_meta().size());
        for (auto& meta_pb : resp.rowset_meta()) {
            VLOG_DEBUG << "get rowset meta, tablet_id=" << meta_pb.tablet_id() << ", version=["
                       << meta_pb.start_version() << '-' << meta_pb.end_version() << ']';
            auto existed_rowset =
                    tablet->get_rowset_by_version({meta_pb.start_version(), meta_pb.end_version()});
            if (existed_rowset &&
                existed_rowset->rowset_id().to_string() == meta_pb.rowset_id_v2()) {
                continue; // Same rowset, skip it
            }
            auto rs_meta = std::make_shared<RowsetMeta>(table_id, index_id);
            rs_meta->init_from_pb(meta_pb);
            RowsetSharedPtr rowset;
            // schema is nullptr implies using RowsetMeta.tablet_schema
            RowsetFactory::create_rowset(nullptr, tablet->tablet_path(), std::move(rs_meta),
                                         &rowset);
            rowsets.push_back(std::move(rowset));
        }
        if (!rowsets.empty()) {
            // `rowsets.empty()` could happen after doing EMPTY_CUMULATIVE compaction. e.g.:
            //   BE has [0-1][2-11][12-12], [12-12] is delete predicate, cp is 2;
            //   after doing EMPTY_CUMULATIVE compaction, MS cp is 13, get_rowset will return [2-11][12-12].
            bool version_overlap = tablet->local_max_version() >= rowsets.front()->start_version();
            tablet->cloud_add_rowsets(std::move(rowsets), version_overlap,
                                      need_download_data_async);
        }
        tablet->set_base_compaction_cnt(stats.base_compaction_cnt());
        tablet->set_cumulative_compaction_cnt(stats.cumulative_compaction_cnt());
        tablet->set_cumulative_layer_point(stats.cumulative_point());
        tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(), stats.num_rows(),
                                        stats.data_size());
    }
    return Status::OK();
}

Status CloudMetaMgr::sync_tablet_delete_bitmap(
        const Tablet* tablet, int64_t old_max_version,
        const google::protobuf::RepeatedPtrField<RowsetMetaPB>& rs_metas,
        DeleteBitmap* delete_bitmap) {
    if (rs_metas.empty()) {
        return Status::OK();
    }

    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    int64_t new_max_version = std::max(old_max_version, rs_metas.rbegin()->end_version());
    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetDeleteBitmapRequest req;
    selectdb::GetDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet->tablet_id());
    // New rowset sync all versions of delete bitmap
    for (auto& rs_meta : rs_metas) {
        req.add_rowset_ids(rs_meta.rowset_id_v2());
        req.add_begin_versions(0);
        req.add_end_versions(new_max_version);
    }

    // old rowset sync incremental versions of delete bitmap
    if (old_max_version < new_max_version) {
        RowsetIdUnorderedSet all_rs_ids = tablet->all_rs_id(old_max_version);
        for (auto& rs_id : all_rs_ids) {
            req.add_rowset_ids(rs_id.to_string());
            req.add_begin_versions(old_max_version + 1);
            req.add_end_versions(new_max_version);
        }
    }
    stub->get_delete_bitmap(&cntl, &req, &res, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get delete bitmap: {}", cntl.ErrorText());
    }
    if (res.status().code() == selectdb::MetaServiceCode::TABLET_NOT_FOUND) {
        return Status::NotFound("failed to get delete bitmap: {}", res.status().msg());
    }
    if (res.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get delete bitmap: {}", res.status().msg());
    }
    auto& rowset_ids = res.rowset_ids();
    auto& segment_ids = res.segment_ids();
    auto& vers = res.versions();
    auto& delete_bitmaps = res.segment_delete_bitmaps();
    for (size_t i = 0; i < rowset_ids.size(); i++) {
        RowsetId rst_id;
        rst_id.init(rowset_ids[i]);
        delete_bitmap->merge({rst_id, segment_ids[i], vers[i]},
                             roaring::Roaring::read(delete_bitmaps[i].data()));
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                    RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "prepare rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta(), true);
    req.set_temporary(is_tmp);
    int retry_times = 0;
    auto rng = std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<uint32_t> u(20, 200);
    std::uniform_int_distribution<uint32_t> u1(500, 1000);
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        stub->prepare_rowset(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (retry_times >= config::meta_service_rpc_retry_times) {
                return Status::RpcError("failed to prepare rowset: {}", cntl.ErrorText());
            }
            uint32_t duration_ms = retry_times <= 100 ? u(rng) : u1(rng);
            LOG(WARNING) << "failed to prepare rowset, tablet_id=" << rs_meta->tablet_id()
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << resp.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        if (resp.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (resp.status().code() == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
                *existed_rs_meta =
                        std::make_shared<RowsetMeta>(rs_meta->table_id(), rs_meta->index_id());
                (*existed_rs_meta)->init_from_pb(resp.existed_rowset_meta());
            }
            return Status::AlreadyExist("failed to prepare rowset: {}", resp.status().msg());
        } else if (resp.status().code() == selectdb::MetaServiceCode::KV_TXN_CONFLICT) {
            uint32_t duration_ms = retry_times <= 100 ? u(rng) : u1(rng);
            LOG(WARNING) << "failed to prepare rowset, tablet_id=" << rs_meta->tablet_id()
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << resp.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        break;
    } while (++retry_times <= config::meta_service_rpc_retry_times);
    return Status::InternalError("failed to prepare rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_rowset(const RowsetMeta* rs_meta, bool is_tmp,
                                   RowsetMetaSharedPtr* existed_rs_meta) {
    VLOG_DEBUG << "commit rowset, tablet_id: " << rs_meta->tablet_id()
               << ", rowset_id: " << rs_meta->rowset_id() << ", is_tmp: " << is_tmp;
    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    selectdb::CreateRowsetRequest req;
    selectdb::CreateRowsetResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    rs_meta->to_rowset_pb(req.mutable_rowset_meta());
    req.set_temporary(is_tmp);
    int retry_times = 0;
    auto rng = std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<uint32_t> u(20, 200);
    std::uniform_int_distribution<uint32_t> u1(500, 1000);
    do {
        brpc::Controller cntl;
        cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
        cntl.set_max_retry(BRPC_RETRY_TIMES);
        stub->commit_rowset(&cntl, &req, &resp, nullptr);
        if (cntl.Failed()) {
            if (retry_times >= config::meta_service_rpc_retry_times) {
                return Status::RpcError("failed to commit rowset: {}", cntl.ErrorText());
            }
            uint32_t duration_ms = retry_times <= 100 ? u(rng) : u1(rng);
            LOG(WARNING) << "failed to commit rowset, tablet_id=" << rs_meta->tablet_id()
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << resp.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        if (resp.status().code() == selectdb::MetaServiceCode::OK) {
            return Status::OK();
        } else if (resp.status().code() == selectdb::MetaServiceCode::ALREADY_EXISTED) {
            if (existed_rs_meta != nullptr && resp.has_existed_rowset_meta()) {
                *existed_rs_meta =
                        std::make_shared<RowsetMeta>(rs_meta->table_id(), rs_meta->index_id());
                (*existed_rs_meta)->init_from_pb(resp.existed_rowset_meta());
            }
            return Status::AlreadyExist("failed to commit rowset: {}", resp.status().msg());
        } else if (resp.status().code() == selectdb::MetaServiceCode::KV_TXN_CONFLICT) {
            uint32_t duration_ms = retry_times <= 100 ? u(rng) : u1(rng);
            LOG(WARNING) << "failed to commit rowset, tablet_id=" << rs_meta->tablet_id()
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << resp.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        }
        break;
    } while (++retry_times <= config::meta_service_rpc_retry_times);
    return Status::InternalError("failed to commit rowset: {}", resp.status().msg());
}

Status CloudMetaMgr::commit_txn(StreamLoadContext* ctx, bool is_2pc) {
    VLOG_DEBUG << "commit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label << ", is_2pc: " << is_2pc;
    selectdb::CommitTxnRequest req;
    selectdb::CommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    req.set_is_2pc(is_2pc);
    return retry_rpc("commit txn", req, res, std::mem_fn(&selectdb::MetaService_Stub::commit_txn));
}

Status CloudMetaMgr::abort_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "abort txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    selectdb::AbortTxnRequest req;
    selectdb::AbortTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    if (ctx->db_id > 0 && !ctx->label.empty()) {
        req.set_db_id(ctx->db_id);
        req.set_label(ctx->label);
    } else {
        req.set_txn_id(ctx->txn_id);
    }
    return retry_rpc("abort txn", req, res, std::mem_fn(&selectdb::MetaService_Stub::abort_txn));
}

Status CloudMetaMgr::precommit_txn(StreamLoadContext* ctx) {
    VLOG_DEBUG << "precommit txn, db_id: " << ctx->db_id << ", txn_id: " << ctx->txn_id
               << ", label: " << ctx->label;
    selectdb::PrecommitTxnRequest req;
    selectdb::PrecommitTxnResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_db_id(ctx->db_id);
    req.set_txn_id(ctx->txn_id);
    return retry_rpc("precommit txn", req, res,
                     std::mem_fn(&selectdb::MetaService_Stub::precommit_txn));
}

Status CloudMetaMgr::get_s3_info(std::vector<std::tuple<std::string, S3Conf>>* s3_infos) {
    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::GetObjStoreInfoRequest req;
    selectdb::GetObjStoreInfoResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    stub->get_obj_store_info(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to get s3 info: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to get s3 info: {}", resp.status().msg());
    }
    for (auto& obj_store : resp.obj_info()) {
        S3Conf s3_conf;
        s3_conf.ak = obj_store.ak();
        s3_conf.sk = obj_store.sk();
        s3_conf.endpoint = obj_store.endpoint();
        s3_conf.region = obj_store.region();
        s3_conf.bucket = obj_store.bucket();
        s3_conf.prefix = obj_store.prefix();
        s3_conf.sse_enabled = obj_store.sse_enabled();
        s3_conf.provider = obj_store.provider();
        s3_infos->emplace_back(obj_store.id(), std::move(s3_conf));
    }
    return Status::OK();
}

Status CloudMetaMgr::prepare_tablet_job(const selectdb::TabletJobInfoPB& job,
                                        selectdb::StartTabletJobResponse* res) {
    VLOG_DEBUG << "prepare_tablet_job: " << job.ShortDebugString();
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::prepare_tablet_job", Status::OK(), job, res);

    selectdb::StartTabletJobRequest req;
    req.mutable_job()->CopyFrom(job);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("start tablet job", req, *res,
                     std::mem_fn(&selectdb::MetaService_Stub::start_tablet_job));
}

Status CloudMetaMgr::commit_tablet_job(const selectdb::TabletJobInfoPB& job,
                                       selectdb::FinishTabletJobResponse* res) {
    VLOG_DEBUG << "commit_tablet_job: " << job.ShortDebugString();
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudMetaMgr::commit_tablet_job", Status::OK(), job, res);

    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    selectdb::FinishTabletJobRequest req;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::COMMIT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("commit tablet job", req, *res,
                     std::mem_fn(&selectdb::MetaService_Stub::finish_tablet_job));
}

Status CloudMetaMgr::abort_tablet_job(const selectdb::TabletJobInfoPB& job) {
    VLOG_DEBUG << "abort_tablet_job: " << job.ShortDebugString();
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::ABORT);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("abort tablet job", req, res,
                     std::mem_fn(&selectdb::MetaService_Stub::finish_tablet_job));
}

Status CloudMetaMgr::lease_tablet_job(const selectdb::TabletJobInfoPB& job) {
    VLOG_DEBUG << "lease_tablet_job: " << job.ShortDebugString();
    selectdb::FinishTabletJobRequest req;
    selectdb::FinishTabletJobResponse res;
    req.mutable_job()->CopyFrom(job);
    req.set_action(selectdb::FinishTabletJobRequest::LEASE);
    req.set_cloud_unique_id(config::cloud_unique_id);
    return retry_rpc("lease tablet job", req, res,
                     std::mem_fn(&selectdb::MetaService_Stub::finish_tablet_job));
}

Status CloudMetaMgr::update_tablet_schema(int64_t tablet_id, const TabletSchema* tablet_schema) {
    VLOG_DEBUG << "send UpdateTabletSchemaRequest, tablet_id: " << tablet_id;

    std::shared_ptr<selectdb::MetaService_Stub> stub;
    RETURN_IF_ERROR(MetaServiceProxy::get_client(&stub));

    brpc::Controller cntl;
    cntl.set_timeout_ms(config::meta_service_brpc_timeout_ms);
    selectdb::UpdateTabletSchemaRequest req;
    selectdb::UpdateTabletSchemaResponse resp;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_tablet_id(tablet_id);

    TabletSchemaPB tablet_schema_pb;
    tablet_schema->to_schema_pb(&tablet_schema_pb);
    req.mutable_tablet_schema()->CopyFrom(tablet_schema_pb);
    stub->update_tablet_schema(&cntl, &req, &resp, nullptr);
    if (cntl.Failed()) {
        return Status::RpcError("failed to update tablet schema: {}", cntl.ErrorText());
    }
    if (resp.status().code() != selectdb::MetaServiceCode::OK) {
        return Status::InternalError("failed to update tablet schema: {}", resp.status().msg());
    }
    VLOG_DEBUG << "succeed to update tablet schema, tablet_id: " << tablet_id;
    return Status::OK();
}

Status CloudMetaMgr::update_delete_bitmap(const Tablet* tablet, int64_t lock_id, int64_t initiator,
                                          DeleteBitmap* delete_bitmap) {
    VLOG_DEBUG << "update_delete_bitmpap , tablet_id: " << tablet->tablet_id();
    selectdb::UpdateDeleteBitmapRequest req;
    selectdb::UpdateDeleteBitmapResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet->table_id());
    req.set_partition_id(tablet->partition_id());
    req.set_tablet_id(tablet->tablet_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    for (auto iter = delete_bitmap->delete_bitmap.begin();
         iter != delete_bitmap->delete_bitmap.end(); ++iter) {
        req.add_rowset_ids(std::get<0>(iter->first).to_string());
        req.add_segment_ids(std::get<1>(iter->first));
        req.add_versions(std::get<2>(iter->first));
        // To save space, convert array and bitmap containers to run containers
        iter->second.runOptimize();
        std::string bitmap_data(iter->second.getSizeInBytes(), '\0');
        iter->second.write(bitmap_data.data());
        *(req.add_segment_delete_bitmaps()) = std::move(bitmap_data);
    }
    return retry_rpc("update delete bitmap", req, res,
                     std::mem_fn(&selectdb::MetaService_Stub::update_delete_bitmap));
}

Status CloudMetaMgr::get_delete_bitmap_update_lock(const Tablet* tablet, int64_t lock_id,
                                                   int64_t initiator) {
    VLOG_DEBUG << "get_delete_bitmap_update_lock , tablet_id: " << tablet->tablet_id();
    selectdb::GetDeleteBitmapUpdateLockRequest req;
    selectdb::GetDeleteBitmapUpdateLockResponse res;
    req.set_cloud_unique_id(config::cloud_unique_id);
    req.set_table_id(tablet->table_id());
    req.add_partition_ids(tablet->partition_id());
    req.set_lock_id(lock_id);
    req.set_initiator(initiator);
    req.set_expiration(10); // 10s expiration time for compaction and schema_change
    int retry_times = 0;
    Status st;
    uint32_t duration_ms = 0;
    auto rng = std::default_random_engine(
            static_cast<uint32_t>(std::chrono::steady_clock::now().time_since_epoch().count()));
    std::uniform_int_distribution<uint32_t> u(500, 2000);
    do {
        st = retry_rpc("get delete bitmap update lock", req, res,
                       std::mem_fn(&selectdb::MetaService_Stub::get_delete_bitmap_update_lock));
        if (res.status().code() == selectdb::LOCK_CONFLICT) {
            duration_ms = u(rng);
            LOG(WARNING) << "get delete bitmap lock conflict. " << debug_info(req)
                         << " retry_times=" << retry_times << " sleep=" << duration_ms
                         << "ms : " << res.status().msg();
            bthread_usleep(duration_ms * 1000);
            continue;
        } else if (!st.ok()) { // Other errors, return error status
            break;
        }
    } while (++retry_times <= 100);
    return st;
}

} // namespace doris::cloud

// vim: et tw=100 ts=4 sw=4 cc=80:
