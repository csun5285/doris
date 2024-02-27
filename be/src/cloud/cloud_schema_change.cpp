#include "cloud/cloud_schema_change.h"

#include <gen_cpp/selectdb_cloud.pb.h>

#include "cloud/cloud_tablet_mgr.h"
#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#include "olap/delete_handler.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "service/backend_options.h"

namespace doris::cloud {
using namespace ErrorCode;

static constexpr int ALTER_TABLE_BATCH_SIZE = 4096;
static constexpr int SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID = -2;

static std::unique_ptr<SchemaChange> get_sc_procedure(const BlockChanger& changer,
                                                      bool sc_sorting) {
    if (sc_sorting) {
        return std::make_unique<VSchemaChangeWithSorting>(
                changer, config::memory_limitation_per_thread_for_schema_change_bytes);
    }
    // else sc_directly
    return std::make_unique<VSchemaChangeDirectly>(changer);
}

CloudSchemaChange::CloudSchemaChange(std::string job_id, int64_t expiration)
        : _job_id(std::move(job_id)), _expiration(expiration) {}

CloudSchemaChange::~CloudSchemaChange() = default;

Status CloudSchemaChange::process_alter_tablet(const TAlterTabletReqV2& request) {
    LOG(INFO) << "Begin to alter tablet. base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version << ", job_id=" << _job_id;

    // new tablet has to exist
    TabletSharedPtr new_tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(request.new_tablet_id, &new_tablet));
    if (new_tablet->tablet_state() == TABLET_RUNNING) {
        LOG(INFO) << "schema change job has already finished. base_tablet_id="
                  << request.base_tablet_id << ", new_tablet_id=" << request.new_tablet_id
                  << ", alter_version=" << request.alter_version << ", job_id=" << _job_id;
        return Status::OK();
    }

    TabletSharedPtr base_tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(request.base_tablet_id, &base_tablet));
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "Failed to obtain schema change lock. base_tablet="
                     << request.base_tablet_id;
        return Status::Error<TRY_LOCK_FAILED>("Failed to obtain schema change lock. base_tablet={}",
                                              request.base_tablet_id);
    }

    // MUST sync rowsets before capturing rowset readers and building DeleteHandler
    RETURN_IF_ERROR(base_tablet->cloud_sync_rowsets(request.alter_version));
    // ATTN: Only convert rowsets of version larger than 1, MUST let the new tablet cache have rowset [0-1]
    _output_cumulative_point = base_tablet->cumulative_layer_point();

    std::vector<RowSetSplits> rs_splits;
    int64_t base_max_version = base_tablet->local_max_version();
    if (request.alter_version > 1) {
        // [0-1] is a placeholder rowset, no need to convert
        RETURN_IF_ERROR(base_tablet->cloud_capture_rs_readers({2, base_max_version}, &rs_splits));
    }
    // FIXME(cyx): Should trigger compaction on base_tablet if there are too many rowsets to convert.

    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->update_tablet_columns(*base_tablet->tablet_schema(), request.columns);

    // delete handlers to filter out deleted rows
    DeleteHandler delete_handler;
    std::vector<RowsetMetaSharedPtr> delete_predicates;
    for (auto& split : rs_splits) {
        auto& rs_meta = split.rs_reader->rowset()->rowset_meta();
        if (rs_meta->has_delete_predicate()) {
            base_tablet_schema->merge_dropped_columns(*rs_meta->tablet_schema());
            delete_predicates.push_back(rs_meta);
        }
    }
    RETURN_IF_ERROR(delete_handler.init(base_tablet_schema, delete_predicates, base_max_version));

    std::vector<ColumnId> return_columns;
    return_columns.resize(base_tablet_schema->num_columns());
    std::iota(return_columns.begin(), return_columns.end(), 0);

    // reader_context is stack variables, it's lifetime MUST keep the same with rs_readers
    RowsetReaderContext reader_context;
    reader_context.reader_type = ReaderType::READER_ALTER_TABLE;
    reader_context.tablet_schema = base_tablet_schema;
    reader_context.need_ordered_result = true;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
    reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
    reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
    reader_context.delete_bitmap = &base_tablet->tablet_meta()->delete_bitmap();
    reader_context.version = Version(0, base_max_version);

    for (auto& split : rs_splits) {
        RETURN_IF_ERROR(split.rs_reader->init(&reader_context));
    }

    SchemaChangeParams sc_params;

    DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl);
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.ref_rowset_readers.reserve(rs_splits.size());
    for (RowSetSplits& split : rs_splits) {
        sc_params.ref_rowset_readers.emplace_back(std::move(split.rs_reader));
    }
    sc_params.delete_handler = &delete_handler;
    sc_params.base_tablet_schema = base_tablet_schema;
    sc_params.be_exec_version = request.be_exec_version;
    DCHECK(request.__isset.alter_tablet_type);
    switch (request.alter_tablet_type) {
    case TAlterTabletType::SCHEMA_CHANGE:
        sc_params.alter_tablet_type = AlterTabletType::SCHEMA_CHANGE;
        break;
    case TAlterTabletType::ROLLUP:
        sc_params.alter_tablet_type = AlterTabletType::ROLLUP;
        break;
    case TAlterTabletType::MIGRATION:
        sc_params.alter_tablet_type = AlterTabletType::MIGRATION;
        break;
    }
    if (!request.__isset.materialized_view_params) {
        return _convert_historical_rowsets(sc_params);
    }
    for (auto item : request.materialized_view_params) {
        AlterMaterializedViewParam mv_param;
        mv_param.column_name = item.column_name;
        /*
         * origin_column_name is always be set now,
         * but origin_column_name may be not set in some materialized view function. eg:count(1)
        */
        if (item.__isset.origin_column_name) {
            mv_param.origin_column_name = item.origin_column_name;
        }

        if (item.__isset.mv_expr) {
            mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
        }
        sc_params.materialized_params_map.insert(std::make_pair(item.column_name, mv_param));
    }
    return _convert_historical_rowsets(sc_params);
}

Status CloudSchemaChange::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "Begin to convert historical rowsets for new_tablet from base_tablet. base_tablet="
              << sc_params.base_tablet->tablet_id()
              << ", new_tablet=" << sc_params.new_tablet->tablet_id() << ", job_id=" << _job_id;

    auto& new_tablet = sc_params.new_tablet;

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    BlockChanger changer(sc_params.new_tablet->tablet_schema(), *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // 1. Parse the Alter request and convert it into an internal representation
    RETURN_IF_ERROR(
            SchemaChangeHandler::parse_request(sc_params, &changer, &sc_sorting, &sc_directly));
    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        LOG(INFO) << "Don't support to add materialized view by linked schema change";
        return Status::InternalError(
                "Don't support to add materialized view by linked schema change");
    }

    // 2. Generate historical data converter
    auto sc_procedure = get_sc_procedure(changer, sc_sorting);

    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(sc_params.base_tablet->tablet_id());
    idx->set_table_id(sc_params.base_tablet->table_id());
    idx->set_index_id(sc_params.base_tablet->index_id());
    idx->set_partition_id(sc_params.base_tablet->partition_id());
    auto sc_job = job.mutable_schema_change();
    sc_job->set_id(_job_id);
    sc_job->set_initiator(BackendOptions::get_localhost() + ':' +
                          std::to_string(config::heartbeat_service_port));
    auto new_tablet_idx = sc_job->mutable_new_tablet_idx();
    new_tablet_idx->set_tablet_id(new_tablet->tablet_id());
    new_tablet_idx->set_table_id(new_tablet->table_id());
    new_tablet_idx->set_index_id(new_tablet->index_id());
    new_tablet_idx->set_partition_id(new_tablet->partition_id());
    selectdb::StartTabletJobResponse start_resp;
    auto st = cloud::meta_mgr()->prepare_tablet_job(job, &start_resp);
    if (!st.ok()) {
        if (start_resp.status().code() == selectdb::JOB_ALREADY_SUCCESS) {
            st = new_tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }

    // 3. Convert historical data
    bool already_exist_any_version = false;
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "Begin to convert a history rowset. version=" << rs_reader->version();

        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.is_persistent = new_tablet->is_persistent();
        context.ttl_seconds = new_tablet->ttl_seconds();
        context.txn_id = rs_reader->rowset()->txn_id();
        context.txn_expiration = _expiration;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = new_tablet->tablet_schema();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.fs = cloud::latest_fs();
        RETURN_IF_ERROR(new_tablet->create_rowset_writer(context, &rowset_writer));

        RowsetMetaSharedPtr existed_rs_meta;
        auto st = meta_mgr()->prepare_rowset(rowset_writer->rowset_meta().get(), &existed_rs_meta);
        if (!st.ok()) {
            if (st.is<ALREADY_EXIST>()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RowsetFactory::create_rowset(nullptr, sc_params.new_tablet->tablet_path(),
                                             std::move(existed_rs_meta), &rowset);
                _output_rowsets.push_back(std::move(rowset));
                already_exist_any_version = true;
                continue;
            } else {
                return st;
            }
        }

        RETURN_IF_ERROR(sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                              sc_params.base_tablet, sc_params.base_tablet_schema));

        RowsetSharedPtr new_rowset;
        st = rowset_writer->build(new_rowset);
        if (!st.ok()) {
            return Status::InternalError("failed to build rowset, version=[{}-{}] status={}",
                                         rs_reader->version().first, rs_reader->version().second,
                                         st.to_string());
        }

        st = meta_mgr()->commit_rowset(rowset_writer->rowset_meta().get(), &existed_rs_meta);
        if (!st.ok()) {
            if (st.is<ALREADY_EXIST>()) {
                LOG(INFO) << "Rowset " << rs_reader->version() << " has already existed in tablet "
                          << new_tablet->tablet_id();
                // Add already committed rowset to _output_rowsets.
                DCHECK(existed_rs_meta != nullptr);
                RowsetSharedPtr rowset;
                // schema is nullptr implies using RowsetMeta.tablet_schema
                RowsetFactory::create_rowset(nullptr, sc_params.new_tablet->tablet_path(),
                                             std::move(existed_rs_meta), &rowset);
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }
        _output_rowsets.push_back(std::move(new_rowset));

        VLOG_TRACE << "Successfully convert a history version " << rs_reader->version();
    }

    if (sc_params.ref_rowset_readers.empty()) {
        sc_job->set_alter_version(1); // no rowset to convert implies alter_version == 1
    } else {
        int64_t num_output_rows = 0;
        int64_t size_output_rowsets = 0;
        int64_t num_output_segments = 0;
        for (auto& rs : _output_rowsets) {
            sc_job->add_txn_ids(rs->txn_id());
            sc_job->add_output_versions(rs->end_version());
            num_output_rows += rs->num_rows();
            size_output_rowsets += rs->data_disk_size();
            num_output_segments += rs->num_segments();
        }
        sc_job->set_num_output_rows(num_output_rows);
        sc_job->set_size_output_rowsets(size_output_rowsets);
        sc_job->set_num_output_segments(num_output_segments);
        sc_job->set_num_output_rowsets(_output_rowsets.size());
        sc_job->set_alter_version(_output_rowsets.back()->end_version());
    }
    _output_cumulative_point = std::min(_output_cumulative_point, sc_job->alter_version() + 1);
    sc_job->set_output_cumulative_point(_output_cumulative_point);

    if (new_tablet->enable_unique_key_merge_on_write()) {
        int64_t initiator = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                            std::numeric_limits<int64_t>::max();
        // If there are historical versions of rowsets, we need to recalculate their delete
        // bitmaps, otherwise we will miss the delete bitmaps of incremental rowsets
        int64_t start_calc_delete_bitmap_version =
                already_exist_any_version ? 0 : sc_job->alter_version() + 1;
        RETURN_IF_ERROR(_process_delete_bitmap(new_tablet, sc_job->alter_version(),
                                               start_calc_delete_bitmap_version, initiator));
        sc_job->set_delete_bitmap_lock_initiator(initiator);
    }

    selectdb::FinishTabletJobResponse finish_resp;
    st = cloud::meta_mgr()->commit_tablet_job(job, &finish_resp);
    if (!st.ok()) {
        if (finish_resp.status().code() == selectdb::JOB_ALREADY_SUCCESS) {
            st = new_tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", new_tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }
    auto& stats = finish_resp.stats();
    {
        std::lock_guard wlock(new_tablet->get_header_lock());
        // new_tablet's state MUST be `TABLET_NOTREADY`, because we won't sync a new tablet in schema change job
        DCHECK(new_tablet->tablet_state() == TABLET_NOTREADY);
        if (new_tablet->tablet_state() != TABLET_NOTREADY) [[unlikely]] {
            LOG(ERROR) << "invalid tablet state, tablet_id=" << new_tablet->tablet_id();
            Status::InternalError("invalid tablet state, tablet_id={}", new_tablet->tablet_id());
        }
        new_tablet->cloud_add_rowsets(std::move(_output_rowsets), true);
        new_tablet->set_cumulative_layer_point(_output_cumulative_point);
        new_tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                            stats.num_rows(), stats.data_size());
        new_tablet->set_tablet_state(TABLET_RUNNING);
    }
    return Status::OK();
}

Status CloudSchemaChange::_process_delete_bitmap(TabletSharedPtr new_tablet, int64_t alter_version,
                                                 int64_t start_calc_delete_bitmap_version,
                                                 int64_t initiator) {
    LOG_INFO("process mow table")
            .tag("new_tablet_id", new_tablet->tablet_id())
            .tag("out_rowset_size", _output_rowsets.size())
            .tag("start_calc_delete_bitmap_version", start_calc_delete_bitmap_version)
            .tag("alter_version", alter_version);
    TabletMetaSharedPtr tmp_meta = std::make_shared<TabletMeta>(*new_tablet->tablet_meta());
    tmp_meta->delete_bitmap().delete_bitmap.clear();
    TabletSharedPtr tmp_tablet = std::make_shared<Tablet>(tmp_meta, nullptr);
    tmp_tablet->cloud_add_rowsets(_output_rowsets, true);

    // step 1, process incremental rowset without delete bitmap update lock
    std::vector<RowsetSharedPtr> incremental_rowsets;
    RETURN_IF_ERROR(cloud::meta_mgr()->sync_tablet_rowsets(tmp_tablet.get()));
    int64_t max_version = tmp_tablet->max_version().second;
    LOG(INFO) << "alter table for mow table, calculate delete bitmap of "
              << "incremental rowsets without lock, version: " << start_calc_delete_bitmap_version
              << "-" << max_version << " new_table_id: " << new_tablet->tablet_id();
    if (max_version >= start_calc_delete_bitmap_version) {
        RETURN_IF_ERROR(tmp_tablet->capture_consistent_rowsets(
                {start_calc_delete_bitmap_version, max_version}, &incremental_rowsets));
        for (auto rowset : incremental_rowsets) {
            tmp_tablet->update_delete_bitmap_without_lock(rowset);
        }
    }

    // step 2, process incremental rowset with delete bitmap update lock
    RETURN_IF_ERROR(cloud::meta_mgr()->get_delete_bitmap_update_lock(
            tmp_tablet.get(), SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID, initiator));
    RETURN_IF_ERROR(cloud::meta_mgr()->sync_tablet_rowsets(tmp_tablet.get()));
    int64_t new_max_version = tmp_tablet->max_version().second;
    LOG(INFO) << "alter table for mow table, calculate delete bitmap of "
              << "incremental rowsets with lock, version: " << max_version + 1 << "-"
              << new_max_version << " new_tablet_id: " << new_tablet->tablet_id();
    std::vector<RowsetSharedPtr> new_incremental_rowsets;
    if (new_max_version > max_version) {
        RETURN_IF_ERROR(tmp_tablet->capture_consistent_rowsets({max_version + 1, new_max_version},
                                                               &new_incremental_rowsets));
        tmp_tablet->cloud_add_rowsets(new_incremental_rowsets, false);
        for (auto rowset : new_incremental_rowsets) {
            tmp_tablet->update_delete_bitmap_without_lock(rowset);
        }
    }

    auto& delete_bitmap = tmp_tablet->tablet_meta()->delete_bitmap();

    // step4, store delete bitmap
    RETURN_IF_ERROR(cloud::meta_mgr()->update_delete_bitmap(
            tmp_tablet.get(), SCHEMA_CHANGE_DELETE_BITMAP_LOCK_ID, initiator, &delete_bitmap));

    new_tablet->tablet_meta()->delete_bitmap() = delete_bitmap;
    return Status::OK();
}

Status CloudSchemaChange::process_alter_inverted_index(const TAlterInvertedIndexReq& request) {
    CHECK(false) << "unused function";
    return Status::OK();
}

Status CloudSchemaChange::_do_process_alter_inverted_index(TabletSharedPtr tablet,
                                                           const TAlterInvertedIndexReq& request) {
    CHECK(false) << "unused function";
    return Status::OK();
}

Status CloudSchemaChange::_add_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                                              DeleteHandler* delete_handler,
                                              const TabletSchemaSPtr& tablet_schema,
                                              TabletSharedPtr tablet,
                                              const TAlterInvertedIndexReq& request) {
    CHECK(false) << "unused function";
    return Status::OK();
}

Status CloudSchemaChange::_drop_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                                               const TabletSchemaSPtr& tablet_schema,
                                               TabletSharedPtr tablet,
                                               const TAlterInvertedIndexReq& request) {
    CHECK(false) << "unused function";
    return Status::OK();
}

} // namespace doris::cloud
