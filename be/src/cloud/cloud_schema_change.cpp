#include "cloud/cloud_schema_change.h"

#include "cloud/utils.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

static constexpr int ALTER_TABLE_BATCH_SIZE = 4096;

static std::unique_ptr<SchemaChange> get_sc_procedure(const RowBlockChanger& rb_changer, bool sc_sorting) {
    if (sc_sorting) {
        return std::make_unique<VSchemaChangeWithSorting>(
                rb_changer, config::memory_limitation_per_thread_for_schema_change_bytes);
    }
    // else sc_directly
    return std::make_unique<VSchemaChangeDirectly>(rb_changer);
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
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    // MUST sync rowsets before capturing rowset readers and building DeleteHandler
    RETURN_IF_ERROR(base_tablet->cloud_sync_rowsets(request.alter_version));
    _output_cumulative_point = base_tablet->cumulative_layer_point();

    std::vector<RowsetReaderSharedPtr> rs_readers;
    int64_t base_max_version = base_tablet->local_max_version();
    if (request.alter_version > 1) {
        // [0-1] is a placeholder rowset, no need to convert
        RETURN_IF_ERROR(base_tablet->cloud_capture_rs_readers({2, base_max_version},
                                                              &rs_readers));
    }
    // FIXME(cyx): Should trigger compaction on base_tablet if there are too many rowsets to convert.

    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->update_tablet_columns(*base_tablet->tablet_schema(), request.columns);

    // delete handlers to filter out deleted rows
    DeleteHandler delete_handler;
    std::vector<RowsetMetaSharedPtr> delete_predicates;
    for (auto& rs_reader : rs_readers) {
        auto& rs_meta = rs_reader->rowset()->rowset_meta();
        if (rs_meta->has_delete_predicate()) {
            base_tablet_schema->merge_dropped_columns(
                    base_tablet->tablet_schema(rs_meta->version()));
            delete_predicates.push_back(rs_meta);
        }
    }
    RETURN_IF_ERROR(delete_handler.init(base_tablet_schema, delete_predicates, base_max_version));

    std::vector<ColumnId> return_columns;
    return_columns.resize(base_tablet_schema->num_columns());
    std::iota(return_columns.begin(), return_columns.end(), 0);

    // reader_context is stack variables, it's lifetime MUST keep the same with rs_readers
    RowsetReaderContext reader_context;
    reader_context.reader_type = READER_ALTER_TABLE;
    reader_context.tablet_schema = base_tablet_schema;
    reader_context.need_ordered_result = true;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
    reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
    reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
    reader_context.is_vec = config::enable_vectorized_alter_table;

    for (auto& rs_reader : rs_readers) {
        RETURN_IF_ERROR(rs_reader->init(&reader_context));
    }

    SchemaChangeParams sc_params;

    DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl);
    sc_params.base_tablet = base_tablet;
    sc_params.new_tablet = new_tablet;
    sc_params.ref_rowset_readers = rs_readers;
    sc_params.delete_handler = &delete_handler;
    sc_params.base_tablet_schema = base_tablet_schema;
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
    for (auto& item : request.materialized_view_params) {
        AlterMaterializedViewParam mv_param;
        mv_param.column_name = item.column_name;

        if (item.__isset.origin_column_name) {
            mv_param.origin_column_name = item.origin_column_name;
        }

        if (item.__isset.mv_expr) {
            if (item.mv_expr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL) {
                mv_param.mv_expr = item.mv_expr.nodes[0].fn.name.function_name;
            } else if (item.mv_expr.nodes[0].node_type == TExprNodeType::CASE_EXPR) {
                mv_param.mv_expr = "count_field";
            }
            mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
        }
        sc_params.materialized_params_map.emplace(item.column_name, std::move(mv_param));
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
    RowBlockChanger rb_changer(new_tablet->tablet_schema(), sc_params.delete_handler,
                               *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // 1. Parse the Alter request and convert it into an internal representation
    RETURN_IF_ERROR(
            SchemaChangeHandler::_parse_request(sc_params, &rb_changer, &sc_sorting, &sc_directly));
    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        LOG(INFO) << "Don't support to add materialized view by linked schema change";
        return Status::InternalError(
                "Don't support to add materialized view by linked schema change");
    }

    // 2. Generate historical data converter
    auto sc_procedure = get_sc_procedure(rb_changer, sc_sorting);

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
    auto st = cloud::meta_mgr()->prepare_tablet_job(job);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
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
        context.oldest_write_timestamp = rs_reader->oldest_write_timestamp();
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();
        context.fs = cloud::latest_fs();
        RETURN_IF_ERROR(new_tablet->create_rowset_writer(context, &rowset_writer));

        RowsetMetaSharedPtr existed_rs_meta;
        auto st = meta_mgr()->prepare_rowset(rowset_writer->rowset_meta(), true, &existed_rs_meta);
        if (!st.ok()) {
            if (st.is_already_exist()) {
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

        RETURN_IF_ERROR(sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                              sc_params.base_tablet, sc_params.base_tablet_schema));

        auto new_rowset = rowset_writer->build();
        if (!new_rowset) {
            return Status::InternalError("failed to build rowset, version=[{}-{}]",
                                         rs_reader->version().first, rs_reader->version().second);
        }

        st = meta_mgr()->commit_rowset(rowset_writer->rowset_meta(), true, &existed_rs_meta);
        if (!st.ok()) {
            if (st.is_already_exist()) {
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

    selectdb::TabletStatsPB stats;
    st = cloud::meta_mgr()->commit_tablet_job(job, &stats);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
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

Status CloudSchemaChange::process_alter_inverted_index(const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to alter inverted index: tablet_id=" << request.tablet_id
              << ", schema_hash=" << request.schema_hash
              << ", alter_version=" << request.alter_version
              << ", job_id=" << _job_id;
    TabletSharedPtr tablet;
    RETURN_IF_ERROR(cloud::tablet_mgr()->get_tablet(request.tablet_id, &tablet));
    if (tablet == nullptr) {
        LOG(WARNING) << "fail to find tablet. tablet=" << request.tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    if (tablet->tablet_state() == TABLET_TOMBSTONED || 
            tablet->tablet_state() == TABLET_STOPPED ||
            tablet->tablet_state() == TABLET_SHUTDOWN) {
        LOG(INFO) << "tablet's state=" << tablet->tablet_state()
                  << " cannot alter inverted index";
        return Status::OLAPInternalError(OLAP_ERR_OTHER_ERROR);
    }

    std::unique_lock<std::mutex> schema_change_lock(tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. tablet="
                     << request.tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    Status res = _do_process_alter_inverted_index(tablet, request);
    LOG(INFO) << "finished alter inverted index process, res=" << res;
    return res;
}

Status CloudSchemaChange::_do_process_alter_inverted_index(TabletSharedPtr tablet, const TAlterInvertedIndexReq& request) {
    Status res = Status::OK();
    // MUST sync rowsets before capturing rowset readers and building DeleteHandler
    RETURN_IF_ERROR(tablet->cloud_sync_rowsets(request.alter_version));
    _output_cumulative_point = tablet->cumulative_layer_point();

    std::vector<RowsetReaderSharedPtr> rs_readers;
    if (request.alter_version > 1) {
        // [0-1] is a placeholder rowset, no need to convert
        RETURN_IF_ERROR(tablet->cloud_capture_rs_readers({2, tablet->local_max_version()},
                                                              &rs_readers));
    }

    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns);

    // delete handlers for base tablet
    DeleteHandler delete_handler;
    {
        std::shared_lock tablet_lock(tablet->get_header_lock());
        auto& all_del_preds = tablet->delete_predicates();
        for (auto& delete_pred : all_del_preds) {
            if (delete_pred->version().first > request.alter_version) {
                continue;
            }
            tablet_schema->merge_dropped_columns(
                    tablet->tablet_schema(delete_pred->version()));
        }
        RETURN_IF_ERROR(
                delete_handler.init(tablet_schema, all_del_preds, request.alter_version));
    }

    std::vector<ColumnId> return_columns;
    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }
    for (auto& inverted_index : alter_inverted_indexs) {
        DCHECK_EQ(inverted_index.columns.size(), 1);
        auto column_name = inverted_index.columns[0];
        auto idx = tablet_schema->field_index(column_name);
        return_columns.emplace_back(idx);
    }

    // reader_context is stack variables, it's lifetime MUST keep the same with rs_readers
    RowsetReaderContext reader_context;
    reader_context.reader_type = READER_ALTER_TABLE;
    reader_context.tablet_schema = tablet_schema;
    reader_context.need_ordered_result = false;
    reader_context.delete_handler = &delete_handler;
    reader_context.return_columns = &return_columns;
    reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
    reader_context.is_unique = tablet->keys_type() == UNIQUE_KEYS;
    reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
    reader_context.is_vec = config::enable_vectorized_alter_table;

    for (auto& rs_reader : rs_readers) {
        RETURN_IF_ERROR(rs_reader->init(&reader_context));
    }

    selectdb::TabletJobInfoPB job;
    auto idx = job.mutable_idx();
    idx->set_tablet_id(tablet->tablet_id());
    idx->set_table_id(tablet->table_id());
    idx->set_index_id(tablet->index_id());
    idx->set_partition_id(tablet->partition_id());
    auto sc_job = job.mutable_schema_change();
    sc_job->set_is_inverted_index_change(true);
    sc_job->set_id(_job_id);
    sc_job->set_initiator(BackendOptions::get_localhost() + ':' +
                          std::to_string(config::heartbeat_service_port));
    auto new_tablet_idx = sc_job->mutable_new_tablet_idx();
    new_tablet_idx->set_tablet_id(tablet->tablet_id());
    new_tablet_idx->set_table_id(tablet->table_id());
    new_tablet_idx->set_index_id(tablet->index_id());
    new_tablet_idx->set_partition_id(tablet->partition_id());
    auto st = cloud::meta_mgr()->prepare_tablet_job(job);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
            st = tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync new tablet")
                        .tag("tablet_id", tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }

    if (request.__isset.is_drop_op && request.is_drop_op) {
        // drop index
        res = _drop_inverted_index(rs_readers, tablet_schema, tablet, request);
    } else {
        // add index
        res = _add_inverted_index(rs_readers, &delete_handler, tablet_schema, tablet, request);
    }

    if (!res.ok()) {
        LOG(WARNING) << "failed to alter tablet. tablet=" << tablet->full_name();
        return res;
    }

    if (rs_readers.empty() || _output_rowsets.empty()) {
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

    selectdb::TabletStatsPB stats;
    st = cloud::meta_mgr()->commit_tablet_job(job, &stats);
    if (!st.ok()) {
        if (st.precise_code() == JOB_ALREADY_SUCCESS) {
            st = tablet->cloud_sync_rowsets();
            if (!st.ok()) {
                LOG_WARNING("failed to sync tablet")
                        .tag("tablet_id", tablet->tablet_id())
                        .error(st);
            }
            return Status::OK();
        }
        return st;
    }

    {
        std::lock_guard wlock(tablet->get_header_lock());
        tablet->set_cumulative_layer_point(_output_cumulative_point);
        tablet->reset_approximate_stats(stats.num_rowsets(), stats.num_segments(),
                                            stats.num_rows(), stats.data_size());
    }

    return Status::OK();
}

Status CloudSchemaChange::_add_inverted_index(
            std::vector<RowsetReaderSharedPtr> rs_readers, 
            DeleteHandler* delete_handler,
            const TabletSchemaSPtr& tablet_schema,
            TabletSharedPtr tablet, 
            const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to add inverted index, tablet=" << tablet->full_name();
    Status res = Status::OK();
    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }
    auto sc_procedure = std::make_unique<SchemaChangeForInvertedIndex>(alter_inverted_indexs, tablet_schema);
    // read tablet data and write inverted index
    for (auto& rs_reader : rs_readers) {
        VLOG_TRACE << "begin to read a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;
        res = sc_procedure->process(rs_reader, nullptr, nullptr, tablet, nullptr);
        if (!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            return res;
        }

        auto rowset = rs_reader->rowset();
        auto rowset_meta = rowset->rowset_meta();
        // update indexes in tablet_schema
        {
            // update tablet level schema
            auto new_tablet_schema = std::make_shared<TabletSchema>();
            new_tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns);
            new_tablet_schema->update_indexes_from_thrift(request.indexes);
            if ((res = meta_mgr()->update_tablet_schema(tablet->tablet_id(), new_tablet_schema)) != Status::OK()) {
                LOG(WARNING) << "failed to update tablet schema, tablet id: " << tablet->tablet_id();
                return res;
            }
            tablet->update_max_version_schema(new_tablet_schema, true);
            
            // update rowset meta level schema
            std::lock_guard<std::mutex> rwlock(tablet->get_rowset_update_lock());
            std::shared_lock<std::shared_mutex> wlock(tablet->get_header_lock());
            auto new_rs_tablet_schema = std::make_shared<TabletSchema>();
            new_rs_tablet_schema->update_tablet_columns(*rowset_meta->tablet_schema(), request.columns);
            new_rs_tablet_schema->update_indexes_from_thrift(request.indexes);
            rowset_meta->set_tablet_schema(new_rs_tablet_schema);
        }

        auto st = meta_mgr()->commit_rowset(rowset_meta, true, nullptr);
        if (!st.ok()) {
            if (st.is_already_exist()) {
                LOG(INFO) << "rowset " << rs_reader->version() << " has already existed in tablet "
                          << tablet->tablet_id();
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }
        _output_rowsets.push_back(std::move(rowset));

        VLOG_TRACE << "succeed to write inverted index."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }

    LOG(INFO) << "finish to write inverted index to tablet: " << tablet->full_name();
    return res;
}

Status CloudSchemaChange::_drop_inverted_index(
        std::vector<RowsetReaderSharedPtr> rs_readers, 
        const TabletSchemaSPtr& tablet_schema,
        TabletSharedPtr tablet, 
        const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to drop inverted index";
    Status res = Status::OK();

    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }

    for (auto& rs_reader : rs_readers) {
        auto rowset = rs_reader->rowset();
        auto rowset_meta = rowset->rowset_meta();
        auto fs = rowset_meta->fs();
        for (auto i = 0; i < rowset_meta->num_segments(); ++i) {
            std::string segment_path = rowset_meta->is_local()
                    ? BetaRowset::segment_file_path(tablet->tablet_path(), rowset_meta->rowset_id(), i)
                    : BetaRowset::remote_segment_path(tablet->tablet_id(), rowset_meta->rowset_id(), i);
            for (auto& inverted_index: alter_inverted_indexs) {
                auto column_name = inverted_index.columns[0];
                auto column = tablet_schema->column(column_name);
                auto index_id = inverted_index.index_id;

                std::string inverted_index_file = InvertedIndexDescriptor::get_index_file_name(segment_path, index_id);
                bool file_exist = false;
                fs->exists(inverted_index_file, &file_exist);
                if (!file_exist) {
                    return Status::OK();
                }
                LOG(INFO) << "will drop inverted index, index id: " << index_id
                        << ", cid: " << column.unique_id()
                        << ", column_name: " << column_name
                        << ", inverted_index_file: " << inverted_index_file;
                res = fs->delete_file(inverted_index_file);
                if (!res.ok()) {
                    LOG(WARNING) << "failed to delete file: " << inverted_index_file 
                            << ", res: " << res.to_string();
                    return res;
                }
            }
        }

        // update indexes in tablet_schema
        {
            // update tablet level schema
            auto new_tablet_schema = std::make_shared<TabletSchema>();
            new_tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns);
            new_tablet_schema->update_indexes_from_thrift(request.indexes);
            if ((res = meta_mgr()->update_tablet_schema(tablet->tablet_id(), new_tablet_schema)) != Status::OK()) {
                LOG(WARNING) << "failed to update tablet schema, tablet id: " << tablet->tablet_id();
                return res;
            }
            tablet->update_max_version_schema(new_tablet_schema, true);
            
            // update rowset meta level schema
            std::lock_guard<std::mutex> rwlock(tablet->get_rowset_update_lock());
            std::shared_lock<std::shared_mutex> wlock(tablet->get_header_lock());
            auto new_rs_tablet_schema = std::make_shared<TabletSchema>();
            new_rs_tablet_schema->update_tablet_columns(*rowset_meta->tablet_schema(), request.columns);
            new_rs_tablet_schema->update_indexes_from_thrift(request.indexes);
            rowset_meta->set_tablet_schema(new_rs_tablet_schema);
        }

        auto st = meta_mgr()->commit_rowset(rowset_meta, true, nullptr);
        if (!st.ok()) {
            if (st.is_already_exist()) {
                LOG(INFO) << "rowset " << rs_reader->version() << " has already existed in tablet "
                          << tablet->tablet_id();
                _output_rowsets.push_back(std::move(rowset));
                continue;
            } else {
                return st;
            }
        }
        _output_rowsets.push_back(std::move(rowset));
    }

    return Status::OK();
}

} // namespace doris::cloud
