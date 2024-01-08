#pragma once

#include "olap/rowset/rowset.h"
#include "olap/schema_change.h"

namespace doris::cloud {

class CloudSchemaChange {
public:
    CloudSchemaChange(std::string job_id, int64_t expiration);
    ~CloudSchemaChange();

    // This method is idempotent for a same request.
    Status process_alter_tablet(const TAlterTabletReqV2& request);

    Status process_alter_inverted_index(const TAlterInvertedIndexReq& request);

private:
    Status _convert_historical_rowsets(const SchemaChangeParams& sc_params);
    Status _process_delete_bitmap(TabletSharedPtr new_tablet, int64_t alter_version,
                                  int64_t start_calc_delete_bitmap_version, int64_t initiator);
    Status _do_process_alter_inverted_index(TabletSharedPtr tablet,
                                            const TAlterInvertedIndexReq& request);
    Status _add_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                               DeleteHandler* delete_handler, const TabletSchemaSPtr& tablet_schema,
                               TabletSharedPtr tablet, const TAlterInvertedIndexReq& request);
    Status _drop_inverted_index(std::vector<RowsetReaderSharedPtr> rs_readers,
                                const TabletSchemaSPtr& tablet_schema, TabletSharedPtr tablet,
                                const TAlterInvertedIndexReq& request);

private:
    std::string _job_id;
    std::vector<RowsetSharedPtr> _output_rowsets;
    int64_t _output_cumulative_point;
    int64_t _expiration;
};

} // namespace doris::cloud
