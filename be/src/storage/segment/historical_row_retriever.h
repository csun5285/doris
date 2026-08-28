// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <span>

#include "common/status.h"
#include "core/block/block.h"
#include "core/block/column_with_type_and_name.h"
#include "storage/olap_define.h"
#include "storage/olap_utils.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
struct RowsetWriterContext;
class HistoricalRowFetcher;
class RowKeyEncoder;
struct MowContext;

namespace segment_v2 {

struct HistoricalRowRetrieverContext {
    BaseTabletSPtr tablet;
    TabletSchemaSPtr tablet_schema;
    RowsetWriterContext* rowset_writer_ctx = nullptr;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    bool is_transient_rowset_writer = false;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
};

class HistoricalRowRetriever {
public:
    HistoricalRowRetriever() = default;
    virtual ~HistoricalRowRetriever() = default;

    virtual Status init(const HistoricalRowRetrieverContext& rowset_writer_context) = 0;

    virtual Status retrieve_historical_row(const Int8* delete_sign_column_data, size_t row_pos,
                                           size_t num_rows) = 0;

    virtual Status build_after_block(Block* block, size_t row_pos, size_t num_rows) = 0;
    virtual Status build_before_block(Block* before_block, const std::vector<uint32_t>& value_cids,
                                      size_t row_pos, size_t num_rows) = 0;

    virtual std::vector<int64_t>& get_operators() = 0;

protected:
    HistoricalRowRetrieverContext _context;
};

class PrimaryKeyModelRowRetriever : public HistoricalRowRetriever {
public:
    // Out of line: _row_fetcher/_key_encoder are held by unique_ptr to types
    // this header only forward-declares.
    PrimaryKeyModelRowRetriever();
    ~PrimaryKeyModelRowRetriever() override;
    Status init(const HistoricalRowRetrieverContext& context) override;

    // The source block the lookup encodes its keys out of, held so it outlives
    // the lookup. `block_cids[position]` is the schema column id at that block
    // position; an empty layout is a block in source schema order, which is
    // everything but a fixed partial update's narrow block.
    Status prepare_lookup_plan(Block block, std::span<const uint32_t> block_cids,
                               std::shared_ptr<MowContext> mow_context);

    Status retrieve_historical_row(const Int8* delete_sign_column_data, size_t row_pos,
                                   size_t num_rows) override;

    // Row Binlog receives the original flexible-partial-update block, while the base writer fills
    // its own copy. Reuse the common flexible aggregator/read plans with a read-only MOW probe to
    // build the same full AFTER rows without changing the base tablet's delete bitmap. The optional
    // LSN sidecar is merged and filtered together with the rows.
    Status materialize_flexible_partial_update(Block* block,
                                               std::shared_ptr<MowContext> mow_context,
                                               std::vector<int64_t>* row_lsns);

    Status build_after_block(Block* block, size_t row_pos, size_t num_rows) override;

    Status build_before_block(Block* before_block, const std::vector<uint32_t>& value_cids,
                              size_t /*row_pos*/, size_t num_rows) override;

    Status revise_operators_by_old_delete_sign(size_t num_rows);

    std::vector<int64_t>& get_operators() override { return _operators; };

private:
    Status _fill_old_delete_signs(const Block& old_value_block,
                                  const std::map<uint32_t, uint32_t>& read_index, size_t num_rows);

    // The block the lookup encodes its keys out of, held by value so it stays
    // alive for as long as the lookup needs it.
    Block _lookup_block;
    std::unique_ptr<RowKeyEncoder> _key_encoder;
    std::shared_ptr<MowContext> _mow_context;

    // owns the rowset pins and the read plan fed by the probe outcomes
    std::unique_ptr<HistoricalRowFetcher> _row_fetcher;

    // cache flags for filling missing columns
    std::vector<bool> _use_default_or_null_flag;
    bool _has_default_or_nullable = false;

    // cache operator for fill_binlog_columns
    std::vector<int64_t> _operators;
    // Aligned by row position in the current append batch.
    std::vector<signed char> _old_delete_signs;
};

} // namespace segment_v2
} // namespace doris
