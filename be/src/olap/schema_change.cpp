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

#include "olap/schema_change.h"

#include "common/status.h"
#include "gutil/integral_types.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/row.h"
#include "olap/row_block.h"
#include "olap/row_cursor.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/rowset_id_generator.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/storage_engine.h"
#include "olap/segment_loader.h"
#include "olap/tablet.h"
#include "olap/tablet_schema.h"
#include "olap/tablet_meta_manager.h"
#include "olap/types.h"
#include "olap/wrapper_field.h"
#include "runtime/memory/mem_tracker.h"
#include "util/defer_op.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/columns/column.h"
#include "vec/core/block.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/olap_data_convertor.h"

using std::nothrow;

namespace doris {

constexpr int ALTER_TABLE_BATCH_SIZE = 4096;

class RowBlockSorter {
public:
    explicit RowBlockSorter(RowBlockAllocator* allocator);
    virtual ~RowBlockSorter();
    size_t num_rows() { return _swap_row_block != nullptr ? _swap_row_block->capacity() : 0; }

    bool sort(RowBlock** row_block);

private:
    static bool _row_cursor_comparator(const std::unique_ptr<RowCursor>& a,
                                       const std::unique_ptr<RowCursor>& b) {
        return compare_row(*a, *b) < 0;
    }

    RowBlockAllocator* _row_block_allocator;
    RowBlock* _swap_row_block;
};

class RowBlockMerger {
public:
    explicit RowBlockMerger(TabletSharedPtr tablet);
    virtual ~RowBlockMerger();

    bool merge(const std::vector<RowBlock*>& row_block_arr, RowsetWriter* rowset_writer,
               uint64_t* merged_rows);

private:
    struct MergeElement {
        bool operator<(const MergeElement& other) const {
            return compare_row(*row_cursor, *other.row_cursor) > 0;
        }

        const RowBlock* row_block;
        RowCursor* row_cursor;
        uint32_t row_block_index;
    };

    bool _make_heap(const std::vector<RowBlock*>& row_block_arr);
    void _pop_heap();

    TabletSharedPtr _tablet;
    std::priority_queue<MergeElement> _heap;
};

class MultiBlockMerger {
public:
    MultiBlockMerger(const TabletSharedPtr& tablet) : _tablet(tablet), _cmp(tablet.get()) {}

    Status merge(const std::vector<std::unique_ptr<vectorized::Block>>& blocks,
                 RowsetWriter* rowset_writer, uint64_t* merged_rows) {
        int rows = 0;
        for (auto& block : blocks) {
            rows += block->rows();
        }
        if (!rows) {
            return Status::OK();
        }

        std::vector<RowRef> row_refs;
        row_refs.reserve(rows);
        for (auto& block : blocks) {
            for (uint16_t i = 0; i < block->rows(); i++) {
                row_refs.emplace_back(block.get(), i);
            }
        }
        // TODO: try to use pdqsort to replace std::sort
        // The block version is incremental.
        std::stable_sort(row_refs.begin(), row_refs.end(), _cmp);

        auto finalized_block = _tablet->tablet_schema()->create_block();
        int columns = finalized_block.columns();
        *merged_rows += rows;

        if (_tablet->keys_type() == KeysType::AGG_KEYS) {
            auto tablet_schema = _tablet->tablet_schema();
            int key_number = _tablet->num_key_columns();

            std::vector<vectorized::AggregateFunctionPtr> agg_functions;
            std::vector<vectorized::AggregateDataPtr> agg_places;

            for (int i = key_number; i < columns; i++) {
                vectorized::AggregateFunctionPtr function =
                        tablet_schema->column(i).get_aggregate_function(
                                {finalized_block.get_data_type(i)}, vectorized::AGG_LOAD_SUFFIX);
                agg_functions.push_back(function);
                // create aggregate data
                vectorized::AggregateDataPtr place = new char[function->size_of_data()];
                function->create(place);
                agg_places.push_back(place);
            }

            for (int i = 0; i < rows; i++) {
                auto row_ref = row_refs[i];

                for (int j = key_number; j < columns; j++) {
                    auto column_ptr = row_ref.get_column(j).get();
                    agg_functions[j - key_number]->add(
                            agg_places[j - key_number],
                            const_cast<const vectorized::IColumn**>(&column_ptr), row_ref.position,
                            nullptr);
                }

                if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                    for (int j = 0; j < key_number; j++) {
                        finalized_block.get_by_position(j).column->assume_mutable()->insert_from(
                                *row_ref.get_column(j), row_ref.position);
                    }

                    for (int j = key_number; j < columns; j++) {
                        agg_functions[j - key_number]->insert_result_into(
                                agg_places[j - key_number],
                                finalized_block.get_by_position(j).column->assume_mutable_ref());
                        agg_functions[j - key_number]->create(agg_places[j - key_number]);
                    }

                    if (i == rows - 1 || finalized_block.rows() == ALTER_TABLE_BATCH_SIZE) {
                        *merged_rows -= finalized_block.rows();
                        rowset_writer->add_block(&finalized_block);
                        finalized_block.clear_column_data();
                    }
                }
            }

            for (int i = 0; i < columns - key_number; i++) {
                agg_functions[i]->destroy(agg_places[i]);
                delete[] agg_places[i];
            }
        } else {
            std::vector<RowRef> pushed_row_refs;
            if (_tablet->keys_type() == KeysType::DUP_KEYS) {
                std::swap(pushed_row_refs, row_refs);
            } else if (_tablet->keys_type() == KeysType::UNIQUE_KEYS) {
                for (int i = 0; i < rows; i++) {
                    if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                        pushed_row_refs.push_back(row_refs[i]);
                    }
                }
            }

            // update real inserted row number
            rows = pushed_row_refs.size();
            *merged_rows -= rows;

            for (int i = 0; i < rows; i += ALTER_TABLE_BATCH_SIZE) {
                int limit = std::min(ALTER_TABLE_BATCH_SIZE, rows - i);

                for (int idx = 0; idx < columns; idx++) {
                    auto column = finalized_block.get_by_position(idx).column->assume_mutable();

                    for (int j = 0; j < limit; j++) {
                        auto row_ref = pushed_row_refs[i + j];
                        column->insert_from(*row_ref.get_column(idx), row_ref.position);
                    }
                }
                rowset_writer->add_block(&finalized_block);
                finalized_block.clear_column_data();
            }
        }

        RETURN_IF_ERROR(rowset_writer->flush());
        return Status::OK();
    }

private:
    struct RowRef {
        RowRef(vectorized::Block* block_, uint16_t position_)
                : block(block_), position(position_) {}
        vectorized::ColumnPtr get_column(int index) const {
            return block->get_by_position(index).column;
        }
        const vectorized::Block* block;
        uint16_t position;
    };

    struct RowRefComparator {
        RowRefComparator(Tablet* tablet) : _num_columns(tablet->num_key_columns()) {}

        int compare(const RowRef& lhs, const RowRef& rhs) const {
            return lhs.block->compare_at(lhs.position, rhs.position, _num_columns, *rhs.block, -1);
        }

        bool operator()(const RowRef& lhs, const RowRef& rhs) const {
            return compare(lhs, rhs) < 0;
        }

        const size_t _num_columns;
    };

    TabletSharedPtr _tablet;
    RowRefComparator _cmp;
};

RowBlockChanger::RowBlockChanger(TabletSchemaSPtr tablet_schema,
                                 const DeleteHandler* delete_handler, DescriptorTbl desc_tbl)
        : _desc_tbl(desc_tbl) {
    _schema_mapping.resize(tablet_schema->num_columns());
    _delete_handler = delete_handler;
}

RowBlockChanger::~RowBlockChanger() {
    for (auto it = _schema_mapping.begin(); it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* RowBlockChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }

    return &(_schema_mapping[column_index]);
}

#define TYPE_REINTERPRET_CAST(FromType, ToType)                         \
    {                                                                   \
        size_t row_num = ref_block->row_block_info().row_num;           \
        for (size_t row = 0, mutable_row = 0; row < row_num; ++row) {   \
            char* ref_ptr = ref_block->field_ptr(row, ref_column);      \
            char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
            *new_ptr = *ref_ptr;                                        \
            *(ToType*)(new_ptr + 1) = *(FromType*)(ref_ptr + 1);        \
        }                                                               \
        break;                                                          \
    }

#define LARGEINT_REINTERPRET_CAST(FromType, ToType)                     \
    {                                                                   \
        size_t row_num = ref_block->row_block_info().row_num;           \
        for (size_t row = 0, mutable_row = 0; row < row_num; ++row) {   \
            char* ref_ptr = ref_block->field_ptr(row, ref_column);      \
            char* new_ptr = mutable_block->field_ptr(mutable_row++, i); \
            *new_ptr = *ref_ptr;                                        \
            ToType new_value = *(FromType*)(ref_ptr + 1);               \
            memcpy(new_ptr + 1, &new_value, sizeof(ToType));            \
        }                                                               \
        break;                                                          \
    }

#define CONVERT_FROM_TYPE(from_type)                                                             \
    {                                                                                            \
        switch (newtype) {                                                                       \
        case OLAP_FIELD_TYPE_TINYINT:                                                            \
            TYPE_REINTERPRET_CAST(from_type, int8_t);                                            \
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:                                                   \
            TYPE_REINTERPRET_CAST(from_type, uint8_t);                                           \
        case OLAP_FIELD_TYPE_SMALLINT:                                                           \
            TYPE_REINTERPRET_CAST(from_type, int16_t);                                           \
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:                                                  \
            TYPE_REINTERPRET_CAST(from_type, uint16_t);                                          \
        case OLAP_FIELD_TYPE_INT:                                                                \
            TYPE_REINTERPRET_CAST(from_type, int32_t);                                           \
        case OLAP_FIELD_TYPE_UNSIGNED_INT:                                                       \
            TYPE_REINTERPRET_CAST(from_type, uint32_t);                                          \
        case OLAP_FIELD_TYPE_BIGINT:                                                             \
            TYPE_REINTERPRET_CAST(from_type, int64_t);                                           \
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:                                                    \
            TYPE_REINTERPRET_CAST(from_type, uint64_t);                                          \
        case OLAP_FIELD_TYPE_LARGEINT:                                                           \
            LARGEINT_REINTERPRET_CAST(from_type, int128_t);                                      \
        case OLAP_FIELD_TYPE_FLOAT:                                                              \
            TYPE_REINTERPRET_CAST(from_type, float);                                             \
        case OLAP_FIELD_TYPE_DOUBLE:                                                             \
            TYPE_REINTERPRET_CAST(from_type, double);                                            \
        default:                                                                                 \
            LOG(WARNING) << "the column type which was altered to was unsupported."              \
                         << " origin_type="                                                      \
                         << ref_block->tablet_schema()->column(ref_column).type()                \
                         << ", alter_type=" << mutable_block->tablet_schema()->column(i).type(); \
            return Status::OLAPInternalError(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID);               \
        }                                                                                        \
        break;                                                                                   \
    }

#define ASSIGN_DEFAULT_VALUE(length)                                            \
    case length: {                                                              \
        for (size_t row = 0; row < ref_block.row_block_info().row_num; ++row) { \
            memcpy(buf, _schema_mapping[i].default_value->ptr(), length);       \
            buf += length;                                                      \
        }                                                                       \
        break;                                                                  \
    }

struct ConvertTypeMapHash {
    size_t operator()(const std::pair<FieldType, FieldType>& pair) const {
        return (pair.first + 31) ^ pair.second;
    }
};

class ConvertTypeResolver {
    DECLARE_SINGLETON(ConvertTypeResolver);

public:
    bool get_convert_type_info(const FieldType from_type, const FieldType to_type) const {
        return _convert_type_set.find(std::make_pair(from_type, to_type)) !=
               _convert_type_set.end();
    }

    template <FieldType from_type, FieldType to_type>
    void add_convert_type_mapping() {
        _convert_type_set.emplace(std::make_pair(from_type, to_type));
    }

private:
    using convert_type_pair = std::pair<FieldType, FieldType>;
    std::unordered_set<convert_type_pair, ConvertTypeMapHash> _convert_type_set;

    DISALLOW_COPY_AND_ASSIGN(ConvertTypeResolver);
};

ConvertTypeResolver::ConvertTypeResolver() {
    // from char type
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_DATE>();

    // supported type convert should annotate in doc:
    // http://doris.apache.org/master/zh-CN/sql-reference/sql-statements/Data%20Definition/ALTER%20TABLE.html#description
    // If type convert is supported here, you should check fe/src/main/java/org/apache/doris/catalog/ColumnType.java to supported it either
    // from varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_VARCHAR, OLAP_FIELD_TYPE_STRING>();

    // to varchar type
    add_convert_type_mapping<OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DOUBLE, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_VARCHAR>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_VARCHAR>();

    // from string
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_TINYINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_SMALLINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_INT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_BIGINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_LARGEINT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_FLOAT>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_DOUBLE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_DATE>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_VARCHAR>();

    // to string
    add_convert_type_mapping<OLAP_FIELD_TYPE_TINYINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_SMALLINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_BIGINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_LARGEINT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DOUBLE, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_DECIMAL, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_CHAR, OLAP_FIELD_TYPE_STRING>();
    add_convert_type_mapping<OLAP_FIELD_TYPE_STRING, OLAP_FIELD_TYPE_STRING>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATE, OLAP_FIELD_TYPE_DATETIME>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_DATETIME, OLAP_FIELD_TYPE_DATE>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_FLOAT, OLAP_FIELD_TYPE_DOUBLE>();

    add_convert_type_mapping<OLAP_FIELD_TYPE_INT, OLAP_FIELD_TYPE_DATE>();
}

ConvertTypeResolver::~ConvertTypeResolver() = default;

bool to_bitmap(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
               int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    BitmapValue bitmap;
    if (!read_helper->is_null(ref_field_idx)) {
        uint64_t origin_value;
        char* src = read_helper->cell_ptr(ref_field_idx);
        switch (ref_column.type()) {
        case OLAP_FIELD_TYPE_TINYINT:
            if (*(int8_t*)src < 0) {
                LOG(WARNING)
                        << "The input: " << *(int8_t*)src
                        << " is not valid, to_bitmap only support bigint value from 0 to "
                           "18446744073709551615 currently, cannot create MV with to_bitmap on "
                           "column with negative values.";
                return false;
            }
            origin_value = *(int8_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
            origin_value = *(uint8_t*)src;
            break;
        case OLAP_FIELD_TYPE_SMALLINT:
            if (*(int16_t*)src < 0) {
                LOG(WARNING)
                        << "The input: " << *(int16_t*)src
                        << " is not valid, to_bitmap only support bigint value from 0 to "
                           "18446744073709551615 currently, cannot create MV with to_bitmap on "
                           "column with negative values.";
                return false;
            }
            origin_value = *(int16_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
            origin_value = *(uint16_t*)src;
            break;
        case OLAP_FIELD_TYPE_INT:
            if (*(int32_t*)src < 0) {
                LOG(WARNING)
                        << "The input: " << *(int32_t*)src
                        << " is not valid, to_bitmap only support bigint value from 0 to "
                           "18446744073709551615 currently, cannot create MV with to_bitmap on "
                           "column with negative values.";
                return false;
            }
            origin_value = *(int32_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
            origin_value = *(uint32_t*)src;
            break;
        case OLAP_FIELD_TYPE_BIGINT:
            if (*(int64_t*)src < 0) {
                LOG(WARNING)
                        << "The input: " << *(int64_t*)src
                        << " is not valid, to_bitmap only support bigint value from 0 to "
                           "18446744073709551615 currently, cannot create MV with to_bitmap on "
                           "column with negative values.";
                return false;
            }
            origin_value = *(int64_t*)src;
            break;
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
            origin_value = *(uint64_t*)src;
            break;
        default:
            LOG(WARNING) << "the column type which was altered from was unsupported."
                         << " from_type=" << ref_column.type();
            return false;
        }
        bitmap.add(origin_value);
    }
    char* buf = reinterpret_cast<char*>(mem_pool->allocate(bitmap.getSizeInBytes()));
    Slice dst(buf, bitmap.getSizeInBytes());
    bitmap.write(dst.data);
    write_helper->set_field_content(field_idx, reinterpret_cast<char*>(&dst), mem_pool);
    return true;
}

bool hll_hash(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
              int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    HyperLogLog hll;
    if (!read_helper->is_null(ref_field_idx)) {
        uint64_t hash_value;

        switch (ref_column.type()) {
        case OLAP_FIELD_TYPE_CHAR: {
            int p = ref_column.length() - 1;
            Slice* slice = reinterpret_cast<Slice*>(read_helper->cell_ptr(ref_field_idx));
            char* buf = slice->data;
            while (p >= 0 && buf[p] == '\0') {
                p--;
            }
            slice->size = p + 1;
        }

        case OLAP_FIELD_TYPE_VARCHAR:
        case OLAP_FIELD_TYPE_STRING: {
            Slice slice = *reinterpret_cast<Slice*>(read_helper->cell_ptr(ref_field_idx));
            hash_value = HashUtil::murmur_hash64A(slice.data, slice.size, HashUtil::MURMUR_SEED);
            break;
        }
        case OLAP_FIELD_TYPE_BOOL:
        case OLAP_FIELD_TYPE_TINYINT:
        case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
        case OLAP_FIELD_TYPE_SMALLINT:
        case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
        case OLAP_FIELD_TYPE_INT:
        case OLAP_FIELD_TYPE_UNSIGNED_INT:
        case OLAP_FIELD_TYPE_BIGINT:
        case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
        case OLAP_FIELD_TYPE_LARGEINT:
        case OLAP_FIELD_TYPE_FLOAT:
        case OLAP_FIELD_TYPE_DOUBLE:
        case OLAP_FIELD_TYPE_DISCRETE_DOUBLE:
        case OLAP_FIELD_TYPE_DATE:
        case OLAP_FIELD_TYPE_DATETIME: {
            std::string ref_column_string =
                    read_helper->column_schema(ref_field_idx)
                            ->type_info()
                            ->to_string(read_helper->cell_ptr(ref_field_idx));
            hash_value = HashUtil::murmur_hash64A(
                    ref_column_string.c_str(), ref_column_string.length(), HashUtil::MURMUR_SEED);
            break;
        }
        default:
            LOG(WARNING) << "fail to hll hash type : " << ref_column.type();
            return false;
        }

        hll.update(hash_value);
    }
    std::string buf;
    buf.resize(hll.max_serialized_size());
    buf.resize(hll.serialize((uint8_t*)buf.c_str()));
    Slice dst(buf);
    write_helper->set_field_content(field_idx, reinterpret_cast<char*>(&dst), mem_pool);
    return true;
}

bool count_field(RowCursor* read_helper, RowCursor* write_helper, const TabletColumn& ref_column,
                 int field_idx, int ref_field_idx, MemPool* mem_pool) {
    write_helper->set_not_null(field_idx);
    int64_t count = read_helper->is_null(ref_field_idx) ? 0 : 1;
    write_helper->set_field_content(field_idx, (char*)&count, mem_pool);
    return true;
}

Status RowBlockChanger::change_row_block(const RowBlock* ref_block, int32_t data_version,
                                         RowBlock* mutable_block,
                                         const uint64_t* filtered_rows) const {
    if (mutable_block == nullptr) {
        LOG(FATAL) << "mutable block is uninitialized.";
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    } else if (mutable_block->tablet_schema()->num_columns() != _schema_mapping.size()) {
        LOG(WARNING) << "mutable block does not match with schema mapping rules. "
                     << "block_schema_size=" << mutable_block->tablet_schema()->num_columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    }

    if (mutable_block->capacity() < ref_block->row_block_info().row_num) {
        LOG(WARNING) << "mutable block is not large enough for storing the changed block. "
                     << "mutable_block_size=" << mutable_block->capacity()
                     << ", ref_block_row_num=" << ref_block->row_block_info().row_num;
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    }

    mutable_block->clear();

    RowCursor write_helper;
    if (write_helper.init(mutable_block->tablet_schema()) != Status::OK()) {
        LOG(WARNING) << "fail to init rowcursor.";
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    }

    RowCursor read_helper;
    if (read_helper.init(ref_block->tablet_schema()) != Status::OK()) {
        LOG(WARNING) << "fail to init rowcursor.";
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    }

    // a.1 First determine whether the data needs to be filtered, and finally only those marked as 1 are left as needed
    // For those without filter, it is equivalent to leave after setting all to 1
    const uint32_t row_num = ref_block->row_block_info().row_num;

    // a.2 Calculate the left row num
    uint32_t new_row_num = row_num - *filtered_rows;

    const bool filter_all = (new_row_num == 0);

    MemPool* mem_pool = mutable_block->mem_pool();
    // b. According to the previous filtering information, only processes that are also marked as 1
    for (size_t i = 0, len = mutable_block->tablet_schema()->num_columns(); !filter_all && i < len;
         ++i) {
        int32_t ref_column = _schema_mapping[i].ref_column;
        if (_schema_mapping[i].ref_column >= 0) {
            if (!_schema_mapping[i].materialized_function.empty()) {
                bool (*_do_materialized_transform)(RowCursor*, RowCursor*, const TabletColumn&, int,
                                                   int, MemPool*) = nullptr;
                if (_schema_mapping[i].materialized_function == "to_bitmap" ||
                    _schema_mapping[i].materialized_function == "to_bitmap_with_check") {
                    _do_materialized_transform = to_bitmap;
                } else if (_schema_mapping[i].materialized_function == "hll_hash") {
                    _do_materialized_transform = hll_hash;
                } else if (_schema_mapping[i].materialized_function == "count_field") {
                    _do_materialized_transform = count_field;
                } else {
                    LOG(WARNING) << "error materialized view function : "
                                 << _schema_mapping[i].materialized_function;
                    return Status::OLAPInternalError(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID);
                }
                VLOG_NOTICE << "_schema_mapping[" << i << "].materialized_function : "
                            << _schema_mapping[i].materialized_function;
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);

                    if (!_do_materialized_transform(&read_helper, &write_helper,
                                                    ref_block->tablet_schema()->column(ref_column),
                                                    i, _schema_mapping[i].ref_column, mem_pool)) {
                        return Status::OLAPInternalError(OLAP_ERR_DATA_QUALITY_ERR);
                    }
                }
                continue;
            }

            // new column will be assigned as referenced column
            // check if the type of new column is equal to the older's.
            FieldType reftype = ref_block->tablet_schema()->column(ref_column).type();
            FieldType newtype = mutable_block->tablet_schema()->column(i).type();
            if (newtype == reftype) {
                // Low efficiency, you can also directly calculate the variable length domain copy, but it will still destroy the package
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    // Specify the new row index to be written (different from the read row_index)
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);

                    if (read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        write_helper.set_not_null(i);
                        if (newtype == OLAP_FIELD_TYPE_CHAR) {
                            // if modify length of CHAR type, the size of slice should be equal
                            // to new length.
                            Slice* src = (Slice*)(read_helper.cell_ptr(ref_column));
                            size_t size = mutable_block->tablet_schema()->column(i).length();
                            char* buf = reinterpret_cast<char*>(mem_pool->allocate(size));
                            memset(buf, 0, size);
                            size_t copy_size = (size < src->size) ? size : src->size;
                            memcpy(buf, src->data, copy_size);
                            Slice dst(buf, size);
                            write_helper.set_field_content(i, reinterpret_cast<char*>(&dst),
                                                           mem_pool);
                        } else {
                            char* src = read_helper.cell_ptr(ref_column);
                            write_helper.set_field_content(i, src, mem_pool);
                        }
                    }
                }
            } else if (ConvertTypeResolver::instance()->get_convert_type_info(reftype, newtype)) {
                for (size_t row_index = 0, new_row_index = 0;
                     row_index < ref_block->row_block_info().row_num; ++row_index) {
                    mutable_block->get_row(new_row_index++, &write_helper);
                    ref_block->get_row(row_index, &read_helper);
                    if (read_helper.is_null(ref_column)) {
                        write_helper.set_null(i);
                    } else {
                        write_helper.set_not_null(i);
                        const Field* ref_field = read_helper.column_schema(ref_column);
                        char* ref_value = read_helper.cell_ptr(ref_column);
                        Status st = write_helper.convert_from(i, ref_value, ref_field->type_info(),
                                                              mem_pool);
                        if (!st) {
                            LOG(WARNING)
                                    << "the column type which was altered from was unsupported."
                                    << "status:" << st.to_string() << ", from_type=" << reftype
                                    << ", to_type=" << newtype;
                            return st;
                        }
                    }
                }
                // Write column i from ref_column.
            } else {
                // copy and alter the field
                // You can stay here for the time being, the new type does not involve type conversion for the time being
                switch (reftype) {
                case OLAP_FIELD_TYPE_TINYINT:
                    CONVERT_FROM_TYPE(int8_t);
                case OLAP_FIELD_TYPE_UNSIGNED_TINYINT:
                    CONVERT_FROM_TYPE(uint8_t);
                case OLAP_FIELD_TYPE_SMALLINT:
                    CONVERT_FROM_TYPE(int16_t);
                case OLAP_FIELD_TYPE_UNSIGNED_SMALLINT:
                    CONVERT_FROM_TYPE(uint16_t);
                case OLAP_FIELD_TYPE_INT:
                    CONVERT_FROM_TYPE(int32_t);
                case OLAP_FIELD_TYPE_UNSIGNED_INT:
                    CONVERT_FROM_TYPE(uint32_t);
                case OLAP_FIELD_TYPE_BIGINT:
                    CONVERT_FROM_TYPE(int64_t);
                case OLAP_FIELD_TYPE_UNSIGNED_BIGINT:
                    CONVERT_FROM_TYPE(uint64_t);
                case OLAP_FIELD_TYPE_LARGEINT:
                    CONVERT_FROM_TYPE(int128_t);
                default:
                    LOG(WARNING) << "the column type which was altered from was unsupported."
                                 << " from_type="
                                 << ref_block->tablet_schema()->column(ref_column).type();
                    return Status::OLAPInternalError(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID);
                }

                if (newtype < reftype) {
                    VLOG_NOTICE << "type degraded while altering column. "
                                << "column=" << mutable_block->tablet_schema()->column(i).name()
                                << ", origin_type="
                                << ref_block->tablet_schema()->column(ref_column).type()
                                << ", alter_type="
                                << mutable_block->tablet_schema()->column(i).type();
                }
            }
        } else {
            // New column, write default value
            for (size_t row_index = 0, new_row_index = 0;
                 row_index < ref_block->row_block_info().row_num; ++row_index) {
                mutable_block->get_row(new_row_index++, &write_helper);

                if (_schema_mapping[i].default_value->is_null()) {
                    write_helper.set_null(i);
                } else {
                    write_helper.set_not_null(i);
                    write_helper.set_field_content(i, _schema_mapping[i].default_value->ptr(),
                                                   mem_pool);
                }
            }
        }
    }

    // NOTE The current row_num of mutable_block is still as much as ref
    // (Actually, you can re-init into less when init, the new_row_num left by the filter)
    // In split_table, there may be no data due to filtering
    mutable_block->finalize(new_row_num);
    return Status::OK();
}

#undef CONVERT_FROM_TYPE
#undef TYPE_REINTERPRET_CAST
#undef ASSIGN_DEFAULT_VALUE

Status RowBlockChanger::change_block(vectorized::Block* ref_block,
                                     vectorized::Block* new_block) const {
    if (new_block->columns() != _schema_mapping.size()) {
        LOG(WARNING) << "block does not match with schema mapping rules. "
                     << "block_schema_size=" << new_block->columns()
                     << ", mapping_schema_size=" << _schema_mapping.size();
        return Status::OLAPInternalError(OLAP_ERR_NOT_INITED);
    }

    ObjectPool pool;
    RuntimeState* state = pool.add(new RuntimeState());
    state->set_desc_tbl(&_desc_tbl);
    RowDescriptor row_desc =
            RowDescriptor(_desc_tbl.get_tuple_descriptor(_desc_tbl.get_row_tuples()[0]), false);

    const int row_size = ref_block->rows();
    const int column_size = new_block->columns();

    // swap ref_block[key] and new_block[value]
    std::map<int, int> swap_idx_map;

    for (int idx = 0; idx < column_size; idx++) {
        int ref_idx = _schema_mapping[idx].ref_column;

        if (ref_idx < 0) {
            // new column, write default value
            auto value = _schema_mapping[idx].default_value;
            auto column = new_block->get_by_position(idx).column->assume_mutable();
            if (value->is_null()) {
                DCHECK(column->is_nullable());
                column->insert_many_defaults(row_size);
            } else {
                auto type_info = get_type_info(_schema_mapping[idx].new_column);
                DefaultValueColumnIterator::insert_default_data(type_info.get(), value->size(),
                                                                value->ptr(), column, row_size);
            }
        } else if (_schema_mapping[idx].expr != nullptr) {
            // calculate special materialized function, to_bitmap/hll_hash/count_field or cast expr
            vectorized::VExprContext* ctx = nullptr;
            RETURN_IF_ERROR(
                    vectorized::VExpr::create_expr_tree(&pool, *_schema_mapping[idx].expr, &ctx));
            Defer defer {[&]() { ctx->close(state); }};
            RETURN_IF_ERROR(ctx->prepare(state, row_desc));
            RETURN_IF_ERROR(ctx->open(state));

            int result_column_id = -1;
            RETURN_IF_ERROR(ctx->execute(ref_block, &result_column_id));
            DCHECK(ref_block->get_by_position(result_column_id).column->size() == row_size)
                    << new_block->get_by_position(idx).name << " size invalid"
                    << ", expect=" << row_size
                    << ", real=" << ref_block->get_by_position(result_column_id).column->size();

            if (ctx->root()->node_type() == TExprNodeType::CAST_EXPR) {
                RETURN_IF_ERROR(
                        _check_cast_valid(ref_block->get_by_position(ref_idx).column,
                                          ref_block->get_by_position(result_column_id).column));
            }
            swap_idx_map[result_column_id] = idx;
        } else {
            // same type, just swap column
            swap_idx_map[ref_idx] = idx;
        }
    }

    for (auto it : swap_idx_map) {
        auto& ref_col = ref_block->get_by_position(it.first);
        auto& new_col = new_block->get_by_position(it.second);

        bool ref_col_nullable = ref_col.column->is_nullable();
        bool new_col_nullable = new_col.column->is_nullable();

        if (ref_col_nullable != new_col_nullable) {
            // not nullable to nullable
            if (new_col_nullable) {
                auto* new_nullable_col = assert_cast<vectorized::ColumnNullable*>(
                        std::move(*new_col.column).mutate().get());

                new_nullable_col->swap_nested_column(ref_col.column);
                new_nullable_col->get_null_map_data().resize_fill(new_nullable_col->size());
            } else {
                // nullable to not nullable:
                // suppose column `c_phone` is originally varchar(16) NOT NULL,
                // then do schema change `alter table test modify column c_phone int not null`,
                // the cast expr of schema change is `CastExpr(CAST String to Nullable(Int32))`,
                // so need to handle nullable to not nullable here
                auto* ref_nullable_col = assert_cast<vectorized::ColumnNullable*>(
                        std::move(*ref_col.column).mutate().get());

                const auto* null_map = ref_nullable_col->get_null_map_column().get_data().data();

                for (size_t i = 0; i < row_size; i++) {
                    if (null_map[i]) {
                        return Status::DataQualityError("is_null of data is changed!");
                    }
                }
                ref_nullable_col->swap_nested_column(new_col.column);
            }
        } else {
            new_block->get_by_position(it.second).column.swap(
                    ref_block->get_by_position(it.first).column);
        }
    }

    return Status::OK();
}

// This check is to prevent schema-change from causing data loss
Status RowBlockChanger::_check_cast_valid(vectorized::ColumnPtr ref_column,
                                          vectorized::ColumnPtr new_column) const {
    if (ref_column->is_nullable() != new_column->is_nullable()) {
        if (ref_column->is_nullable()) {
            return Status::DataQualityError("Can not change nullable column to not nullable");
        } else {
            auto* new_null_map =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(new_column)
                            ->get_null_map_column()
                            .get_data()
                            .data();

            bool is_changed = false;
            for (size_t i = 0; i < ref_column->size(); i++) {
                is_changed |= new_null_map[i];
            }
            if (is_changed) {
                return Status::DataQualityError("is_null of data is changed!");
            }
        }
    }

    if (ref_column->is_nullable() && new_column->is_nullable()) {
        auto* ref_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(ref_column)
                        ->get_null_map_column()
                        .get_data()
                        .data();
        auto* new_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(new_column)
                        ->get_null_map_column()
                        .get_data()
                        .data();

        bool is_changed = false;
        for (size_t i = 0; i < ref_column->size(); i++) {
            is_changed |= (ref_null_map[i] != new_null_map[i]);
        }
        if (is_changed) {
            return Status::DataQualityError("is_null of data is changed!");
        }
    }
    return Status::OK();
}

RowBlockSorter::RowBlockSorter(RowBlockAllocator* row_block_allocator)
        : _row_block_allocator(row_block_allocator), _swap_row_block(nullptr) {}

RowBlockSorter::~RowBlockSorter() {
    if (_swap_row_block) {
        _row_block_allocator->release(_swap_row_block);
        _swap_row_block = nullptr;
    }
}

bool RowBlockSorter::sort(RowBlock** row_block) {
    uint32_t row_num = (*row_block)->row_block_info().row_num;
    bool null_supported = (*row_block)->row_block_info().null_supported;

    if (_swap_row_block == nullptr || _swap_row_block->capacity() < row_num) {
        if (_swap_row_block != nullptr) {
            _row_block_allocator->release(_swap_row_block);
            _swap_row_block = nullptr;
        }

        if (!_row_block_allocator->allocate(&_swap_row_block, row_num, null_supported)) {
            LOG(WARNING) << "fail to allocate memory.";
            return false;
        }
    }

    RowCursor helper_row;
    auto res = helper_row.init(_swap_row_block->tablet_schema());
    if (!res) {
        LOG(WARNING) << "row cursor init failed.res:" << res;
        return false;
    }

    std::vector<std::unique_ptr<RowCursor>> row_cursor_list;
    row_cursor_list.reserve((*row_block)->row_block_info().row_num);
    // create an list of row cursor as long as the number of rows in data block.
    for (size_t i = 0; i < (*row_block)->row_block_info().row_num; ++i) {
        row_cursor_list.emplace_back(new (nothrow) RowCursor());
        if (row_cursor_list[i] == nullptr) {
            LOG(WARNING) << "failed to malloc RowCursor. size=" << sizeof(RowCursor);
            return false;
        }

        if (row_cursor_list[i]->init((*row_block)->tablet_schema()) != Status::OK()) {
            return false;
        }

        (*row_block)->get_row(i, row_cursor_list[i].get());
    }

    // Must use 'std::' because this class has a function whose name is sort too
    std::stable_sort(row_cursor_list.begin(), row_cursor_list.end(), _row_cursor_comparator);

    // copy the results sorted to temp row block.
    _swap_row_block->clear();
    for (size_t i = 0; i < row_cursor_list.size(); ++i) {
        _swap_row_block->get_row(i, &helper_row);
        copy_row(&helper_row, *row_cursor_list[i], _swap_row_block->mem_pool());
    }

    _swap_row_block->finalize(row_cursor_list.size());

    // swap the row block for reducing memory allocating.
    std::swap(*row_block, _swap_row_block);

    return true;
}

RowBlockAllocator::RowBlockAllocator(TabletSchemaSPtr tablet_schema, size_t memory_limitation)
        : _tablet_schema(tablet_schema),
          _tracker(std::make_unique<MemTracker>("RowBlockAllocator")),
          _row_len(tablet_schema->row_size()),
          _memory_limitation(memory_limitation) {
    VLOG_NOTICE << "RowBlockAllocator(). row_len=" << _row_len;
}

RowBlockAllocator::~RowBlockAllocator() {
    if (_tracker->consumption() != 0) {
        LOG(WARNING) << "memory lost in RowBlockAllocator. memory_size=" << _tracker->consumption();
    }
}

Status RowBlockAllocator::allocate(RowBlock** row_block, size_t num_rows, bool null_supported) {
    size_t row_block_size = _row_len * num_rows;

    if (_memory_limitation > 0 && _tracker->consumption() + row_block_size > _memory_limitation) {
        *row_block = nullptr;
        return Status::OLAPInternalError(OLAP_ERR_FETCH_MEMORY_EXCEEDED);
    }

    // TODO(lijiao) : Why abandon the original m_row_block_buffer
    *row_block = new (nothrow) RowBlock(_tablet_schema);

    if (*row_block == nullptr) {
        LOG(WARNING) << "failed to malloc RowBlock. size=" << sizeof(RowBlock);
        return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
    }

    RowBlockInfo row_block_info(0U, num_rows);
    row_block_info.null_supported = null_supported;
    (*row_block)->init(row_block_info);

    _tracker->consume(row_block_size);
    VLOG_NOTICE << "RowBlockAllocator::allocate() this=" << this << ", num_rows=" << num_rows
                << ", m_memory_allocated=" << _tracker->consumption()
                << ", row_block_addr=" << *row_block;
    return Status::OK();
}

void RowBlockAllocator::release(RowBlock* row_block) {
    if (row_block == nullptr) {
        LOG(INFO) << "null row block released.";
        return;
    }

    _tracker->release(row_block->capacity() * _row_len);

    VLOG_NOTICE << "RowBlockAllocator::release() this=" << this
                << ", num_rows=" << row_block->capacity()
                << ", m_memory_allocated=" << _tracker->consumption()
                << ", row_block_addr=" << row_block;
    delete row_block;
}

bool RowBlockAllocator::is_memory_enough_for_sorting(size_t num_rows, size_t allocated_rows) {
    if (num_rows <= allocated_rows) {
        return true;
    }
    size_t row_block_size = _row_len * (num_rows - allocated_rows);
    return _tracker->consumption() + row_block_size < _memory_limitation;
}

RowBlockMerger::RowBlockMerger(TabletSharedPtr tablet) : _tablet(tablet) {}

RowBlockMerger::~RowBlockMerger() = default;

bool RowBlockMerger::merge(const std::vector<RowBlock*>& row_block_arr, RowsetWriter* rowset_writer,
                           uint64_t* merged_rows) {
    uint64_t tmp_merged_rows = 0;
    RowCursor row_cursor;
    std::unique_ptr<MemPool> mem_pool(new MemPool());
    std::unique_ptr<ObjectPool> agg_object_pool(new ObjectPool());

    auto merge_error = [&]() -> bool {
        while (!_heap.empty()) {
            MergeElement element = _heap.top();
            _heap.pop();
            SAFE_DELETE(element.row_cursor);
        }
        return false;
    };

    if (row_cursor.init(_tablet->tablet_schema()) != Status::OK()) {
        LOG(WARNING) << "fail to init row cursor.";
        return merge_error();
    }

    if (!_make_heap(row_block_arr)) {
        // There is error log in _make_heap, so no need to more log.
        return merge_error();
    }

    row_cursor.allocate_memory_for_string_type(_tablet->tablet_schema());
    while (_heap.size() > 0) {
        init_row_with_others(&row_cursor, *(_heap.top().row_cursor), mem_pool.get(),
                             agg_object_pool.get());

        _pop_heap();

        if (KeysType::DUP_KEYS == _tablet->keys_type()) {
            if (rowset_writer->add_row(row_cursor) != Status::OK()) {
                LOG(WARNING) << "fail to add row to rowset writer.";
                return merge_error();
            }
            continue;
        }

        while (!_heap.empty() && compare_row(row_cursor, *_heap.top().row_cursor) == 0) {
            // TODO(zc): Currently we keep nullptr to indicate that this is a query path,
            // we should fix this trick ASAP
            agg_update_row(&row_cursor, *(_heap.top().row_cursor), nullptr);
            ++tmp_merged_rows;
            _pop_heap();
        }
        agg_finalize_row(&row_cursor, mem_pool.get());
        if (rowset_writer->add_row(row_cursor) != Status::OK()) {
            LOG(WARNING) << "fail to add row to rowset writer.";
            return merge_error();
        }

        // the memory allocate by mem pool has been copied,
        // so we should release memory immediately
        mem_pool->clear();
        agg_object_pool.reset(new ObjectPool());
    }
    if (rowset_writer->flush() != Status::OK()) {
        LOG(WARNING) << "failed to finalizing writer.";
        return merge_error();
    }

    *merged_rows = tmp_merged_rows;
    return true;
}

bool RowBlockMerger::_make_heap(const std::vector<RowBlock*>& row_block_arr) {
    for (auto row_block : row_block_arr) {
        MergeElement element;
        element.row_block = row_block;
        element.row_block_index = 0;
        element.row_cursor = new (nothrow) RowCursor();

        if (element.row_cursor == nullptr) {
            LOG(FATAL) << "failed to malloc RowCursor. size=" << sizeof(RowCursor);
            return false;
        }

        if (element.row_cursor->init(element.row_block->tablet_schema()) != Status::OK()) {
            LOG(WARNING) << "failed to init row cursor.";
            SAFE_DELETE(element.row_cursor);
            return false;
        }

        element.row_block->get_row(element.row_block_index, element.row_cursor);

        _heap.push(element);
    }

    return true;
}

void RowBlockMerger::_pop_heap() {
    MergeElement element = _heap.top();
    _heap.pop();

    if (++element.row_block_index >= element.row_block->row_block_info().row_num) {
        SAFE_DELETE(element.row_cursor);
        return;
    }

    element.row_block->get_row(element.row_block_index, element.row_cursor);

    _heap.push(element);
}

Status LinkedSchemaChange::process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                                   TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
                                   TabletSchemaSPtr base_tablet_schema) {
#ifdef CLOUD_MODE
    LOG(FATAL) << "Not support LinkedSchemaChange in cloud mode";
#endif
    // In some cases, there may be more than one type of rowset in a tablet,
    // in which case the conversion cannot be done directly by linked schema change,
    // but requires direct schema change to rewrite the data.
    if (rowset_reader->type() != rowset_writer->type()) {
        LOG(INFO) << "the type of rowset " << rowset_reader->rowset()->rowset_id()
                  << " in base tablet is not same as type " << rowset_writer->type()
                  << ", use direct schema change.";
        return SchemaChangeHandler::get_sc_procedure(_row_block_changer, false, true)
                ->process(rowset_reader, rowset_writer, new_tablet, base_tablet,
                          base_tablet_schema);
    } else {
        Status status = rowset_writer->add_rowset_for_linked_schema_change(rowset_reader->rowset());
        if (!status) {
            LOG(WARNING) << "fail to convert rowset."
                         << ", new_tablet=" << new_tablet->full_name()
                         << ", version=" << rowset_writer->version().first << "-"
                         << rowset_writer->version().second << ", error status " << status;
            return status;
        }
        // copy delete bitmap to new tablet.
        if (new_tablet->keys_type() == UNIQUE_KEYS &&
            new_tablet->enable_unique_key_merge_on_write()) {
            DeleteBitmap origin_delete_bitmap(base_tablet->tablet_id());
            base_tablet->tablet_meta()->delete_bitmap().subset(
                    {rowset_reader->rowset()->rowset_id(), 0, 0},
                    {rowset_reader->rowset()->rowset_id(), UINT32_MAX, INT64_MAX},
                    &origin_delete_bitmap);
            for (auto iter = origin_delete_bitmap.delete_bitmap.begin();
                 iter != origin_delete_bitmap.delete_bitmap.end(); ++iter) {
                int ret = new_tablet->tablet_meta()->delete_bitmap().set(
                        {rowset_writer->rowset_id(), std::get<1>(iter->first),
                         std::get<2>(iter->first)},
                        iter->second);
                DCHECK(ret == 1);
            }
        }
        return Status::OK();
    }
}

SchemaChangeForInvertedIndex::SchemaChangeForInvertedIndex(
            const std::vector<TOlapTableIndex>& alter_inverted_indexs,
            const TabletSchemaSPtr& tablet_schema) 
        : SchemaChange(), _alter_inverted_indexs(alter_inverted_indexs), _tablet_schema(tablet_schema) {}

SchemaChangeForInvertedIndex::~SchemaChangeForInvertedIndex() {
    VLOG_NOTICE << "~SchemaChangeForInvertedIndex()";
    _inverted_index_builders.clear();
    _index_metas.clear();
}

Status SchemaChangeForInvertedIndex::_add_nullable(const std::string& column_name,
            const std::string& index_writer_sign,
            Field* field,
            const uint8_t* null_map, 
            const uint8_t** ptr, 
            size_t num_rows) {
    size_t offset = 0;
    auto next_run_step = [&]() {
        size_t step = 1;
        for (auto i = offset + 1; i < num_rows; ++i) {
            if (null_map[offset] == null_map[i])
                step++;
            else
                break;
        }
        return step;
    };

    try {
        do {
            auto step = next_run_step();
            if (null_map[offset]) {
                RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_nulls(step));
                *ptr += field->size() * step;
            } else {
                if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
                    DCHECK(field->get_sub_field_count() == 1);
                    const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                                                                                field->get_sub_field(0)->size(), col_cursor, step));
                } else {
                    RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(column_name, *ptr,
                                                                                        step));
                }
            }
            offset += step;
        } while (offset < num_rows);
    } catch (const std::exception& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }
    
    return Status::OK();
}

Status SchemaChangeForInvertedIndex::_add_data(const std::string& column_name,
            const std::string& index_writer_sign,
            Field* field,
            const uint8_t** ptr, 
            size_t num_rows) {
    try {
        if (field->type() == FieldType::OLAP_FIELD_TYPE_ARRAY) {
            DCHECK(field->get_sub_field_count() == 1);
            const auto* col_cursor = reinterpret_cast<const CollectionValue*>(*ptr);
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_array_values(
                                                                        field->get_sub_field(0)->size(), col_cursor, num_rows));
        } else {
            RETURN_IF_ERROR(_inverted_index_builders[index_writer_sign]->add_values(column_name, *ptr,
                                                                                num_rows));
        }
    } catch (const std::exception& e) {
        LOG(WARNING) << "CLuceneError occured: " << e.what();
        return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
    }
    
    return Status::OK();
}

Status SchemaChangeForInvertedIndex::_write_inverted_index(
            int32_t segment_idx, 
            vectorized::Block* block) {
    LOG(INFO) << "begin to write inverted index";

    // converter block data
    vectorized::OlapBlockDataConvertor olap_data_convertor;
    olap_data_convertor.reserve(block->columns());
    for (auto& inverted_index: _alter_inverted_indexs) {
        auto column_name = inverted_index.columns[0];
        auto column = _tablet_schema->column(column_name);
        olap_data_convertor.add_column_data_convertor(column);
    }
    olap_data_convertor.set_source_content(block, 0, block->rows());

    int idx = 0;
    for (auto& inverted_index: _alter_inverted_indexs) {
        auto column_name = inverted_index.columns[0];
        auto column = _tablet_schema->column(column_name);
        auto index_id = inverted_index.index_id;

        auto converted_result = olap_data_convertor.convert_column_data(idx++);
        if (converted_result.first != Status::OK()) {
        LOG(WARNING) << "failed to convert block, errcode: " << converted_result.first;
            return converted_result.first;
        }

        std::string writer_sign = std::to_string(segment_idx) + "_" + std::to_string(index_id);
        std::unique_ptr<Field> field(FieldFactory::create(column));
        const auto* ptr = (const uint8_t*)converted_result.second->get_data();
        if (converted_result.second->get_nullmap()) {
            RETURN_IF_ERROR(
                    _add_nullable(column_name, writer_sign, field.get(),
                        converted_result.second->get_nullmap(), &ptr, block->rows()));
        } else {
            RETURN_IF_ERROR(
                _add_data(column_name, writer_sign, field.get(), &ptr, block->rows()));
        }
    }

    return Status::OK();
}

Status SchemaChangeForInvertedIndex::process(
            RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
            TabletSharedPtr new_tablet, TabletSharedPtr base_tablet,
            TabletSchemaSPtr base_tablet_schema) {
    Status res = Status::OK();
    if (rowset_reader->rowset()->empty() || rowset_reader->rowset()->num_rows() == 0) {
        return Status::OK();
    }

    std::vector<ColumnId> return_columns;
    for (auto& inverted_index : _alter_inverted_indexs) {
        DCHECK_EQ(inverted_index.columns.size(), 1);
        auto column_name = inverted_index.columns[0];
        auto idx = _tablet_schema->field_index(column_name);
        return_columns.emplace_back(idx);
    }

    // create inverted index writer
    auto rowset_meta = rowset_reader->rowset()->rowset_meta();
    std::string segment_dir = rowset_meta->is_local()
                ? base_tablet->tablet_path()
                : fmt::format("{}/{}", DATA_PREFIX, base_tablet->tablet_id());
    auto fs = rowset_meta->fs();
    for (auto i = 0; i < rowset_meta->num_segments(); ++i) {
        std::string segment_filename = fmt::format("{}_{}.dat", rowset_meta->rowset_id().to_string(), i);
        for (auto& inverted_index : _alter_inverted_indexs) {
            DCHECK_EQ(inverted_index.columns.size(), 1);
            auto column_name = inverted_index.columns[0];
            auto column = _tablet_schema->column(column_name);
            auto index_id = inverted_index.index_id;

            std::unique_ptr<Field> field(FieldFactory::create(column));
            _index_metas.emplace_back(new TabletIndex());
            _index_metas.back()->init_from_thrift(inverted_index, *_tablet_schema);
            std::unique_ptr<segment_v2::InvertedIndexColumnWriter> inverted_index_builder;
            try {
                RETURN_IF_ERROR(segment_v2::InvertedIndexColumnWriter::create(field.get(), &inverted_index_builder,
                                                              index_id, segment_filename,
                                                              segment_dir,
                                                              _index_metas.back().get(),
                                                              fs));
            } catch (const std::exception& e) {
                LOG(WARNING) << "CLuceneError occured: " << e.what();
                return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
            }

            if (inverted_index_builder) {
                std::string writer_sign = std::to_string(i) + "_" + std::to_string(index_id);
                _inverted_index_builders.insert(std::make_pair(writer_sign, std::move(inverted_index_builder)));
            }
        }
    }

    SegmentCacheHandle segment_cache_handle;
    // load segments
    RETURN_NOT_OK(SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(rowset_reader->rowset()), 
            &segment_cache_handle, false));

    // create iterator for each segment
    StorageReadOptions read_options;
    OlapReaderStatistics stats;
    read_options.stats = &stats;
    read_options.tablet_schema = _tablet_schema;
    std::set<int32_t> output_columns(return_columns.begin(), return_columns.end());
    read_options.output_columns = &output_columns;
    std::unique_ptr<Schema> schema =
            std::make_unique<Schema>(_tablet_schema->columns(), return_columns);
    for (auto& seg_ptr : segment_cache_handle.get_segments()) {
        std::unique_ptr<RowwiseIterator> iter;
        res = seg_ptr->new_iterator(*schema, read_options, &iter);
        if (!res.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << res.to_string();
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_READER_INIT);
        }

        std::shared_ptr<vectorized::Block> block = std::make_shared<vectorized::Block>(
                    _tablet_schema->create_block(return_columns));
        do {
            block->clear_column_data();
            res = iter->next_batch(block.get());
            if (!res.ok()) {
                if (res.is_end_of_file()) {
                    res = Status::OK();
                    break;
                } 
                if (res.is_end_of_file()) {
                    return Status::OLAPInternalError(OLAP_ERR_DATA_EOF);
                }
                RETURN_NOT_OK_LOG(res, "failed to read next block when schema change for inverted index.");
            }

            // copy block
            auto ref_block = *block;

            // write inverted index
            if (_write_inverted_index(iter->data_id(), &ref_block) != Status::OK()) {
                res = Status::OLAPInternalError(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID);
                LOG(WARNING) << "failed to write block.";
                return res;
            }
        } while (block->rows() != 0);
    }

    // finish write inverted index, flush data to compound file
    for (auto i = 0; i < rowset_meta->num_segments(); ++i) {
        for (auto& inverted_index : _alter_inverted_indexs) {
            DCHECK_EQ(inverted_index.columns.size(), 1);
            auto column_name = inverted_index.columns[0];
            auto column = _tablet_schema->column(column_name);
            auto index_id = inverted_index.index_id;
            std::string writer_sign = std::to_string(i) + "_" + std::to_string(index_id);
            try {
                if (_inverted_index_builders[writer_sign]) {
                    _inverted_index_builders[writer_sign]->finish();
                }
            } catch (const std::exception& e) {
                LOG(WARNING) << "CLuceneError occured: " << e.what();
                return Status::OLAPInternalError(OLAP_ERR_IO_ERROR);
            }
        }
    }
    _inverted_index_builders.clear();
    _index_metas.clear();
    
    LOG(INFO) << "all row nums. source_rows=" << rowset_reader->rowset()->num_rows();
    return res;
}

SchemaChangeDirectly::SchemaChangeDirectly(const RowBlockChanger& row_block_changer)
        : _row_block_changer(row_block_changer), _row_block_allocator(nullptr), _cursor(nullptr) {}

SchemaChangeDirectly::~SchemaChangeDirectly() {
    VLOG_NOTICE << "~SchemaChangeDirectly()";
    SAFE_DELETE(_row_block_allocator);
    SAFE_DELETE(_cursor);
}

bool SchemaChangeDirectly::_write_row_block(RowsetWriter* rowset_writer, RowBlock* row_block) {
    for (uint32_t i = 0; i < row_block->row_block_info().row_num; i++) {
        row_block->get_row(i, _cursor);
        if (!rowset_writer->add_row(*_cursor)) {
            LOG(WARNING) << "fail to write to new rowset for direct schema change";
            return false;
        }
    }

    return true;
}

Status reserve_block(std::unique_ptr<RowBlock, RowBlockDeleter>* block_handle_ptr, int row_num,
                     RowBlockAllocator* allocator) {
    auto& block_handle = *block_handle_ptr;
    if (block_handle == nullptr || block_handle->capacity() < row_num) {
        // release old block and alloc new block
        if (block_handle != nullptr) {
            block_handle.reset();
        }
        RowBlock* new_row_block = nullptr;
        auto res = allocator->allocate(&new_row_block, row_num, true);
        RETURN_NOT_OK_LOG(res, "failed to allocate RowBlock.");
        block_handle.reset(new_row_block);
    } else {
        block_handle->clear();
    }
    return Status::OK();
}

Status SchemaChangeDirectly::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                            RowsetWriter* rowset_writer, TabletSharedPtr new_tablet,
                                            TabletSchemaSPtr base_tablet_schema) {
#ifdef CLOUD_MODE
    LOG(FATAL) << "Not support SchemaChangeDirectly in cloud mode";
#endif
    if (_row_block_allocator == nullptr) {
        _row_block_allocator = new RowBlockAllocator(new_tablet->tablet_schema(), 0);
        if (_row_block_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    if (nullptr == _cursor) {
        _cursor = new (nothrow) RowCursor();
        if (nullptr == _cursor) {
            LOG(WARNING) << "fail to allocate row cursor.";
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }

        if (!_cursor->init(new_tablet->tablet_schema())) {
            LOG(WARNING) << "fail to init row cursor.";
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    Status res = Status::OK();

    VLOG_NOTICE << "init writer. new_tablet=" << new_tablet->full_name()
                << ", block_row_number=" << new_tablet->num_rows_per_row_block();

    std::unique_ptr<RowBlock, RowBlockDeleter> new_row_block(nullptr, [&](RowBlock* block) {
        if (block != nullptr) {
            _row_block_allocator->release(block);
        }
    });

    RowBlock* ref_row_block = nullptr;
    rowset_reader->next_block(&ref_row_block);
    while (ref_row_block != nullptr && ref_row_block->has_remaining()) {
        // We will allocate blocks of the same size as before
        // to ensure that the data can be stored
        RETURN_NOT_OK(reserve_block(&new_row_block, ref_row_block->row_block_info().row_num,
                                    _row_block_allocator));

        // Change ref to new. This step is reasonable to say that it does need to wait for a large block, but theoretically it has nothing to do with the writer.
        uint64_t filtered_rows = 0;
        res = _row_block_changer.change_row_block(ref_row_block, rowset_reader->version().second,
                                                  new_row_block.get(), &filtered_rows);
        RETURN_NOT_OK_LOG(res, "failed to change data in row block.");

        // rows filtered by delete handler one by one
        _add_filtered_rows(filtered_rows);

        if (!_write_row_block(rowset_writer, new_row_block.get())) {
            res = Status::OLAPInternalError(OLAP_ERR_SCHEMA_CHANGE_INFO_INVALID);
            LOG(WARNING) << "failed to write row block.";
            return res;
        }

        ref_row_block->clear();
        rowset_reader->next_block(&ref_row_block);
    }

    if (!rowset_writer->flush()) {
        return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
    }

    return res;
}

Status VSchemaChangeDirectly::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                             RowsetWriter* rowset_writer,
                                             TabletSharedPtr new_tablet,
                                             TabletSchemaSPtr base_tablet_schema) {
    do {
        auto new_block =
                std::make_unique<vectorized::Block>(new_tablet->tablet_schema()->create_block());
        auto ref_block = std::make_unique<vectorized::Block>(base_tablet_schema->create_block());

        rowset_reader->next_block(ref_block.get());
        if (ref_block->rows() < 1) {
            break;
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));
        RETURN_IF_ERROR(rowset_writer->add_block(new_block.get()));
    } while (true);

    if (!rowset_writer->flush()) {
        return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
    }

    return Status::OK();
}

SchemaChangeWithSorting::SchemaChangeWithSorting(const RowBlockChanger& row_block_changer,
                                                 size_t memory_limitation)
        : _row_block_changer(row_block_changer),
          _memory_limitation(memory_limitation),
          _temp_delta_versions(Version::mock()),
          _row_block_allocator(nullptr) {}

SchemaChangeWithSorting::~SchemaChangeWithSorting() {
    VLOG_NOTICE << "~SchemaChangeWithSorting()";
    SAFE_DELETE(_row_block_allocator);
}

VSchemaChangeWithSorting::VSchemaChangeWithSorting(const RowBlockChanger& row_block_changer,
                                                   size_t memory_limitation)
        : _changer(row_block_changer),
          _memory_limitation(memory_limitation),
          _temp_delta_versions(Version::mock()) {
    _mem_tracker = std::make_unique<MemTracker>(fmt::format(
            "VSchemaChangeWithSorting:changer={}", std::to_string(int64(&row_block_changer))));
}

Status SchemaChangeWithSorting::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                               RowsetWriter* rowset_writer,
                                               TabletSharedPtr new_tablet,
                                               TabletSchemaSPtr base_tablet_schema) {
#ifdef CLOUD_MODE
    LOG(FATAL) << "Not support SchemaChangeWithSorting in cloud mode";
#endif
    if (_row_block_allocator == nullptr) {
        _row_block_allocator =
                new (nothrow) RowBlockAllocator(new_tablet->tablet_schema(), _memory_limitation);
        if (_row_block_allocator == nullptr) {
            LOG(FATAL) << "failed to malloc RowBlockAllocator. size=" << sizeof(RowBlockAllocator);
            return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
        }
    }

    Status res = Status::OK();
    RowsetSharedPtr rowset = rowset_reader->rowset();

    RowBlockSorter row_block_sorter(_row_block_allocator);

    // for internal sorting
    RowBlock* new_row_block = nullptr;
    std::vector<RowBlock*> row_block_arr;

    // for external sorting
    // src_rowsets to store the rowset generated by internal sorting
    std::vector<RowsetSharedPtr> src_rowsets;

    Defer defer {[&]() {
        // remove the intermediate rowsets generated by internal sorting
        for (auto& row_set : src_rowsets) {
            StorageEngine::instance()->add_unused_rowset(row_set);
        }

        for (auto block : row_block_arr) {
            _row_block_allocator->release(block);
        }

        row_block_arr.clear();
    }};

    _temp_delta_versions.first = _temp_delta_versions.second;

    SegmentsOverlapPB segments_overlap = rowset->rowset_meta()->segments_overlap();
    int64_t oldest_write_timestamp = rowset->oldest_write_timestamp();
    int64_t newest_write_timestamp = rowset->newest_write_timestamp();
    RowBlock* ref_row_block = nullptr;
    rowset_reader->next_block(&ref_row_block);
    while (ref_row_block != nullptr && ref_row_block->has_remaining()) {
        auto st = _row_block_allocator->allocate(&new_row_block,
                                                 ref_row_block->row_block_info().row_num, true);
        // if OLAP_ERR_FETCH_MEMORY_EXCEEDED == st.precise_code()
        // that mean RowBlockAllocator::alocate() memory exceeded.
        // But we can flush row_block_arr if row_block_arr is not empty.
        // Don't return directly.
        if (OLAP_ERR_MALLOC_ERROR == st.precise_code()) {
            return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
        } else if (st) {
            // do memory check for sorting, in case schema change task fail at row block sorting because of
            // not doing internal sorting first
            if (!_row_block_allocator->is_memory_enough_for_sorting(
                        ref_row_block->row_block_info().row_num, row_block_sorter.num_rows())) {
                if (new_row_block != nullptr) {
                    _row_block_allocator->release(new_row_block);
                    new_row_block = nullptr;
                }
            }
        }

        if (new_row_block == nullptr) {
            if (row_block_arr.empty()) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change: "
                             << "memory_limitation=" << _memory_limitation
                             << ". You can increase the memory by changing the config: "
                             << "memory_limitation_per_thread_for_schema_change_bytes";
                return Status::OLAPInternalError(OLAP_ERR_FETCH_MEMORY_EXCEEDED);
            }

            // enter here while memory limitation is reached.
            RowsetSharedPtr rowset;
            if (!_internal_sorting(
                        row_block_arr,
                        Version(_temp_delta_versions.second, _temp_delta_versions.second),
                        oldest_write_timestamp, newest_write_timestamp, new_tablet,
                        segments_overlap, &rowset)) {
                LOG(WARNING) << "failed to sorting internally.";
                return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
            }

            src_rowsets.push_back(rowset);

            for (auto block : row_block_arr) {
                _row_block_allocator->release(block);
            }

            row_block_arr.clear();

            // increase temp version
            ++_temp_delta_versions.second;
            continue;
        }

        uint64_t filtered_rows = 0;
        res = _row_block_changer.change_row_block(ref_row_block, rowset_reader->version().second,
                                                  new_row_block, &filtered_rows);
        if (!res) {
            row_block_arr.push_back(new_row_block);
            LOG(WARNING) << "failed to change data in row block.";
            return res;
        }
        _add_filtered_rows(filtered_rows);

        if (new_row_block->row_block_info().row_num > 0) {
            if (!row_block_sorter.sort(&new_row_block)) {
                row_block_arr.push_back(new_row_block);
                LOG(WARNING) << "failed to sort row block.";
                return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
            }
            row_block_arr.push_back(new_row_block);
        } else {
            LOG(INFO) << "new block num rows is: " << new_row_block->row_block_info().row_num;
            _row_block_allocator->release(new_row_block);
            new_row_block = nullptr;
        }

        ref_row_block->clear();
        rowset_reader->next_block(&ref_row_block);
    }

    if (!row_block_arr.empty()) {
        // enter here while memory limitation is reached.
        RowsetSharedPtr rowset = nullptr;

        if (!_internal_sorting(row_block_arr,
                               Version(_temp_delta_versions.second, _temp_delta_versions.second),
                               oldest_write_timestamp, newest_write_timestamp, new_tablet,
                               segments_overlap, &rowset)) {
            LOG(WARNING) << "failed to sorting internally.";
            return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
        }

        src_rowsets.push_back(rowset);

        for (auto block : row_block_arr) {
            _row_block_allocator->release(block);
        }

        row_block_arr.clear();

        // increase temp version
        ++_temp_delta_versions.second;
    }

    if (src_rowsets.empty()) {
        res = rowset_writer->flush();
        if (!res) {
            LOG(WARNING) << "create empty version for schema change failed."
                         << " version=" << rowset_writer->version().first << "-"
                         << rowset_writer->version().second;
            return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
        }
    } else if (!_external_sorting(src_rowsets, rowset_writer, new_tablet)) {
        LOG(WARNING) << "failed to sorting externally.";
        return Status::OLAPInternalError(OLAP_ERR_ALTER_STATUS_ERR);
    }

    return res;
}

Status VSchemaChangeWithSorting::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                                RowsetWriter* rowset_writer,
                                                TabletSharedPtr new_tablet,
                                                TabletSchemaSPtr base_tablet_schema) {
    // for internal sorting
    std::vector<std::unique_ptr<vectorized::Block>> blocks;

    // for external sorting
    // src_rowsets to store the rowset generated by internal sorting
    std::vector<RowsetSharedPtr> src_rowsets;

    Defer defer {[&]() {
    // remove the intermediate rowsets generated by internal sorting
#ifdef CLOUD_MODE
        for (auto& rs : src_rowsets) {
            WARN_IF_ERROR(rs->remove(), "Failed to remove rowsets generated by internal sorting");
        }
#else
        for (auto& row_set : src_rowsets) {
            StorageEngine::instance()->add_unused_rowset(row_set);
        }
#endif
    }};

    RowsetSharedPtr rowset = rowset_reader->rowset();
    SegmentsOverlapPB segments_overlap = rowset->rowset_meta()->segments_overlap();
    int64_t oldest_write_timestamp = rowset->oldest_write_timestamp();
    int64_t newest_write_timestamp = rowset->newest_write_timestamp();
    _temp_delta_versions.first = _temp_delta_versions.second;

    auto create_rowset = [&]() -> Status {
        if (blocks.empty()) {
            return Status::OK();
        }

        RowsetSharedPtr rowset;
        RETURN_IF_ERROR(_internal_sorting(
                blocks, Version(_temp_delta_versions.second, _temp_delta_versions.second),
                oldest_write_timestamp, newest_write_timestamp, new_tablet, BETA_ROWSET,
                segments_overlap, &rowset));
        src_rowsets.push_back(rowset);

        for (auto& block : blocks) {
            _mem_tracker->release(block->allocated_bytes());
        }
        blocks.clear();

        // increase temp version
        _temp_delta_versions.second++;
        return Status::OK();
    };

    auto new_block =
            std::make_unique<vectorized::Block>(new_tablet->tablet_schema()->create_block());

    do {
        auto ref_block = std::make_unique<vectorized::Block>(base_tablet_schema->create_block());
        rowset_reader->next_block(ref_block.get());
        if (ref_block->rows() < 1) {
            break;
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));
        if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation) {
            RETURN_IF_ERROR(create_rowset());

            if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation) {
                LOG(WARNING) << "Memory limitation is too small for Schema Change."
                             << " _memory_limitation=" << _memory_limitation
                             << ", new_block->allocated_bytes()=" << new_block->allocated_bytes()
                             << ", consumption=" << _mem_tracker->consumption();
                return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR);
            }
        }
        _mem_tracker->consume(new_block->allocated_bytes());

        // move unique ptr
        blocks.push_back(
                std::make_unique<vectorized::Block>(new_tablet->tablet_schema()->create_block()));
        swap(blocks.back(), new_block);
    } while (true);

    RETURN_IF_ERROR(create_rowset());

    if (src_rowsets.empty()) {
        RETURN_IF_ERROR(rowset_writer->flush());
    } else {
        RETURN_IF_ERROR(_external_sorting(src_rowsets, rowset_writer, new_tablet));
    }

    return Status::OK();
}

bool SchemaChangeWithSorting::_internal_sorting(
        const std::vector<RowBlock*>& row_block_arr, const Version& version,
        int64_t oldest_write_timestamp, int64_t newest_write_timestamp, TabletSharedPtr new_tablet,
        SegmentsOverlapPB segments_overlap, RowsetSharedPtr* rowset) {
    uint64_t merged_rows = 0;
    RowBlockMerger merger(new_tablet);

    VLOG_NOTICE << "init rowset builder. tablet=" << new_tablet->full_name()
                << ", block_row_size=" << new_tablet->num_rows_per_row_block();

    std::unique_ptr<RowsetWriter> rowset_writer;
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = segments_overlap;
    context.tablet_schema = new_tablet->tablet_schema();
    context.oldest_write_timestamp = oldest_write_timestamp;
    context.newest_write_timestamp = newest_write_timestamp;
    if (!new_tablet->create_rowset_writer(context, &rowset_writer)) {
        return false;
    }

    if (!merger.merge(row_block_arr, rowset_writer.get(), &merged_rows)) {
        LOG(WARNING) << "failed to merge row blocks.";
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        return false;
    }
    new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                               rowset_writer->rowset_id().to_string());
    _add_merged_rows(merged_rows);
    *rowset = rowset_writer->build();
    return true;
}

Status VSchemaChangeWithSorting::_internal_sorting(
        const std::vector<std::unique_ptr<vectorized::Block>>& blocks, const Version& version,
        int64_t oldest_write_timestamp, int64_t newest_write_timestamp, TabletSharedPtr new_tablet,
        RowsetTypePB new_rowset_type, SegmentsOverlapPB segments_overlap, RowsetSharedPtr* rowset) {
    uint64_t merged_rows = 0;
    MultiBlockMerger merger(new_tablet);

    std::unique_ptr<RowsetWriter> rowset_writer;
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = segments_overlap;
    context.tablet_schema = new_tablet->tablet_schema();
    context.oldest_write_timestamp = oldest_write_timestamp;
    context.newest_write_timestamp = newest_write_timestamp;
    RETURN_IF_ERROR(new_tablet->create_rowset_writer(context, &rowset_writer));

    Defer defer {[&]() {
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
    }};

    RETURN_IF_ERROR(merger.merge(blocks, rowset_writer.get(), &merged_rows));

    _add_merged_rows(merged_rows);
    *rowset = rowset_writer->build();
    return Status::OK();
}

bool SchemaChangeWithSorting::_external_sorting(vector<RowsetSharedPtr>& src_rowsets,
                                                RowsetWriter* rowset_writer,
                                                TabletSharedPtr new_tablet) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : src_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        auto res = rowset->create_reader(&rs_reader);
        if (!res) {
            LOG(WARNING) << "failed to create rowset reader.";
            return false;
        }
        rs_readers.push_back(rs_reader);
    }

    Merger::Statistics stats;
    auto res = Merger::merge_rowsets(new_tablet, READER_ALTER_TABLE, new_tablet->tablet_schema(),
                                     rs_readers, rowset_writer, &stats);
    if (!res) {
        LOG(WARNING) << "failed to merge rowsets. tablet=" << new_tablet->full_name()
                     << ", version=" << rowset_writer->version().first << "-"
                     << rowset_writer->version().second;
        return false;
    }
    _add_merged_rows(stats.merged_rows);
    _add_filtered_rows(stats.filtered_rows);
    return true;
}

Status VSchemaChangeWithSorting::_external_sorting(const vector<RowsetSharedPtr>& src_rowsets,
                                                   RowsetWriter* rowset_writer,
                                                   const TabletSharedPtr& new_tablet) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : src_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        rs_readers.push_back(std::move(rs_reader));
    }

    Merger::Statistics stats;
    RETURN_IF_ERROR(Merger::vmerge_rowsets(new_tablet, READER_ALTER_TABLE,
                                           new_tablet->tablet_schema(), rs_readers, rowset_writer,
                                           &stats));

    _add_merged_rows(stats.merged_rows);
    _add_filtered_rows(stats.filtered_rows);
    return Status::OK();
}

Status SchemaChangeHandler::process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    if (!request.__isset.desc_tbl) {
        return Status::OLAPInternalError(OLAP_ERR_INPUT_PARAMETER_ERROR)
                .append("desc_tbl is not set. Maybe the FE version is not equal to the BE "
                        "version.");
    }

    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version;

    TabletSharedPtr base_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }
    // Lock schema_change_lock util schema change info is stored in tablet header
    std::unique_lock<std::mutex> schema_change_lock(base_tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    Status res = _do_process_alter_tablet_v2(request);
    LOG(INFO) << "finished alter tablet process, res=" << res;
    return res;
}

std::shared_mutex SchemaChangeHandler::_mutex;
std::unordered_set<int64_t> SchemaChangeHandler::_tablet_ids_in_converting;
std::set<std::string> SchemaChangeHandler::_supported_functions = {"hll_hash", "to_bitmap",
                                                                   "to_bitmap_with_check"};

Status SchemaChangeHandler::process_alter_inverted_index(const TAlterInvertedIndexReq& request) {
    LOG(INFO) << "begin to do request alter inverted index: tablet_id=" << request.tablet_id
              << ", schema_hash=" << request.schema_hash
              << ", alter_version=" << request.alter_version;
    TabletSharedPtr tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.tablet_id);
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

    // Lock schema_change_lock util schema change info is stored in tablet header
    std::unique_lock<std::mutex> schema_change_lock(tablet->get_schema_change_lock(),
                                                    std::try_to_lock);
    if (!schema_change_lock.owns_lock()) {
        LOG(WARNING) << "failed to obtain schema change lock. "
                     << "tablet=" << request.tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TRY_LOCK_FAILED);
    }

    Status res = _do_process_alter_inverted_index(tablet, request);
    LOG(INFO) << "finished alter inverted index process, res=" << res;
    return res;
}

Status SchemaChangeHandler::_get_rowset_readers(
            TabletSharedPtr tablet, 
            const TabletSchemaSPtr& tablet_schema,
            const TAlterInvertedIndexReq& request,
            std::vector<RowsetReaderSharedPtr>* rs_readers,
            DeleteHandler* delete_handler) {
    Status res = Status::OK();
    std::vector<Version> versions_to_be_changed;
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

    // obtain base tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> tablet_lock(tablet->get_push_lock());
        std::lock_guard<std::shared_mutex> tablet_wlock(tablet->get_header_lock());

        // reader_context is stack variables, it's lifetime should keep the same with rs_readers
        RowsetReaderContext reader_context;
        reader_context.reader_type = READER_ALTER_TABLE;
        reader_context.tablet_schema = tablet_schema;
        reader_context.need_ordered_result = true;
        reader_context.delete_handler = delete_handler;
        reader_context.return_columns = &return_columns;
        reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
        reader_context.is_vec = true;
        reader_context.is_unique = tablet->keys_type() == UNIQUE_KEYS;

        do {
            RowsetSharedPtr max_rowset;
            // get history data to be converted and it will check if there is hold in base tablet
            res = _get_versions_to_be_changed(tablet, &versions_to_be_changed, &max_rowset);
            if (!res.ok()) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            // should check the max_version >= request.alter_version, if not the convert is useless
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                LOG(WARNING) << "base tablet's max version="
                             << (max_rowset == nullptr ? 0 : max_rowset->end_version())
                             << " is less than request version=" << request.alter_version;
                res = Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
                break;
            }

            // init one delete handler
            int32_t end_version = -1;
            for (auto& version : versions_to_be_changed) {
                if (version.second > end_version) {
                    end_version = version.second;
                }
            }

            res = delete_handler->init(tablet_schema, tablet->delete_predicates(),end_version);
            if (!res.ok()) {
                LOG(WARNING) << "init delete handler failed. base_tablet="
                             << tablet->full_name() << ", end_version=" << end_version;

                // release delete handlers which have been inited successfully.
                delete_handler->finalize();
                break;
            }

            // acquire data sources correspond to history versions
            tablet->capture_rs_readers(versions_to_be_changed, rs_readers);
            if (rs_readers->size() < 1) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers->size();
                res = Status::OLAPInternalError(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS);
                break;
            }

            for (auto& rs_reader : *rs_readers) {
                res = rs_reader->init(&reader_context);
                if (!res.ok()) {
                    LOG(WARNING) << "failed to init rowset reader: " << tablet->full_name();
                    break;
                }
            }

        } while (0);
    }

    return res;
}

Status SchemaChangeHandler::_drop_inverted_index(
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
        auto rowset_meta = rs_reader->rowset()->rowset_meta();
        auto fs = rowset_meta->fs();
        for (auto i = 0; i < rowset_meta->num_segments(); ++i) {
            std::string segment_path = BetaRowset::segment_file_path(tablet->tablet_path(),
                                                                     rowset_meta->rowset_id(), i);
            for (auto& inverted_index: alter_inverted_indexs) {
                auto column_name = inverted_index.columns[0];
                auto column = tablet_schema->column(column_name);
                auto index_id = inverted_index.index_id;

                std::string inverted_index_file =
                    InvertedIndexDescriptor::get_index_file_name(segment_path, index_id);
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

    }

    return Status::OK();
}

Status SchemaChangeHandler::_add_inverted_index(
            std::vector<RowsetReaderSharedPtr> rs_readers, 
            DeleteHandler* delete_handler,
            const TabletSchemaSPtr& tablet_schema,
            TabletSharedPtr tablet, 
            const TAlterInvertedIndexReq& request) {
    Status res = Status::OK();

    std::vector<TOlapTableIndex> alter_inverted_indexs;
    if (request.__isset.alter_inverted_indexes) {
        alter_inverted_indexs = request.alter_inverted_indexes;
    }
    
    {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        _tablet_ids_in_converting.insert(tablet->tablet_id());
    }

    res = _rebuild_inverted_index(rs_readers, delete_handler, tablet_schema, tablet, alter_inverted_indexs);
    
    {
        std::lock_guard<std::shared_mutex> wrlock(_mutex);
        _tablet_ids_in_converting.erase(tablet->tablet_id());
    }

    if (res.ok()) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_alter_result(tablet, request);
    }

    if (!res.ok()) {
        LOG(WARNING) << "failed to alter add inverte index. tablet=" << tablet->full_name();
        return res;
    }

    return Status::OK();
}

Status SchemaChangeHandler::_do_process_alter_inverted_index(TabletSharedPtr tablet, const TAlterInvertedIndexReq& request) {
    Status res = Status::OK();
    std::shared_lock base_migration_rlock(tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }

    TabletSchemaSPtr tablet_schema = std::make_shared<TabletSchema>();
    tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns);

    // get rowset reader
    std::vector<RowsetReaderSharedPtr> rs_readers;
    DeleteHandler delete_handler;
    RETURN_IF_ERROR(_get_rowset_readers(tablet, tablet_schema, request, &rs_readers, &delete_handler));
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

    // TODO xk fe set new schema version
    // update indexes in tablet_schema
    {
        // update tablet level schema
        auto new_tablet_schema = std::make_shared<TabletSchema>();
        new_tablet_schema->update_tablet_columns(*tablet->tablet_schema(), request.columns);
        new_tablet_schema->update_indexes_from_thrift(request.indexes);
        tablet->update_max_version_schema(new_tablet_schema, true);

        std::lock_guard<std::shared_mutex> wlock(tablet->get_header_lock());

        // update tablet level schema
        for (RowsetMetaSharedPtr rowset_meta : tablet->tablet_meta()->all_mutable_rs_metas()) {
            auto new_tablet_schema = std::make_shared<TabletSchema>();
            new_tablet_schema->update_tablet_columns(*rowset_meta->tablet_schema(), request.columns);
            new_tablet_schema->update_indexes_from_thrift(request.indexes);
            rowset_meta->set_tablet_schema(new_tablet_schema);
        }

        tablet->save_meta();
    }
    
    return Status::OK();
}

// In the past schema change and rollup will create new tablet  and will wait for txns starting before the task to finished
// It will cost a lot of time to wait and the task is very difficult to understand.
// In alter task v2, FE will call BE to create tablet and send an alter task to BE to convert historical data.
// The admin should upgrade all BE and then upgrade FE.
// Should delete the old code after upgrade finished.
Status SchemaChangeHandler::_do_process_alter_tablet_v2(const TAlterTabletReqV2& request) {
    Status res = Status::OK();
    TabletSharedPtr base_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.base_tablet_id);
    if (base_tablet == nullptr) {
        LOG(WARNING) << "fail to find base tablet. base_tablet=" << request.base_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    // new tablet has to exist
    TabletSharedPtr new_tablet =
            StorageEngine::instance()->tablet_manager()->get_tablet(request.new_tablet_id);
    if (new_tablet == nullptr) {
        LOG(WARNING) << "fail to find new tablet."
                     << " new_tablet=" << request.new_tablet_id;
        return Status::OLAPInternalError(OLAP_ERR_TABLE_NOT_FOUND);
    }

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (new_tablet->tablet_state() != TABLET_NOTREADY) {
        res = _validate_alter_result(new_tablet, request);
        LOG(INFO) << "tablet's state=" << new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << res;
        return res;
    }

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet "
                 "to new tablet"
              << " base_tablet=" << base_tablet->full_name()
              << " new_tablet=" << new_tablet->full_name();

    std::shared_lock base_migration_rlock(base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }
    std::shared_lock new_migration_rlock(new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::OLAPInternalError(OLAP_ERR_RWLOCK_ERROR);
    }

    std::vector<Version> versions_to_be_changed;
    int64_t end_version = -1;
    // reader_context is stack variables, it's lifetime should keep the same
    // with rs_readers
    RowsetReaderContext reader_context;
    std::vector<RowsetReaderSharedPtr> rs_readers;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;
    // Create a new tablet schema, should merge with dropped columns in light weight schema change
    TabletSchemaSPtr base_tablet_schema = std::make_shared<TabletSchema>();
    base_tablet_schema->update_tablet_columns(*base_tablet->tablet_schema(), request.columns);
    // Use tablet schema directly from base tablet, they are the newest schema, not contain
    // dropped column during light weight schema change.
    // But the tablet schema in base tablet maybe not the latest from FE, so that if fe pass through
    // a tablet schema, then use request schema.
    size_t num_cols = request.columns.empty() ? base_tablet->tablet_schema()->num_columns()
                                              : request.columns.size();
    return_columns.resize(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        return_columns[i] = i;
    }

    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard<std::mutex> base_tablet_lock(base_tablet->get_push_lock());
        std::lock_guard<std::mutex> new_tablet_lock(new_tablet->get_push_lock());
        std::lock_guard<std::shared_mutex> base_tablet_wlock(base_tablet->get_header_lock());
        std::lock_guard<std::shared_mutex> new_tablet_wlock(new_tablet->get_header_lock());

        do {
            RowsetSharedPtr max_rowset;
            // get history data to be converted and it will check if there is hold in base tablet
            if (!_get_versions_to_be_changed(base_tablet, &versions_to_be_changed, &max_rowset)) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            // should check the max_version >= request.alter_version, if not the convert is useless
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                res = Status::InternalError(
                        "base tablet's max version={} is less than request version={}",
                        (max_rowset == nullptr ? 0 : max_rowset->end_version()),
                        request.alter_version);
                break;
            }
            // before calculating version_to_be_changed,
            // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
            LOG(INFO) << "begin to remove all data from new tablet to prevent rewrite."
                      << " new_tablet=" << new_tablet->full_name();
            std::vector<RowsetSharedPtr> rowsets_to_delete;
            std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
            new_tablet->acquire_version_and_rowsets(&version_rowsets);
            std::sort(version_rowsets.begin(), version_rowsets.end(),
                      [](const std::pair<Version, RowsetSharedPtr>& l,
                         const std::pair<Version, RowsetSharedPtr>& r) {
                          return l.first.first < r.first.first;
                      });
            for (auto& pair : version_rowsets) {
                if (pair.first.second <= max_rowset->end_version()) {
                    rowsets_to_delete.push_back(pair.second);
                } else if (pair.first.first <= max_rowset->end_version()) {
                    // If max version is [X-10] and new tablet has version [7-9][10-12],
                    // we only can remove [7-9] from new tablet. If we add [X-10] to new tablet, it will has version
                    // cross: [X-10] [10-12].
                    // So, we should return OLAP_ERR_VERSION_ALREADY_MERGED for fast fail.
                    LOG(WARNING) << "New tablet has a version " << pair.first
                                 << " crossing base tablet's max_version="
                                 << max_rowset->end_version();
                    return Status::OLAPInternalError(OLAP_ERR_VERSION_ALREADY_MERGED);
                }
            }
            std::vector<RowsetSharedPtr> empty_vec;
            new_tablet->modify_rowsets(empty_vec, rowsets_to_delete);
            // inherit cumulative_layer_point from base_tablet
            // check if new_tablet.ce_point > base_tablet.ce_point?
            new_tablet->set_cumulative_layer_point(-1);
            // save tablet meta
            new_tablet->save_meta();
            for (auto& rowset : rowsets_to_delete) {
                // do not call rowset.remove directly, using gc thread to delete it
                StorageEngine::instance()->add_unused_rowset(rowset);
            }

            // init one delete handler
            for (auto& version : versions_to_be_changed) {
                end_version = std::max(end_version, version.second);
            }

            // acquire data sources correspond to history versions
            base_tablet->capture_rs_readers(versions_to_be_changed, &rs_readers);
            if (rs_readers.empty()) {
                LOG(WARNING) << "fail to acquire all data sources. "
                             << "version_num=" << versions_to_be_changed.size()
                             << ", data_source_num=" << rs_readers.size();
                res = Status::OLAPInternalError(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS);
                break;
            }
            auto& all_del_preds = base_tablet->delete_predicates();
            for (auto& delete_pred : all_del_preds) {
                if (delete_pred->version().first > end_version) {
                    continue;
                }
                base_tablet_schema->merge_dropped_columns(
                        base_tablet->tablet_schema(delete_pred->version()));
            }
            res = delete_handler.init(base_tablet_schema, all_del_preds, end_version);
            if (!res) {
                LOG(WARNING) << "init delete handler failed. base_tablet="
                             << base_tablet->full_name() << ", end_version=" << end_version;
                break;
            }

            reader_context.reader_type = READER_ALTER_TABLE;
            reader_context.tablet_schema = base_tablet_schema;
            reader_context.need_ordered_result = true;
            reader_context.delete_handler = &delete_handler;
            reader_context.return_columns = &return_columns;
            reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
            reader_context.is_unique = base_tablet->keys_type() == UNIQUE_KEYS;
            reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
            reader_context.is_vec = config::enable_vectorized_alter_table;
            reader_context.delete_bitmap = &base_tablet->tablet_meta()->delete_bitmap();
            reader_context.version = Version(0, end_version);
            for (auto& rs_reader : rs_readers) {
                res = rs_reader->init(&reader_context);
                if (!res) {
                    LOG(WARNING) << "failed to init rowset reader: " << base_tablet->full_name();
                    break;
                }
            }
        } while (false);
    }

    do {
        if (!res) {
            break;
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
        if (request.__isset.materialized_view_params) {
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

                /*
                * TODO(lhy)
                * Building the materialized view function for schema_change here based on defineExpr.
                * This is a trick because the current storage layer does not support expression evaluation.
                * We can refactor this part of the code until the uniform expression evaluates the logic.
                * count distinct materialized view will set mv_expr with to_bitmap or hll_hash.
                * count materialized view will set mv_expr with count.
                */
                if (item.__isset.mv_expr) {
                    if (item.mv_expr.nodes[0].node_type == TExprNodeType::FUNCTION_CALL) {
                        mv_param.mv_expr = item.mv_expr.nodes[0].fn.name.function_name;
                        if (!_supported_functions.count(mv_param.mv_expr)) {
                            return Status::NotSupported("Unknow materialized view expr " +
                                                        mv_param.mv_expr);
                        }
                    } else if (item.mv_expr.nodes[0].node_type == TExprNodeType::CASE_EXPR) {
                        mv_param.mv_expr = "count_field";
                    }

                    mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
                }
                sc_params.materialized_params_map.insert(
                        std::make_pair(item.column_name, mv_param));
            }
        }
        {
            std::lock_guard<std::shared_mutex> wrlock(_mutex);
            _tablet_ids_in_converting.insert(new_tablet->tablet_id());
        }
        res = _convert_historical_rowsets(sc_params);
        if (new_tablet->keys_type() != UNIQUE_KEYS ||
            !new_tablet->enable_unique_key_merge_on_write() || !res) {
            {
                std::lock_guard<std::shared_mutex> wrlock(_mutex);
                _tablet_ids_in_converting.erase(new_tablet->tablet_id());
            }
        }
        if (!res) {
            break;
        }

        // For unique with merge-on-write table, should process delete bitmap here.
        // 1. During double write, the newly imported rowsets does not calculate
        // delete bitmap and publish successfully.
        // 2. After conversion, calculate delete bitmap for the rowsets imported
        // during double write. During this period, new data can still be imported
        // witout calculating delete bitmap and publish successfully.
        // 3. Block the new publish, calculate the delete bitmap of the
        // incremental rowsets.
        // 4. Switch the tablet status to TABLET_RUNNING. The newly imported
        // data will calculate delete bitmap.
        if (new_tablet->keys_type() == UNIQUE_KEYS &&
            new_tablet->enable_unique_key_merge_on_write()) {
            // step 2
            int64_t max_version = new_tablet->max_version().second;
            std::vector<RowsetSharedPtr> rowsets;
            if (end_version < max_version) {
                LOG(INFO)
                        << "alter table for unique with merge-on-write, calculate delete bitmap of "
                        << "double write rowsets for version: " << end_version + 1 << "-"
                        << max_version;
                RETURN_IF_ERROR(new_tablet->capture_consistent_rowsets(
                        {end_version + 1, max_version}, &rowsets));
            }
            for (auto rowset_ptr : rowsets) {
                if (rowset_ptr->version().second <= end_version) {
                    continue;
                }
                std::lock_guard<std::mutex> rwlock(new_tablet->get_rowset_update_lock());
                std::shared_lock<std::shared_mutex> wrlock(new_tablet->get_header_lock());
                RETURN_IF_ERROR(new_tablet->update_delete_bitmap_without_lock(rowset_ptr));
            }

            // step 3
            std::lock_guard<std::mutex> rwlock(new_tablet->get_rowset_update_lock());
            std::lock_guard<std::shared_mutex> new_wlock(new_tablet->get_header_lock());
            int64_t new_max_version = new_tablet->max_version().second;
            rowsets.clear();
            if (max_version < new_max_version) {
                LOG(INFO)
                        << "alter table for unique with merge-on-write, calculate delete bitmap of "
                        << "incremental rowsets for version: " << max_version + 1 << "-"
                        << new_max_version;
                RETURN_IF_ERROR(new_tablet->capture_consistent_rowsets(
                        {max_version + 1, new_max_version}, &rowsets));
            }
            for (auto rowset_ptr : rowsets) {
                if (rowset_ptr->version().second <= max_version) {
                    continue;
                }
                RETURN_IF_ERROR(new_tablet->update_delete_bitmap_without_lock(rowset_ptr));
            }

            // step 4
            {
                std::lock_guard<std::shared_mutex> wrlock(_mutex);
                _tablet_ids_in_converting.erase(new_tablet->tablet_id());
            }
            res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
            if (!res) {
                break;
            }
            new_tablet->save_meta();
        } else {
            // set state to ready
            std::lock_guard<std::shared_mutex> new_wlock(new_tablet->get_header_lock());
            res = new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
            if (!res) {
                break;
            }
            new_tablet->save_meta();
        }
    } while (false);

    if (res) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_alter_result(new_tablet, request);
    }

    // if failed convert history data, then just remove the new tablet
    if (!res) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << base_tablet->full_name()
                     << ", drop new_tablet=" << new_tablet->full_name();
        // do not drop the new tablet and its data. GC thread will
    }

    return res;
}

bool SchemaChangeHandler::tablet_in_converting(int64_t tablet_id) {
    std::shared_lock rdlock(_mutex);
    return _tablet_ids_in_converting.find(tablet_id) != _tablet_ids_in_converting.end();
}

Status SchemaChangeHandler::_get_versions_to_be_changed(
        const TabletSharedPtr& base_tablet, std::vector<Version>* versions_to_be_changed,
        RowsetSharedPtr* max_rowset) {
    RowsetSharedPtr rowset = base_tablet->rowset_with_max_version();
    if (rowset == nullptr) {
        LOG(WARNING) << "Tablet has no version. base_tablet=" << base_tablet->full_name();
        return Status::OLAPInternalError(OLAP_ERR_ALTER_DELTA_DOES_NOT_EXISTS);
    }
    *max_rowset = rowset;

    RETURN_NOT_OK(base_tablet->capture_consistent_versions(Version(0, rowset->version().second),
                                                           versions_to_be_changed));

    return Status::OK();
}

Status SchemaChangeHandler::_rebuild_inverted_index(
            const std::vector<RowsetReaderSharedPtr>& rs_readers, 
            DeleteHandler* delete_handler,
            const TabletSchemaSPtr& tablet_schema,
            TabletSharedPtr tablet,
            const std::vector<TOlapTableIndex>& alter_inverted_indexs) {
    LOG(INFO) << "begin to rebuild inverted index"
              << ", tablet=" << tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < rs_readers.size(); ++i) {
        if (rs_readers[i]->version().second > end_version) {
            end_version = rs_readers[i]->version().second;
        }
    }

    Status res = Status::OK();
    SchemaChange* sc_procedure =  new (nothrow) SchemaChangeForInvertedIndex(alter_inverted_indexs, tablet_schema);
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

        VLOG_TRACE << "succeed to write inverted index."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }
    SAFE_DELETE(sc_procedure);
    LOG(INFO) << "finish to write inverted index to tablet: " << tablet->full_name();
    return res;
}

Status SchemaChangeHandler::_convert_historical_rowsets(const SchemaChangeParams& sc_params) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << sc_params.base_tablet->full_name()
              << ", new_tablet=" << sc_params.new_tablet->full_name();

    // find end version
    int32_t end_version = -1;
    for (size_t i = 0; i < sc_params.ref_rowset_readers.size(); ++i) {
        if (sc_params.ref_rowset_readers[i]->version().second > end_version) {
            end_version = sc_params.ref_rowset_readers[i]->version().second;
        }
    }

    // Add filter information in change, and filter column information will be set in _parse_request
    // And filter some data every time the row block changes
    RowBlockChanger rb_changer(sc_params.new_tablet->tablet_schema(), sc_params.delete_handler,
                               *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // a.Parse the Alter request and convert it into an internal representation
    Status res = _parse_request(sc_params, &rb_changer, &sc_sorting, &sc_directly);

    auto process_alter_exit = [&]() -> Status {
        {
            // save tablet meta here because rowset meta is not saved during add rowset
            std::lock_guard<std::shared_mutex> new_wlock(sc_params.new_tablet->get_header_lock());
            sc_params.new_tablet->save_meta();
        }
        if (res) {
            Version test_version(0, end_version);
            res = sc_params.new_tablet->check_version_integrity(test_version);
        }

        LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
                  << "base_tablet=" << sc_params.base_tablet->full_name()
                  << ", new_tablet=" << sc_params.new_tablet->full_name();
        return res;
    };

    if (!res) {
        LOG(WARNING) << "failed to parse the request. res=" << res;
        return process_alter_exit();
    }

    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        res = Status::InternalError(
                "Don't support to add materialized view by linked schema change");
        return process_alter_exit();
    }

    // b. Generate historical data converter
    auto sc_procedure = get_sc_procedure(rb_changer, sc_sorting, sc_directly);

    // c.Convert historical data
    for (auto& rs_reader : sc_params.ref_rowset_readers) {
        VLOG_TRACE << "begin to convert a history rowset. version=" << rs_reader->version().first
                   << "-" << rs_reader->version().second;

        // set status for monitor
        // As long as there is a new_table as running, ref table is set as running
        // NOTE If the first sub_table fails first, it will continue to go as normal here
        TabletSharedPtr new_tablet = sc_params.new_tablet;
        // When tablet create new rowset writer, it may change rowset type, in this case
        // linked schema change will not be used.
        std::unique_ptr<RowsetWriter> rowset_writer;
        RowsetWriterContext context;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = new_tablet->tablet_schema();
        context.oldest_write_timestamp = -1;
        context.newest_write_timestamp = -1;
        context.fs = rs_reader->rowset()->rowset_meta()->fs();
        Status status = new_tablet->create_rowset_writer(context, &rowset_writer);
        if (!status.ok()) {
            res = Status::OLAPInternalError(OLAP_ERR_ROWSET_BUILDER_INIT);
            return process_alter_exit();
        }

        if (res = sc_procedure->process(rs_reader, rowset_writer.get(), sc_params.new_tablet,
                                        sc_params.base_tablet, sc_params.base_tablet_schema);
            !res) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                       rowset_writer->rowset_id().to_string());
            return process_alter_exit();
        }
        new_tablet->data_dir()->remove_pending_ids(ROWSET_ID_PREFIX +
                                                   rowset_writer->rowset_id().to_string());
        // Add the new version of the data to the header
        // In order to prevent the occurrence of deadlock, we must first lock the old table, and then lock the new table
        std::lock_guard<std::mutex> lock(sc_params.new_tablet->get_push_lock());
        RowsetSharedPtr new_rowset = rowset_writer->build();
        if (new_rowset == nullptr) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            return process_alter_exit();
        }
        res = sc_params.new_tablet->add_rowset(new_rowset);
        if (res.precise_code() == OLAP_ERR_PUSH_VERSION_ALREADY_EXIST) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << sc_params.new_tablet->full_name() << ", version='"
                         << rs_reader->version().first << "-" << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            res = Status::OK();
        } else if (!res) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << sc_params.new_tablet->full_name()
                         << ", version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            StorageEngine::instance()->add_unused_rowset(new_rowset);
            return process_alter_exit();
        } else {
            VLOG_NOTICE << "register new version. tablet=" << sc_params.new_tablet->full_name()
                        << ", version=" << rs_reader->version().first << "-"
                        << rs_reader->version().second;
        }

        VLOG_TRACE << "succeed to convert a history version."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }

    // XXX:The SchemaChange state should not be canceled at this time, because the new Delta has to be converted to the old and new Schema version
    return process_alter_exit();
}

// @static
// Analyze the mapping of the column and the mapping of the filter key
Status SchemaChangeHandler::_parse_request(const SchemaChangeParams& sc_params,
                                           RowBlockChanger* rb_changer, bool* sc_sorting,
                                           bool* sc_directly) {
    TabletSharedPtr base_tablet = sc_params.base_tablet;
    TabletSharedPtr new_tablet = sc_params.new_tablet;
    TabletSchemaSPtr base_tablet_schema = sc_params.base_tablet_schema;
    const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map =
            sc_params.materialized_params_map;
    DescriptorTbl desc_tbl = *sc_params.desc_tbl;
    // set column mapping
    for (int i = 0, new_schema_size = new_tablet->tablet_schema()->num_columns();
         i < new_schema_size; ++i) {
        const TabletColumn& new_column = new_tablet->tablet_schema()->column(i);
        const string& column_name = new_column.name();
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);
        column_mapping->new_column = &new_column;

        if (materialized_function_map.find(column_name) != materialized_function_map.end()) {
            auto mvParam = materialized_function_map.find(column_name)->second;
            column_mapping->materialized_function = mvParam.mv_expr;
            column_mapping->expr = mvParam.expr;
            int32_t column_index = base_tablet_schema->field_index(mvParam.origin_column_name);
            if (column_index >= 0) {
                column_mapping->ref_column = column_index;
                continue;
            } else {
                LOG(WARNING) << "referenced column was missing. "
                             << "[column=" << column_name << " referenced_column=" << column_index
                             << "]";
                return Status::OLAPInternalError(OLAP_ERR_CE_CMD_PARAMS_ERROR);
            }
        }

        int32_t column_index = base_tablet_schema->field_index(column_name);
        if (column_index >= 0) {
            column_mapping->ref_column = column_index;
            continue;
        }

        // Newly added column go here
        column_mapping->ref_column = -1;

        if (i < base_tablet_schema->num_short_key_columns()) {
            *sc_directly = true;
        }
        RETURN_IF_ERROR(
                _init_column_mapping(column_mapping, new_column, new_column.default_value()));

        VLOG_TRACE << "A column with default value will be added after schema changing. "
                   << "column=" << column_name << ", default_value=" << new_column.default_value();
    }

    // Check if re-aggregation is needed.
    *sc_sorting = false;
    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0, new_schema_size = new_tablet->num_key_columns(); i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);

        if (column_mapping->ref_column < 0) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column != i - num_default_value) {
            *sc_sorting = true;
            return Status::OK();
        }
    }

    TabletSchemaSPtr new_tablet_schema = new_tablet->tablet_schema();
    if (base_tablet_schema->keys_type() != new_tablet_schema->keys_type()) {
        // only when base table is dup and mv is agg
        // the rollup job must be reagg.
        *sc_sorting = true;
        return Status::OK();
    }

    // If the sort of key has not been changed but the new keys num is less then base's,
    // the new table should be re agg.
    // So we also need to set sc_sorting = true.
    // A, B, C are keys(sort keys), D is value
    // followings need resort:
    //      old keys:    A   B   C   D
    //      new keys:    A   B
    if (new_tablet_schema->keys_type() != KeysType::DUP_KEYS &&
        new_tablet->num_key_columns() < base_tablet_schema->num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return Status::OK();
    }

    if (base_tablet_schema->num_short_key_columns() != new_tablet->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    for (size_t i = 0; i < new_tablet->num_columns(); ++i) {
        ColumnMapping* column_mapping = rb_changer->get_mutable_column_mapping(i);
        if (column_mapping->ref_column < 0) {
            continue;
        } else {
            auto column_new = new_tablet_schema->column(i);
            auto column_old = base_tablet_schema->column(column_mapping->ref_column);
            if (column_new.type() != column_old.type() ||
                column_new.precision() != column_old.precision() ||
                column_new.frac() != column_old.frac() ||
                column_new.length() != column_old.length() ||
                column_new.is_bf_column() != column_old.is_bf_column() ||
                column_new.has_bitmap_index() != column_old.has_bitmap_index()) {
                *sc_directly = true;
                return Status::OK();
            }
        }
    }

    if (!sc_params.delete_handler->empty()) {
        // there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
    }

    if (base_tablet->tablet_meta()->preferred_rowset_type() !=
        new_tablet->tablet_meta()->preferred_rowset_type()) {
        // If the base_tablet and new_tablet rowset types are different, just use directly type
        *sc_directly = true;
    }

    // if rs_reader has remote files, link schema change is not supported,
    // use directly schema change instead.
    if (!(*sc_directly) && !(*sc_sorting)) {
        // check has remote rowset
        for (auto& rs_reader : sc_params.ref_rowset_readers) {
            if (!rs_reader->rowset()->is_local()) {
                *sc_directly = true;
                break;
            }
        }
    }

    return Status::OK();
}

Status SchemaChangeHandler::_init_column_mapping(ColumnMapping* column_mapping,
                                                 const TabletColumn& column_schema,
                                                 const std::string& value) {
    column_mapping->default_value = WrapperField::create(column_schema);

    if (column_mapping->default_value == nullptr) {
        return Status::OLAPInternalError(OLAP_ERR_MALLOC_ERROR);
    }

    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        column_mapping->default_value->from_string(value, column_schema.precision(),
                                                   column_schema.frac());
    }

    return Status::OK();
}

Status SchemaChangeHandler::_check_rowset(TabletSharedPtr tablet) {
    std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
    {
        std::shared_lock rdlock(tablet->get_header_lock());
        tablet->acquire_version_and_rowsets(&version_rowsets);
    }
    for (auto& pair : version_rowsets) {
        RowsetSharedPtr rowset = pair.second;
        if (!rowset->check_file_exist()) {
            return Status::OLAPInternalError(OLAP_ERR_FILE_NOT_EXIST);
        }
    }
    return Status::OK();
}

Status SchemaChangeHandler::_validate_alter_result(const TabletSharedPtr& tablet, 
                                                const TAlterInvertedIndexReq& request) {
    Version max_continuous_version = {-1, 0};
    tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.alter_version) {
        return Status::OLAPInternalError(OLAP_ERR_WRITE_PROTOBUF_ERROR);
    }

    RETURN_IF_ERROR(_check_rowset(tablet));
    return Status::OK();
}

Status SchemaChangeHandler::_validate_alter_result(const TabletSharedPtr& new_tablet,
                                                   const TAlterTabletReqV2& request) {
    Version max_continuous_version = {-1, 0};
    new_tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << new_tablet->full_name()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.alter_version) {
        return Status::InternalError("result version={} is less than request version={}",
                                     max_continuous_version.second, request.alter_version);
    }

    RETURN_IF_ERROR(_check_rowset(new_tablet));
    return Status::OK();
}

} // namespace doris
