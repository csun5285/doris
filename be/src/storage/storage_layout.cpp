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

#include "storage/storage_layout.h"

#include <algorithm>
#include <cstring>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "core/assert_cast.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/value/bitmap_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "util/jsonb_document.h"

namespace doris {
namespace storage_layout_detail {

template <FieldType FT>
const uint8_t* BitCast<FT>::column_to_storage(const IColumn& column, size_t row_pos, size_t /*n*/,
                                              StorageBuffers& /*buffers*/,
                                              const uint8_t* /*null_map*/) {
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    return reinterpret_cast<const uint8_t*>(assert_cast<const Column&>(column).get_data().data() +
                                            row_pos);
}

template <FieldType FT>
void repack_from_column(const IColumn& column, size_t row_pos, size_t n, uint8_t* dst,
                        const uint8_t* null_map) {
    using StorageValue = typename CppTypeTraits<FT>::CppType;
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    const auto& data = assert_cast<const Column&>(column).get_data();
    for (size_t i = 0; i < n; ++i) {
        // A NULL item can hold an invalid V1 temporal/decimal payload; never convert it.
        StorageValue value {};
        if (null_map == nullptr || null_map[i] == 0) {
            value = StorageLayout<FT>::to_storage(data[row_pos + i]);
        }
        memcpy(dst + i * sizeof(StorageValue), &value, sizeof(StorageValue));
    }
}

template <FieldType FT>
const uint8_t* FloatingPoint<FT>::column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                                    StorageBuffers& buffers,
                                                    const uint8_t* /*null_map*/) {
    using Column =
            typename PrimitiveTypeTraits<storage_field_type_to_primitive_type(FT)>::ColumnType;
    const PrimitiveValue* values = assert_cast<const Column&>(column).get_data().data() + row_pos;
    const auto* first_nan = std::find_if(values, values + n,
                                         [](PrimitiveValue value) { return std::isnan(value); });
    if (first_nan == values + n) {
        return reinterpret_cast<const uint8_t*>(values);
    }
    auto& bytes = buffers.bytes;
    bytes.resize(n * sizeof(StorageValue));
    const auto prefix_rows = static_cast<size_t>(first_nan - values);
    memcpy(bytes.data(), values, prefix_rows * sizeof(StorageValue));
    for (size_t i = prefix_rows; i < n; ++i) {
        const StorageValue value = to_storage(values[i]);
        memcpy(bytes.data() + i * sizeof(StorageValue), &value, sizeof(StorageValue));
    }
    return reinterpret_cast<const uint8_t*>(bytes.data());
}

const uint8_t* Bytes::column_to_storage(const IColumn& column, size_t row_pos, size_t n,
                                        StorageBuffers& buffers, const uint8_t* null_map) {
    const auto& strings = assert_cast<const ColumnString&>(column);
    auto& cells = buffers.cells;
    cells.resize(n);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            cells[i] = Slice();
            continue;
        }
        const auto value = strings.get_data_at(row_pos + i);
        cells[i] = Slice(value.data, value.size);
    }
    return reinterpret_cast<const uint8_t*>(cells.data());
}

// Serialises the selected non-NULL values of an object column into buffers.bytes, sized
// once from size_of, and describes each one with a cell. write returns the bytes a value
// took, at most size_of(value).
template <class Column, class SizeOf, class Write>
const uint8_t* serialize_batch(const IColumn& column, size_t row_pos, size_t n,
                               StorageBuffers& buffers, const uint8_t* null_map, SizeOf size_of,
                               Write write) {
    const auto* values = assert_cast<const Column&>(column).get_data().data() + row_pos;
    size_t total_size = 0;
    for (size_t i = 0; i < n; ++i) {
        if (null_map == nullptr || null_map[i] == 0) {
            total_size += size_of(values[i]);
        }
    }
    auto& bytes = buffers.bytes;
    auto& cells = buffers.cells;
    bytes.resize(total_size);
    cells.resize(n);
    char* out = bytes.data();
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            cells[i] = Slice();
            continue;
        }
        const size_t size = write(values[i], out);
        cells[i] = Slice(out, size);
        out += size;
    }
    DCHECK_LE(out, bytes.data() + bytes.size());
    return reinterpret_cast<const uint8_t*>(cells.data());
}

} // namespace storage_layout_detail

// Match the extern template declarations in storage_layout.h.
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BOOL>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TINYINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_SMALLINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_INT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_BIGINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_LARGEINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL32>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL64>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL128I>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DECIMAL256>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATEV2>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_DATETIMEV2>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV4>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_IPV6>;
template struct storage_layout_detail::BitCast<FieldType::OLAP_FIELD_TYPE_UUID>;
template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_FLOAT>;
template struct storage_layout_detail::FloatingPoint<FieldType::OLAP_FIELD_TYPE_DOUBLE>;

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATE>(
            column, row_pos, n, reinterpret_cast<uint8_t*>(buffers.bytes.data()), null_map);
    return reinterpret_cast<const uint8_t*>(buffers.bytes.data());
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DATETIME>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DATETIME>(
            column, row_pos, n, reinterpret_cast<uint8_t*>(buffers.bytes.data()), null_map);
    return reinterpret_cast<const uint8_t*>(buffers.bytes.data());
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_DECIMAL>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    buffers.bytes.resize(n * sizeof(StorageValue));
    storage_layout_detail::repack_from_column<FieldType::OLAP_FIELD_TYPE_DECIMAL>(
            column, row_pos, n, reinterpret_cast<uint8_t*>(buffers.bytes.data()), null_map);
    return reinterpret_cast<const uint8_t*>(buffers.bytes.data());
}

bool StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::is_padded(const IColumn& column,
                                                               size_t length) {
    const uint8_t* null_map = nullptr;
    const auto& strings = assert_cast<const ColumnString&>(peel_nullable(column, 0, &null_map));
    return strings.get_chars().size() == strings.size() * length;
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, size_t length, StorageBuffers& buffers,
        const uint8_t* null_map) {
    const auto& strings = assert_cast<const ColumnString&>(column);
    ColumnString::check_chars_length(n * length, n);
    auto& bytes = buffers.bytes;
    auto& cells = buffers.cells;
    bytes.resize(n * length);
    // Erase old padding when a reused slot changes, e.g. "abcd" -> "a" in CHAR(4).
    memset(bytes.data(), 0, bytes.size());
    cells.resize(n);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            cells[i] = Slice();
            continue;
        }
        const auto value = strings.get_data_at(row_pos + i);
        DCHECK_LE(value.size, length);
        char* data = bytes.data() + i * length;
        memcpy(data, value.data, value.size);
        cells[i] = Slice(data, length);
    }
    return reinterpret_cast<const uint8_t*>(cells.data());
}

void StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::pad_column(const IColumn& column,
                                                                size_t row_pos, size_t n,
                                                                size_t length, ColumnString& out) {
    const uint8_t* null_map = nullptr;
    const auto& strings =
            assert_cast<const ColumnString&>(peel_nullable(column, row_pos, &null_map));
    DCHECK_LE(row_pos + n, strings.size());
    ColumnString::check_chars_length(n * length, n);
    auto& chars = out.get_chars();
    // Reusing a shorter batch must erase bytes from the previous one as well, e.g.
    // replacing "abcd" with "a" in CHAR(4) must leave three zero bytes.
    chars.resize(n * length);
    memset(chars.data(), 0, chars.size());
    auto& offsets = out.get_offsets();
    offsets.resize(n);
    for (size_t i = 0; i < n; ++i) {
        offsets[i] = cast_set<ColumnString::Offset, size_t, false>((i + 1) * length);
        if (null_map != nullptr && null_map[i] != 0) {
            continue;
        }
        const StringRef value = strings.get_data_at(row_pos + i);
        DCHECK_LE(value.size, length);
        memcpy(chars.data() + i * length, value.data, value.size);
    }
}

void StorageLayout<FieldType::OLAP_FIELD_TYPE_CHAR>::pad_column_with_nulls(const IColumn& column,
                                                                           size_t row_pos, size_t n,
                                                                           size_t length,
                                                                           IColumn& out) {
    if (const auto* nullable = check_and_get_column<ColumnNullable>(column)) {
        auto& stored = assert_cast<ColumnNullable&>(out);
        pad_column(column, row_pos, n, length,
                   assert_cast<ColumnString&>(stored.get_nested_column()));
        const auto& null_map = nullable->get_null_map_data();
        stored.get_null_map_data().assign(null_map.begin() + row_pos,
                                          null_map.begin() + row_pos + n);
    } else {
        pad_column(column, row_pos, n, length, assert_cast<ColumnString&>(out));
    }
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_BITMAP>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    return storage_layout_detail::serialize_batch<ColumnBitmap>(
            column, row_pos, n, buffers, null_map,
            [](const BitmapValue& value) { return value.getSizeInBytes(); },
            [](const BitmapValue& value, char* out) {
                value.write_to(out);
                return value.getSizeInBytes();
            });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_HLL>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    return storage_layout_detail::serialize_batch<ColumnHLL>(
            column, row_pos, n, buffers, null_map,
            [](const HyperLogLog& value) { return value.max_serialized_size(); },
            [](const HyperLogLog& value, char* out) {
                return value.serialize(reinterpret_cast<uint8_t*>(out));
            });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    return storage_layout_detail::serialize_batch<ColumnQuantileState>(
            column, row_pos, n, buffers, null_map,
            [](const QuantileState& value) { return value.get_serialized_size(); },
            [](const QuantileState& value, char* out) {
                return value.serialize(reinterpret_cast<uint8_t*>(out));
            });
}

const uint8_t* StorageLayout<FieldType::OLAP_FIELD_TYPE_AGG_STATE>::column_to_storage(
        const IColumn& column, size_t row_pos, size_t n, StorageBuffers& buffers,
        const uint8_t* null_map) {
    const auto& objects = assert_cast<const ColumnFixedLengthObject&>(column);
    const size_t item_size = objects.item_size();
    const auto* data =
            reinterpret_cast<const char*>(objects.get_data().data()) + row_pos * item_size;
    auto& cells = buffers.cells;
    cells.resize(n);
    for (size_t i = 0; i < n; ++i) {
        cells[i] = null_map != nullptr && null_map[i] != 0 ? Slice()
                                                           : Slice(data + i * item_size, item_size);
    }
    return reinterpret_cast<const uint8_t*>(cells.data());
}

Status admit_storage_rows(FieldType type, const IColumn& column, size_t row_pos, size_t n,
                          const uint8_t* null_map) {
    if (type != FieldType::OLAP_FIELD_TYPE_STRING && type != FieldType::OLAP_FIELD_TYPE_JSONB) {
        return Status::OK();
    }
    const auto& strings = assert_cast<const ColumnString&>(column);
    for (size_t i = 0; i < n; ++i) {
        if (null_map != nullptr && null_map[i] != 0) {
            continue;
        }
        const StringRef value = strings.get_data_at(row_pos + i);
        if (UNLIKELY(value.size > config::string_type_length_soft_limit_bytes)) {
            return Status::NotSupported(
                    "Not support string len over than `string_type_length_soft_limit_bytes`"
                    " in vec engine.");
        }
        // Make sure that the json binary data written in is the correct jsonb value.
        if (type == FieldType::OLAP_FIELD_TYPE_JSONB) {
            const JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value.data, value.size, &doc));
        }
    }
    return Status::OK();
}

} // namespace doris
