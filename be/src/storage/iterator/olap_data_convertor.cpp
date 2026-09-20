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

#include "storage/iterator/olap_data_convertor.h"

#include <memory>
#include <new>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/block.h"
#include "core/column/column.h"
#include "core/column/column_array.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_map.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_struct.h"
#include "core/column/column_vector.h"
#include "core/column/variant_v2/column_variant_v2.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_struct.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "core/value/vdatetime_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/jsonb_document.h"

namespace doris {

ColumnDataConvertorUPtr create_agg_state_data_convertor(const TabletColumn& column) {
    auto data_type = DataTypeFactory::instance().create_data_type(column);
    const auto* agg_state_type = assert_cast<const DataTypeAggState*>(data_type.get());
    auto type = agg_state_type->get_serialized_type()->get_primitive_type();

    // Terialized type of most functions is string, and some of them are fixed object.
    // Finally, the serialized type of some special functions is bitmap/array/map...
    if (type == PrimitiveType::TYPE_STRING) {
        return std::make_unique<VarcharDataConvertor>(false);
    } else if (type == PrimitiveType::TYPE_BITMAP) {
        return std::make_unique<BitmapDataConvertor>();
    } else if (type == PrimitiveType::TYPE_FIXED_LENGTH_OBJECT) {
        // INVALID_TYPE means function's serialized type is fixed object
        return std::make_unique<AggStateDataConvertor>();
    } else {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "OLAP_FIELD_TYPE_AGG_STATE meet unsupported type: {}",
                        agg_state_type->get_name());
    }
}

ColumnDataConvertorUPtr create_column_data_convertor(const TabletColumn& column) {
    switch (column.type()) {
    case FieldType::OLAP_FIELD_TYPE_BITMAP: {
        return std::make_unique<BitmapDataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_QUANTILE_STATE: {
        return std::make_unique<QuantileStateDataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_AGG_STATE: {
        return create_agg_state_data_convertor(column);
    }
    case FieldType::OLAP_FIELD_TYPE_HLL: {
        return std::make_unique<HllDataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_CHAR: {
        return std::make_unique<CharDataConvertor>(column.length());
    }
    case FieldType::OLAP_FIELD_TYPE_VARCHAR: {
        return std::make_unique<VarcharDataConvertor>(false);
    }
    case FieldType::OLAP_FIELD_TYPE_STRING: {
        return std::make_unique<VarcharDataConvertor>(true);
    }
    case FieldType::OLAP_FIELD_TYPE_DATE: {
        return std::make_unique<DateV1DataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIME: {
        return std::make_unique<DateTimeV1DataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATEV2: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DATEV2>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DATETIMEV2: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DATETIMEV2>>();
    }
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMP_NS: {
        return std::make_unique<PassthroughDataConvertor<TYPE_TIMESTAMP_NS>>();
    }
    case FieldType::OLAP_FIELD_TYPE_TIMESTAMPTZ: {
        return std::make_unique<PassthroughDataConvertor<TYPE_TIMESTAMPTZ>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL: {
        return std::make_unique<DecimalV1DataConvertor>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL32: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DECIMAL32>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL64: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DECIMAL64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL128I: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DECIMAL128I>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DECIMAL256: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DECIMAL256>>();
    }
    case FieldType::OLAP_FIELD_TYPE_JSONB: {
        return std::make_unique<VarcharDataConvertor>(true, true);
    }
    case FieldType::OLAP_FIELD_TYPE_BOOL: {
        return std::make_unique<PassthroughDataConvertor<TYPE_BOOLEAN>>();
    }
    case FieldType::OLAP_FIELD_TYPE_TINYINT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_TINYINT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_SMALLINT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_SMALLINT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_INT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_INT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_BIGINT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_BIGINT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_UNSIGNED_BIGINT: {
        // used by internal length/offset columns (e.g. ColumnOffset64).
        return std::make_unique<PassthroughDataConvertor<TYPE_UINT64>>();
    }
    case FieldType::OLAP_FIELD_TYPE_LARGEINT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_LARGEINT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV4: {
        return std::make_unique<PassthroughDataConvertor<TYPE_IPV4>>();
    }
    case FieldType::OLAP_FIELD_TYPE_IPV6: {
        return std::make_unique<PassthroughDataConvertor<TYPE_IPV6>>();
    }
    case FieldType::OLAP_FIELD_TYPE_FLOAT: {
        return std::make_unique<PassthroughDataConvertor<TYPE_FLOAT>>();
    }
    case FieldType::OLAP_FIELD_TYPE_DOUBLE: {
        return std::make_unique<PassthroughDataConvertor<TYPE_DOUBLE>>();
    }
    case FieldType::OLAP_FIELD_TYPE_VARIANT: {
        // A variant column's storage-facing form is its root JSONB, a string
        // column. Subcolumns get encoders from their own TabletColumn.
        return std::make_unique<VarcharDataConvertor>(true);
    }
    default: {
        throw Exception(ErrorCode::INTERNAL_ERROR, "Invalid type in olap data convertor: {}",
                        int(column.type()));
    }
    }
}

const IColumn& ColumnDataConvertor::bind(const IColumn& column, size_t row_pos, size_t num_rows,
                                         ColumnStorageScratch& scratch) {
    DCHECK(row_pos + num_rows <= column.size())
            << "row_pos=" << row_pos << ", num_rows=" << num_rows
            << ", column.size()=" << column.size();
    scratch.data = nullptr;
    scratch.nullmap = nullptr;
    // Drop the previous batch's cells but keep their allocation: a zero-copy
    // batch must not report stale copies, and the next copying batch reuses
    // the buffer.
    scratch.bytes.clear();
    scratch.slices.clear();
    if (is_column_nullable(column)) {
        const auto* nullable_column = assert_cast<const ColumnNullable*>(&column);
        scratch.nullmap = nullable_column->get_null_map_data().data() + row_pos;
        return nullable_column->get_nested_column();
    }
    return column;
}

Status BitmapDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                   ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind_object(column, row_pos, num_rows, scratch);
    const auto* column_bitmap = assert_cast<const ColumnBitmap*>(&nested);

    const BitmapValue* bitmap_value = column_bitmap->get_data().data() + row_pos;
    const BitmapValue* bitmap_value_cur = bitmap_value;
    const BitmapValue* bitmap_value_end = bitmap_value_cur + num_rows;

    size_t total_size = 0;
    if (scratch.nullmap != nullptr) {
        const UInt8* nullmap_cur = scratch.nullmap;
        while (bitmap_value_cur != bitmap_value_end) {
            if (!*nullmap_cur) {
                total_size += bitmap_value_cur->getSizeInBytes();
            }
            ++nullmap_cur;
            ++bitmap_value_cur;
        }
    } else {
        while (bitmap_value_cur != bitmap_value_end) {
            total_size += bitmap_value_cur->getSizeInBytes();
            ++bitmap_value_cur;
        }
    }
    scratch.bytes.resize(total_size);

    bitmap_value_cur = bitmap_value;
    size_t slice_size;
    char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
    Slice* slice = scratch.slices.data();
    if (scratch.nullmap != nullptr) {
        const UInt8* nullmap_cur = scratch.nullmap;
        while (bitmap_value_cur != bitmap_value_end) {
            if (!*nullmap_cur) {
                slice_size = bitmap_value_cur->getSizeInBytes();
                bitmap_value_cur->write_to(raw_data);

                slice->data = raw_data;
                slice->size = slice_size;
                raw_data += slice_size;
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            ++slice;
            ++nullmap_cur;
            ++bitmap_value_cur;
        }
        assert(nullmap_cur == scratch.nullmap + num_rows && slice == scratch.slices.get_end_ptr());
    } else {
        while (bitmap_value_cur != bitmap_value_end) {
            slice_size = bitmap_value_cur->getSizeInBytes();
            bitmap_value_cur->write_to(raw_data);

            slice->data = raw_data;
            slice->size = slice_size;
            raw_data += slice_size;

            ++slice;
            ++bitmap_value_cur;
        }
        assert(slice == scratch.slices.get_end_ptr());
    }
    return Status::OK();
}

Status QuantileStateDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                          ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind_object(column, row_pos, num_rows, scratch);
    if (scratch.nullmap != nullptr) {
        return Status::NotSupported("QuantileState column does not support nullable");
    }
    const auto* column_quantile_state = assert_cast<const ColumnQuantileState*>(&nested);

    const QuantileState* quantile_state = column_quantile_state->get_data().data() + row_pos;
    const QuantileState* quantile_state_cur = quantile_state;
    const QuantileState* quantile_state_end = quantile_state_cur + num_rows;

    size_t total_size = 0;
    while (quantile_state_cur != quantile_state_end) {
        total_size += quantile_state_cur->get_serialized_size();
        ++quantile_state_cur;
    }
    scratch.bytes.resize(total_size);

    quantile_state_cur = quantile_state;
    size_t slice_size;
    char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
    Slice* slice = scratch.slices.data();

    while (quantile_state_cur != quantile_state_end) {
        slice_size = quantile_state_cur->get_serialized_size();
        quantile_state_cur->serialize((uint8_t*)raw_data);

        slice->data = raw_data;
        slice->size = slice_size;
        raw_data += slice_size;

        ++slice;
        ++quantile_state_cur;
    }
    assert(slice == scratch.slices.get_end_ptr());
    return Status::OK();
}

Status HllDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind_object(column, row_pos, num_rows, scratch);
    if (scratch.nullmap != nullptr) {
        return Status::NotSupported("HLL column does not support nullable");
    }
    const auto* column_hll = assert_cast<const ColumnHLL*>(&nested);

    const HyperLogLog* hll_value = column_hll->get_data().data() + row_pos;
    const HyperLogLog* hll_value_cur = hll_value;
    const HyperLogLog* hll_value_end = hll_value_cur + num_rows;

    size_t total_size = 0;
    while (hll_value_cur != hll_value_end) {
        total_size += hll_value_cur->max_serialized_size();
        ++hll_value_cur;
    }
    scratch.bytes.resize(total_size);

    size_t slice_size;
    char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
    Slice* slice = scratch.slices.data();

    hll_value_cur = hll_value;
    while (hll_value_cur != hll_value_end) {
        slice_size = hll_value_cur->serialize((uint8_t*)raw_data);

        slice->data = raw_data;
        slice->size = slice_size;
        raw_data += slice_size;

        ++slice;
        ++hll_value_cur;
    }
    assert(slice == scratch.slices.get_end_ptr());
    return Status::OK();
}

// class CharDataConvertor
CharDataConvertor::CharDataConvertor(size_t length) : _length(length) {
    assert(length > 0);
}

Status CharDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                 ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind(column, row_pos, num_rows, scratch);
    scratch.slices.resize(num_rows);
    scratch.data = reinterpret_cast<const uint8_t*>(scratch.slices.data());
    const auto* column_string = assert_cast<const ColumnString*>(&nested);

    // A column read back from a segment is already padded to full width, so the
    // slices can point straight into it and nothing is copied.
    if (!should_padding(column_string, _length)) {
        for (size_t i = 0; i < num_rows; i++) {
            if (scratch.nullmap == nullptr || !scratch.nullmap[i]) {
                scratch.slices[i] = column_string->get_data_at(i + row_pos).to_slice();
                DCHECK(scratch.slices[i].size == _length)
                        << "char type data length not equal to schema, schema=" << _length
                        << ", real=" << scratch.slices[i].size;
            }
        }
        return Status::OK();
    }

    // Otherwise pad into the scratch, which the caller reuses across batches.
    // Only the rows being encoded are padded, and a null row's cell stays all
    // zeroes -- nothing reads it, since the writers skip null runs.
    scratch.bytes.resize(num_rows * _length);
    auto* padded = reinterpret_cast<char*>(scratch.bytes.data());
    memset(padded, 0, num_rows * _length);
    for (size_t i = 0; i < num_rows; i++) {
        char* cell = padded + i * _length;
        scratch.slices[i] = Slice(cell, _length);
        if (scratch.nullmap != nullptr && scratch.nullmap[i]) {
            continue;
        }
        const auto str = column_string->get_data_at(i + row_pos);
        DCHECK(str.size <= _length)
                << "char type data length over limit, schema=" << _length << ", real=" << str.size;
        if (str.size != 0) {
            memcpy(cell, str.data, str.size);
        }
    }

    return Status::OK();
}

// class VarcharDataConvertor
VarcharDataConvertor::VarcharDataConvertor(bool check_length, bool is_jsonb)
        : _check_length(check_length), _is_jsonb(is_jsonb) {}

Status VarcharDataConvertor::encode_string(const UInt8* null_map, const ColumnString* column_string,
                                           size_t row_pos, size_t num_rows,
                                           ColumnStorageScratch& scratch) const {
    assert(column_string);
    const char* char_data = (const char*)(column_string->get_chars().data());
    const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + row_pos;
    const ColumnString::Offset* offset_end = offset_cur + num_rows;

    Slice* slice = scratch.slices.data();
    size_t string_offset = *(offset_cur - 1);
    if (null_map) {
        const UInt8* nullmap_cur = null_map;
        while (offset_cur != offset_end) {
            if (!*nullmap_cur) {
                slice->data = const_cast<char*>(char_data + string_offset);
                slice->size = *offset_cur - string_offset;
                if (UNLIKELY(slice->size > config::string_type_length_soft_limit_bytes &&
                             _check_length)) {
                    return Status::NotSupported(
                            "Not support string len over than "
                            "`string_type_length_soft_limit_bytes` in vec engine.");
                }
                // Make sure that the json binary data written in is the correct jsonb value.
                if (_is_jsonb) {
                    const JsonbDocument* doc = nullptr;
                    RETURN_IF_ERROR(doris::JsonbDocument::checkAndCreateDocument(
                            slice->data, slice->size, &doc));
                }
            } else {
                // TODO: this may not be necessary, check and remove later
                slice->data = nullptr;
                slice->size = 0;
            }
            string_offset = *offset_cur;
            ++nullmap_cur;
            ++slice;
            ++offset_cur;
        }
        assert(nullmap_cur == null_map + num_rows && slice == scratch.slices.get_end_ptr());
    } else {
        while (offset_cur != offset_end) {
            slice->data = const_cast<char*>(char_data + string_offset);
            slice->size = *offset_cur - string_offset;
            if (UNLIKELY(slice->size > config::string_type_length_soft_limit_bytes &&
                         _check_length)) {
                return Status::NotSupported(
                        "Not support string len over than `string_type_length_soft_limit_bytes`"
                        " in vec engine.");
            }
            // Make sure that the json binary data written in is the correct jsonb value.
            if (_is_jsonb) {
                const JsonbDocument* doc = nullptr;
                RETURN_IF_ERROR(doris::JsonbDocument::checkAndCreateDocument(slice->data,
                                                                             slice->size, &doc));
            }
            string_offset = *offset_cur;
            ++slice;
            ++offset_cur;
        }
        assert(slice == scratch.slices.get_end_ptr());
    }
    return Status::OK();
}

Status VarcharDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                    ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind(column, row_pos, num_rows, scratch);
    scratch.slices.resize(num_rows);
    scratch.data = reinterpret_cast<const uint8_t*>(scratch.slices.data());
    const auto* column_string = assert_cast<const ColumnString*>(&nested);
    RETURN_IF_ERROR(encode_string(scratch.nullmap, column_string, row_pos, num_rows, scratch));
    return Status::OK();
}

Status AggStateDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                     ColumnStorageScratch& scratch) const {
    const IColumn& nested = bind(column, row_pos, num_rows, scratch);
    scratch.slices.resize(num_rows);
    scratch.data = reinterpret_cast<const uint8_t*>(scratch.slices.data());
    if (scratch.nullmap != nullptr) {
        return Status::NotSupported("AGG_STATE column does not support nullable");
    }
    const auto* column_fixed_object = assert_cast<const ColumnFixedLengthObject*>(&nested);

    auto item_size = column_fixed_object->item_size();

    auto* cur_values = (uint8_t*)(column_fixed_object->get_data().data()) + (item_size * row_pos);
    auto* end_values = cur_values + (item_size * num_rows);
    Slice* slice = scratch.slices.data();

    while (cur_values != end_values) {
        slice->data = reinterpret_cast<char*>(cur_values);
        slice->size = item_size;
        ++slice;
        cur_values = cur_values + item_size;
    }
    assert(slice == scratch.slices.get_end_ptr());

    return Status::OK();
}

Status DateV1DataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                   ColumnStorageScratch& scratch) const {
    uint24_t* cells = nullptr;
    const IColumn& nested = bind_repack(column, row_pos, num_rows, scratch, &cells);
    const auto* column_datetime = assert_cast<const ColumnDate*>(&nested);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + num_rows;
    uint24_t* value = cells;
    if (scratch.nullmap != nullptr) {
        const UInt8* nullmap_cur = scratch.nullmap;
        while (datetime_cur != datetime_end) {
            if (!*nullmap_cur) {
                *value = datetime_cur->to_olap_date();
            } else {
                // do nothing
            }
            ++value;
            ++datetime_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == scratch.nullmap + num_rows && value == cells + num_rows);
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_date();
            ++value;
            ++datetime_cur;
        }
        assert(value == cells + num_rows);
    }
    return Status::OK();
}

Status DateTimeV1DataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                       ColumnStorageScratch& scratch) const {
    uint64_t* cells = nullptr;
    const IColumn& nested = bind_repack(column, row_pos, num_rows, scratch, &cells);
    const auto* column_datetime = assert_cast<const ColumnDateTime*>(&nested);

    const VecDateTimeValue* datetime_cur =
            (const VecDateTimeValue*)(column_datetime->get_data().data()) + row_pos;
    const VecDateTimeValue* datetime_end = datetime_cur + num_rows;
    uint64_t* value = cells;
    if (scratch.nullmap != nullptr) {
        const UInt8* nullmap_cur = scratch.nullmap;
        while (datetime_cur != datetime_end) {
            if (!*nullmap_cur) {
                *value = datetime_cur->to_olap_datetime();
            } else {
                // do nothing
            }
            ++value;
            ++datetime_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == scratch.nullmap + num_rows && value == cells + num_rows);
    } else {
        while (datetime_cur != datetime_end) {
            *value = datetime_cur->to_olap_datetime();
            ++value;
            ++datetime_cur;
        }
        assert(value == cells + num_rows);
    }
    return Status::OK();
}

Status DecimalV1DataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                      ColumnStorageScratch& scratch) const {
    decimal12_t* cells = nullptr;
    const IColumn& nested = bind_repack(column, row_pos, num_rows, scratch, &cells);
    const auto* column_decimal = assert_cast<const ColumnDecimal128V2*>(&nested);

    const DecimalV2Value* decimal_cur =
            (const DecimalV2Value*)(column_decimal->get_data().data()) + row_pos;
    const DecimalV2Value* decimal_end = decimal_cur + num_rows;
    decimal12_t* value = cells;
    if (scratch.nullmap != nullptr) {
        const UInt8* nullmap_cur = scratch.nullmap;
        while (decimal_cur != decimal_end) {
            if (!*nullmap_cur) {
                value->integer = decimal_cur->int_value();
                value->fraction = decimal_cur->frac_value();
            } else {
                // do nothing
            }
            ++value;
            ++decimal_cur;
            ++nullmap_cur;
        }
        assert(nullmap_cur == scratch.nullmap + num_rows && value == cells + num_rows);
    } else {
        while (decimal_cur != decimal_end) {
            value->integer = decimal_cur->int_value();
            value->fraction = decimal_cur->frac_value();
            ++value;
            ++decimal_cur;
        }
        assert(value == cells + num_rows);
    }
    return Status::OK();
}

} // namespace doris
