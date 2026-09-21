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

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>
#include <memory>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/exception.h"
#include "common/status.h"
#include "core/assert_cast.h"
#include "core/column/column.h"
#include "core/column/column_complex.h"
#include "core/column/column_decimal.h"
#include "core/column/column_fixed_length_object.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_agg_state.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/decimal12.h"
#include "core/types.h"
#include "core/uint24.h"
#include "core/value/bitmap_value.h"
#include "core/value/decimalv2_value.h"
#include "core/value/hll.h"
#include "core/value/quantile_state.h"
#include "core/value/vdatetime_value.h"
#include "exprs/aggregate/aggregate_function.h"
#include "storage/olap_common.h"
#include "storage/tablet/tablet_schema.h"
#include "util/jsonb_document.h"
#include "util/slice.h"

namespace doris {

ColumnEncoding::ColumnEncoding() = default;
ColumnEncoding::~ColumnEncoding() = default;
ColumnEncoding::ColumnEncoding(ColumnEncoding&&) noexcept = default;
ColumnEncoding& ColumnEncoding::operator=(ColumnEncoding&&) noexcept = default;

Status ColumnEncoding::encode(const IColumn& column, size_t row_pos, size_t num_rows) {
    DCHECK(encoder != nullptr);
    RETURN_IF_ERROR_OR_CATCH_EXCEPTION(encoder->encode(column, row_pos, num_rows, scratch));
    return Status::OK();
}

Status ColumnDataConvertor::encode(const IColumn& column, size_t row_pos, size_t num_rows,
                                   ColumnStorageScratch& scratch) const {
    DCHECK(row_pos + num_rows <= column.size())
            << "row_pos=" << row_pos << ", num_rows=" << num_rows
            << ", column.size()=" << column.size();
    scratch.data = nullptr;
    // Drop the previous batch's cells but keep their allocation: a zero-copy
    // batch must not report stale copies, and the next copying batch reuses
    // the buffer.
    scratch.bytes.clear();
    scratch.slices.clear();
    const IColumn& nested = peel_nullable(column, row_pos, &scratch.nullmap);
    return encode_nested(nested, row_pos, num_rows, scratch);
}

namespace {

// The string-shaped and object encoders hand back one Slice per row.
Slice* slices_for(ColumnStorageScratch& scratch, size_t num_rows) {
    scratch.slices.resize(num_rows);
    scratch.data = reinterpret_cast<const uint8_t*>(scratch.slices.data());
    return scratch.slices.data();
}

// The cell of a NULL row: nothing reads it, but it stays a null pointer rather
// than Slice()'s empty string so a consumer that does look sees no data.
Slice null_slice() {
    return Slice(static_cast<const char*>(nullptr), 0);
}

// The V1 layouts repack every row into a narrower on-disk cell.
template <typename T>
T* cells_for(ColumnStorageScratch& scratch, size_t num_rows) {
    T* cells = scratch.cells<T>(num_rows);
    scratch.data = reinterpret_cast<const uint8_t*>(cells);
    return cells;
}

class BitmapDataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_bitmap = assert_cast<const ColumnBitmap*>(&nested);
        const BitmapValue* bitmap_value = column_bitmap->get_data().data() + row_pos;

        size_t total_size = 0;
        for (size_t i = 0; i < num_rows; ++i) {
            if (!scratch.is_null_at(i)) {
                total_size += bitmap_value[i].getSizeInBytes();
            }
        }
        scratch.bytes.resize(total_size);

        Slice* slices = slices_for(scratch, num_rows);
        char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
        for (size_t i = 0; i < num_rows; ++i) {
            if (scratch.is_null_at(i)) {
                slices[i] = null_slice();
                continue;
            }
            const size_t slice_size = bitmap_value[i].getSizeInBytes();
            bitmap_value[i].write_to(raw_data);
            slices[i] = Slice(raw_data, slice_size);
            raw_data += slice_size;
        }
        DCHECK(raw_data == reinterpret_cast<char*>(scratch.bytes.data()) + total_size);
        return Status::OK();
    }
};

class QuantileStateDataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        if (scratch.nullmap != nullptr) {
            return Status::NotSupported("QuantileState column does not support nullable");
        }
        const auto* column_quantile_state = assert_cast<const ColumnQuantileState*>(&nested);
        const QuantileState* quantile_state = column_quantile_state->get_data().data() + row_pos;
        const QuantileState* quantile_state_end = quantile_state + num_rows;

        size_t total_size = 0;
        for (const QuantileState* cur = quantile_state; cur != quantile_state_end; ++cur) {
            total_size += cur->get_serialized_size();
        }
        scratch.bytes.resize(total_size);

        Slice* slice = slices_for(scratch, num_rows);
        char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
        for (const QuantileState* cur = quantile_state; cur != quantile_state_end; ++cur, ++slice) {
            const size_t slice_size = cur->get_serialized_size();
            cur->serialize((uint8_t*)raw_data);
            *slice = Slice(raw_data, slice_size);
            raw_data += slice_size;
        }
        DCHECK(slice == scratch.slices.get_end_ptr());
        return Status::OK();
    }
};

class HllDataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        if (scratch.nullmap != nullptr) {
            return Status::NotSupported("HLL column does not support nullable");
        }
        const auto* column_hll = assert_cast<const ColumnHLL*>(&nested);
        const HyperLogLog* hll_value = column_hll->get_data().data() + row_pos;
        const HyperLogLog* hll_value_end = hll_value + num_rows;

        size_t total_size = 0;
        for (const HyperLogLog* cur = hll_value; cur != hll_value_end; ++cur) {
            total_size += cur->max_serialized_size();
        }
        scratch.bytes.resize(total_size);

        Slice* slice = slices_for(scratch, num_rows);
        char* raw_data = reinterpret_cast<char*>(scratch.bytes.data());
        for (const HyperLogLog* cur = hll_value; cur != hll_value_end; ++cur, ++slice) {
            const size_t slice_size = cur->serialize((uint8_t*)raw_data);
            *slice = Slice(raw_data, slice_size);
            raw_data += slice_size;
        }
        DCHECK(slice == scratch.slices.get_end_ptr());
        return Status::OK();
    }
};

class CharDataConvertor final : public ColumnDataConvertor {
public:
    explicit CharDataConvertor(size_t length) : _length(length) { DCHECK(length > 0); }

protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_string = assert_cast<const ColumnString*>(&nested);
        Slice* slices = slices_for(scratch, num_rows);

        // A column read back from a segment is already padded to full width, so the
        // slices can point straight into it and nothing is copied.
        if (_is_padded(*column_string)) {
            for (size_t i = 0; i < num_rows; i++) {
                if (!scratch.is_null_at(i)) {
                    slices[i] = column_string->get_data_at(i + row_pos).to_slice();
                    DCHECK(slices[i].size == _length)
                            << "char type data length not equal to schema, schema=" << _length
                            << ", real=" << slices[i].size;
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
            slices[i] = Slice(cell, _length);
            if (scratch.is_null_at(i)) {
                continue;
            }
            const auto str = column_string->get_data_at(i + row_pos);
            DCHECK(str.size <= _length) << "char type data length over limit, schema=" << _length
                                        << ", real=" << str.size;
            if (str.size != 0) {
                memcpy(cell, str.data, str.size);
            }
        }
        return Status::OK();
    }

private:
    // Every row exactly the declared width -- what reading a segment back
    // gives, since CHAR is stored padded.
    bool _is_padded(const ColumnString& column) const {
        return column.size() * _length == column.get_chars().size();
    }

    const size_t _length;
};

class VarcharDataConvertor final : public ColumnDataConvertor {
public:
    VarcharDataConvertor(bool check_length, bool is_jsonb = false)
            : _check_length(check_length), _is_jsonb(is_jsonb) {}

protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_string = assert_cast<const ColumnString*>(&nested);
        const char* char_data = (const char*)(column_string->get_chars().data());
        const ColumnString::Offset* offset_cur = column_string->get_offsets().data() + row_pos;
        const ColumnString::Offset* offset_end = offset_cur + num_rows;

        Slice* slices = slices_for(scratch, num_rows);
        size_t string_offset = *(offset_cur - 1);
        for (size_t i = 0; offset_cur != offset_end; ++i, ++offset_cur) {
            if (scratch.is_null_at(i)) {
                slices[i] = null_slice();
            } else {
                slices[i] = Slice(char_data + string_offset, *offset_cur - string_offset);
                RETURN_IF_ERROR(_check(slices[i]));
            }
            string_offset = *offset_cur;
        }
        return Status::OK();
    }

private:
    Status _check(const Slice& value) const {
        if (UNLIKELY(_check_length && value.size > config::string_type_length_soft_limit_bytes)) {
            return Status::NotSupported(
                    "Not support string len over than `string_type_length_soft_limit_bytes`"
                    " in vec engine.");
        }
        // Make sure that the json binary data written in is the correct jsonb value.
        if (_is_jsonb) {
            const JsonbDocument* doc = nullptr;
            RETURN_IF_ERROR(JsonbDocument::checkAndCreateDocument(value.data, value.size, &doc));
        }
        return Status::OK();
    }

    const bool _check_length;
    const bool _is_jsonb;
};

class AggStateDataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        if (scratch.nullmap != nullptr) {
            return Status::NotSupported("AGG_STATE column does not support nullable");
        }
        const auto* column_fixed_object = assert_cast<const ColumnFixedLengthObject*>(&nested);
        const size_t item_size = column_fixed_object->item_size();
        const auto* cur_values =
                (const uint8_t*)(column_fixed_object->get_data().data()) + item_size * row_pos;

        Slice* slice = slices_for(scratch, num_rows);
        for (size_t i = 0; i < num_rows; ++i, ++slice, cur_values += item_size) {
            *slice = Slice(cur_values, item_size);
        }
        DCHECK(slice == scratch.slices.get_end_ptr());
        return Status::OK();
    }
};

class DateV1DataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_date = assert_cast<const ColumnDate*>(&nested);
        const auto* date_cur = (const VecDateTimeValue*)(column_date->get_data().data()) + row_pos;
        uint24_t* cells = cells_for<uint24_t>(scratch, num_rows);
        for (size_t i = 0; i < num_rows; ++i, ++date_cur) {
            if (!scratch.is_null_at(i)) {
                cells[i] = date_cur->to_olap_date();
            }
        }
        return Status::OK();
    }
};

class DateTimeV1DataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_datetime = assert_cast<const ColumnDateTime*>(&nested);
        const auto* datetime_cur =
                (const VecDateTimeValue*)(column_datetime->get_data().data()) + row_pos;
        uint64_t* cells = cells_for<uint64_t>(scratch, num_rows);
        for (size_t i = 0; i < num_rows; ++i, ++datetime_cur) {
            if (!scratch.is_null_at(i)) {
                cells[i] = datetime_cur->to_olap_datetime();
            }
        }
        return Status::OK();
    }
};

class DecimalV1DataConvertor final : public ColumnDataConvertor {
protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_decimal = assert_cast<const ColumnDecimal128V2*>(&nested);
        const auto* decimal_cur =
                (const DecimalV2Value*)(column_decimal->get_data().data()) + row_pos;
        decimal12_t* cells = cells_for<decimal12_t>(scratch, num_rows);
        for (size_t i = 0; i < num_rows; ++i, ++decimal_cur) {
            if (!scratch.is_null_at(i)) {
                cells[i].integer = decimal_cur->int_value();
                cells[i].fraction = decimal_cur->frac_value();
            }
        }
        return Status::OK();
    }
};

// The types whose compute-layer value already is the storage cell: int, float,
// double, decimalv3, datev2, ...
template <PrimitiveType T>
class PassthroughDataConvertor final : public ColumnDataConvertor {
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
    using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;

protected:
    Status encode_nested(const IColumn& nested, size_t row_pos, size_t num_rows,
                         ColumnStorageScratch& scratch) const override {
        const auto* column_data = assert_cast<const ColumnType*>(&nested);
        const CppType* values = column_data->get_data().data() + row_pos;
        // DATA and ROW_BINLOG flush tasks may encode the same source Block concurrently,
        // so this encoder must never mutate the source column or its nullable nested column.
        // Most simple types can expose the requested source slice directly with zero copy.
        // FLOAT/DOUBLE are special because storage requires a canonical NaN representation:
        // scan only the requested slice and keep the zero-copy path when it contains no NaN;
        // otherwise, normalize a copy in the caller's scratch.
        if constexpr (T == TYPE_FLOAT || T == TYPE_DOUBLE) {
            const CppType* values_end = values + num_rows;
            const CppType* first_nan = std::find_if(
                    values, values_end, [](CppType value) { return std::isnan(value); });
            if (UNLIKELY(first_nan != values_end)) {
                CppType* cells = scratch.cells<CppType>(num_rows);
                std::copy(values, values_end, cells);
                for (CppType* cell = cells + (first_nan - values); cell != cells + num_rows;
                     ++cell) {
                    if (std::isnan(*cell)) {
                        *cell = std::numeric_limits<CppType>::quiet_NaN();
                    }
                }
                values = cells;
            }
        }
        scratch.data = reinterpret_cast<const uint8_t*>(values);
        return Status::OK();
    }
};

} // namespace

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

} // namespace doris
