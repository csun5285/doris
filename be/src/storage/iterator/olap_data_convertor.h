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

#include <assert.h>
#include <glog/logging.h>
#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <cmath>
#include <limits>
#include <memory>
#include <ostream>
#include <utility>
#include <vector>

#include "common/status.h"
#include "core/assert_cast.h"
#include "core/block/column_with_type_and_name.h"
#include "core/column/column.h"
#include "core/column/column_nullable.h"
#include "core/column/column_string.h"
#include "core/column/column_vector.h"
#include "core/data_type/data_type.h"
#include "core/data_type/data_type_array.h"
#include "core/data_type/data_type_factory.hpp"
#include "core/data_type/data_type_map.h"
#include "core/data_type/data_type_variant.h"
#include "core/decimal12.h"
#include "core/pod_array_fwd.h"
#include "core/string_ref.h"
#include "core/types.h"
#include "core/uint24.h"
#include "util/slice.h"

namespace doris {

class TabletSchema;
class TabletColumn;

class Block;
class ColumnArray;
class ColumnMap;
class DataTypeMap;
template <PrimitiveType T>
class ColumnDecimal;

// One batch of storage-format bytes plus everything they are made of. The
// caller owns one of these per column it encodes and reuses it across batches;
// that is what lets the encoders themselves stay immutable. Only the encoders
// write the members.
//
// The result borrows from the source column -- `nullmap` always, and `data` too
// for the types that need no conversion -- so it is readable only while the
// caller still holds the column it encoded, and only until the next encode into
// this scratch.
struct ColumnStorageScratch {
    // num_rows storage-format cells laid out back to back. One cell is
    // `CppTypeTraits<the column's FieldType>::CppType` -- uint24_t for a v1
    // DATE, decimal12_t for a DECIMALV2, Slice for anything string-shaped --
    // and the stride is field_type_size() of that same FieldType. It stays a
    // byte pointer because the FieldType is only known at runtime; the page
    // builders and KeyCoders reinterpret it back on their side. Every encode
    // that returns OK sets it.
    const uint8_t* data = nullptr;
    // One byte per row, already offset to the first encoded row. Null means the
    // source column is not nullable, so this batch has no null bits at all.
    const UInt8* nullmap = nullptr;

    // One {pointer, length} cell per row, for the string-shaped and object types.
    PaddedPODArray<Slice> slices;
    // The bytes `data` or `slices` point into: the repacked fixed-width values,
    // CHAR's padded cells, or the serialized object bytes. One encoder only
    // ever puts one of those here.
    PaddedPODArray<uint8_t> bytes;

    // Whether the row at `offset` within the encoded batch is null.
    bool is_null_at(size_t offset) const { return nullmap != nullptr && nullmap[offset] != 0; }

    // `bytes` read back as T cells; PaddedPODArray over-aligns its buffer.
    template <typename T>
    T* cells(size_t num_rows) {
        bytes.resize(num_rows * sizeof(T));
        return reinterpret_cast<T*>(bytes.data());
    }
};

class ColumnDataConvertor;
using ColumnDataConvertorUPtr = std::unique_ptr<ColumnDataConvertor>;
using ColumnDataConvertorSPtr = std::shared_ptr<ColumnDataConvertor>;

// Build the encoder for one column. Every holder that needs storage-format
// bytes owns its own encoder: the column writer, IndexBuilder, and the block
// transform stages that encode keys before any writer exists.
ColumnDataConvertorUPtr create_column_data_convertor(const TabletColumn& column);
ColumnDataConvertorUPtr create_agg_state_data_convertor(const TabletColumn& column);

// Turns one column from its compute-layer layout, where a value is a
// `PrimitiveTypeTraits<PrimitiveType>::CppType`, into an array of storage cells,
// where a value is a `CppTypeTraits<FieldType>::CppType`. The two coincide for
// most types -- those encode with zero copies, handing back a pointer into the
// source column -- and the ones that differ are what the subclasses are for.
//
// Immutable: everything an encode() needs arrives as an argument, and
// everything it produces goes into the caller's scratch and the returned view,
// so one encoder can serve any number of columns of its type at once.
//
// The view stays valid until the next encode() into the same scratch.
class ColumnDataConvertor {
public:
    ColumnDataConvertor() = default;
    virtual ~ColumnDataConvertor() = default;
    ColumnDataConvertor(const ColumnDataConvertor&) = delete;
    ColumnDataConvertor& operator=(const ColumnDataConvertor&) = delete;
    ColumnDataConvertor(ColumnDataConvertor&&) = delete;
    ColumnDataConvertor& operator=(ColumnDataConvertor&&) = delete;

    // Encode rows [row_pos, row_pos + num_rows) into storage format.
    virtual Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                          ColumnStorageScratch& scratch) const = 0;

protected:
    // Every encode() starts here: clear the last result, and split a Nullable
    // column into the null bits (offset to row_pos, left in the scratch) and
    // the nested column, which is returned for the caller to encode.
    static const IColumn& bind(const IColumn& column, size_t row_pos, size_t num_rows,
                               ColumnStorageScratch& scratch);
};

class ObjectDataConvertor : public ColumnDataConvertor {
protected:
    // Serialized bytes go into scratch.bytes and scratch.slices points into
    // them, so both are reset together at the start of every encode().
    static const IColumn& bind_object(const IColumn& column, size_t row_pos, size_t num_rows,
                                      ColumnStorageScratch& scratch) {
        const IColumn& nested = bind(column, row_pos, num_rows, scratch);
        scratch.bytes.clear();
        scratch.slices.resize(num_rows);
        scratch.data = reinterpret_cast<const uint8_t*>(scratch.slices.data());
        return nested;
    }
};

class HllDataConvertor final : public ObjectDataConvertor {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

class BitmapDataConvertor final : public ObjectDataConvertor {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

class QuantileStateDataConvertor final : public ObjectDataConvertor {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

class CharDataConvertor : public ColumnDataConvertor {
public:
    CharDataConvertor(size_t length);
    ~CharDataConvertor() override = default;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;

private:
    static bool should_padding(const ColumnString* column, size_t padding_length) {
        // Check sum of data length, including terminating zero.
        return column->size() * padding_length != column->get_chars().size();
    }

    const size_t _length;
};

class VarcharDataConvertor : public ColumnDataConvertor {
public:
    VarcharDataConvertor(bool check_length, bool is_jsonb = false);
    ~VarcharDataConvertor() override = default;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;

private:
    // `null_map` is already offset to the encoded slice, as bind() produces it.
    Status encode_string(const UInt8* null_map, const ColumnString* column_string, size_t row_pos,
                         size_t num_rows, ColumnStorageScratch& scratch) const;

    const bool _check_length;
    // Make sure that the json binary data written in is the correct jsonb value.
    const bool _is_jsonb = false;
};

class AggStateDataConvertor : public ColumnDataConvertor {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

// Base for the V1 layouts that repack every row into a narrower on-disk cell.
template <typename T>
class RepackDataConvertor : public ColumnDataConvertor {
protected:
    static const IColumn& bind_repack(const IColumn& column, size_t row_pos, size_t num_rows,
                                      ColumnStorageScratch& scratch, T** cells) {
        const IColumn& nested = bind(column, row_pos, num_rows, scratch);
        *cells = scratch.template cells<T>(num_rows);
        scratch.data = reinterpret_cast<const uint8_t*>(*cells);
        return nested;
    }
};

class DateV1DataConvertor : public RepackDataConvertor<uint24_t> {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

class DateTimeV1DataConvertor : public RepackDataConvertor<uint64_t> {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

class DecimalV1DataConvertor : public RepackDataConvertor<decimal12_t> {
public:
    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override;
};

// class PassthroughDataConvertor for simple types, which don't need to do any convert, like int, float, double, etc...
template <PrimitiveType T>
class PassthroughDataConvertor : public ColumnDataConvertor {
public:
    PassthroughDataConvertor() = default;
    ~PassthroughDataConvertor() override = default;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override {
        using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;

        const IColumn& nested = bind(column, row_pos, num_rows, scratch);
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
                CppType* cells = scratch.template cells<CppType>(num_rows);
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

protected:
    using CppType = typename PrimitiveTypeTraits<T>::CppType;
};

// decimalv3 don't need to do any convert
template <PrimitiveType T>
class DecimalV3DataConvertor : public PassthroughDataConvertor<T> {
public:
    DecimalV3DataConvertor() = default;
    ~DecimalV3DataConvertor() override = default;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows,
                  ColumnStorageScratch& scratch) const override {
        using ColumnType = typename PrimitiveTypeTraits<T>::ColumnType;
        const IColumn& nested = ColumnDataConvertor::bind(column, row_pos, num_rows, scratch);
        const auto* column_data = assert_cast<const ColumnType*>(&nested);
        scratch.data = reinterpret_cast<const uint8_t*>(column_data->get_data().data() + row_pos);
        return Status::OK();
    }
};

// One column's encoder together with the buffers it writes into. Every holder
// of an encoder needs both, so they travel as a pair.
struct ColumnEncoding {
    ColumnDataConvertorUPtr encoder;
    ColumnStorageScratch scratch;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows) {
        DCHECK(encoder != nullptr);
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(encoder->encode(column, row_pos, num_rows, scratch));
        return Status::OK();
    }
};

} // namespace doris
