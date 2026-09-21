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

#include <cstddef>
#include <cstdint>
#include <memory>

#include "common/status.h"
#include "core/pod_array.h"
#include "core/types.h"
#include "util/slice.h"

// What a holder of a column encoder needs: the scratch it encodes into and the
// pair that travels together. The encoder family itself and its factory live
// in olap_data_convertor.h, which only the code that creates encoders includes.

namespace doris {

class IColumn;
class ColumnDataConvertor;
using ColumnDataConvertorUPtr = std::unique_ptr<ColumnDataConvertor>;

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

// One column's encoder together with the buffers it writes into. Every holder
// of an encoder needs both, so they travel as a pair, and every holder encodes
// through encode() below, which is where an encoder's exception (a bad cast, an
// allocation failure) is turned into a Status.
struct ColumnEncoding {
    ColumnDataConvertorUPtr encoder;
    ColumnStorageScratch scratch;

    ColumnEncoding();
    ~ColumnEncoding();
    ColumnEncoding(ColumnEncoding&&) noexcept;
    ColumnEncoding& operator=(ColumnEncoding&&) noexcept;

    Status encode(const IColumn& column, size_t row_pos, size_t num_rows);
};

} // namespace doris
