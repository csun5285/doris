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

#include <gtest/gtest.h>

#include <barrier>
#include <bit>
#include <cstdint>
#include <limits>
#include <memory>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>

#include "core/block/column_with_type_and_name.h"
#include "core/column/column_array.h"
#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {
namespace {

template <PrimitiveType T>
using CppType = typename PrimitiveTypeTraits<T>::CppType;

template <PrimitiveType T>
struct NumericSource {
    ColumnWithTypeAndName typed_column;
    const CppType<T>* values;
    const IColumn* nested_column;
};

template <PrimitiveType T>
NumericSource<T> create_numeric_source(std::vector<CppType<T>> values,
                                       std::vector<UInt8> null_map = {}) {
    auto nested_column = PrimitiveTypeTraits<T>::ColumnType::create();
    nested_column->get_data().assign(values.begin(), values.end());
    const CppType<T>* source_values = nested_column->get_data().data();
    const IColumn* source_nested_column = nested_column.get();

    DataTypePtr data_type = std::make_shared<typename PrimitiveTypeTraits<T>::DataType>();
    ColumnPtr column;
    if (null_map.empty()) {
        column = std::move(nested_column);
    } else {
        auto null_map_column = ColumnUInt8::create();
        null_map_column->get_data().assign(null_map.begin(), null_map.end());
        column = ColumnNullable::create(std::move(nested_column), std::move(null_map_column));
        data_type = std::make_shared<DataTypeNullable>(data_type);
    }
    return {{std::move(column), std::move(data_type), "value"},
            source_values,
            source_nested_column};
}

TabletColumn create_tablet_column(FieldType type, bool nullable) {
    TabletColumn column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE, type, nullable);
    column.set_unique_id(0);
    column.set_name("value");
    const int32_t length =
            type == FieldType::OLAP_FIELD_TYPE_FLOAT ? sizeof(float) : sizeof(int64_t);
    column.set_length(length);
    column.set_index_length(length);
    return column;
}

template <PrimitiveType T>
FieldType field_type() {
    if constexpr (T == TYPE_BIGINT) {
        return FieldType::OLAP_FIELD_TYPE_BIGINT;
    } else if constexpr (T == TYPE_FLOAT) {
        return FieldType::OLAP_FIELD_TYPE_FLOAT;
    } else {
        static_assert(T == TYPE_DOUBLE);
        return FieldType::OLAP_FIELD_TYPE_DOUBLE;
    }
}

// The pair a column writer holds, typed for one numeric source.
template <PrimitiveType T>
struct TestEncoder : ColumnEncoding {
    explicit TestEncoder(bool nullable) {
        encoder = create_column_data_convertor(create_tablet_column(field_type<T>(), nullable));
    }

    Status encode(const NumericSource<T>& source, size_t row_pos, size_t num_rows) {
        return ColumnEncoding::encode(*source.typed_column.column, row_pos, num_rows);
    }

    const CppType<T>* data() const { return reinterpret_cast<const CppType<T>*>(scratch.data); }

    // What get_data_at() used to hand back: the row's cell, or null for a NULL row.
    const CppType<T>* cell_at(size_t offset) const {
        if (scratch.nullmap != nullptr && scratch.nullmap[offset] != 0) {
            return nullptr;
        }
        return data() + offset;
    }

    // How many values the encoder had to copy into the scratch instead of
    // pointing straight at the source.
    size_t copied_values() const { return scratch.bytes.size() / sizeof(CppType<T>); }
};

template <typename T>
using UIntOfSize = std::conditional_t<sizeof(T) == sizeof(uint32_t), uint32_t, uint64_t>;

template <typename T>
T from_bits(UIntOfSize<T> bits) {
    return std::bit_cast<T>(bits);
}

template <typename T>
UIntOfSize<T> bits_of(T value) {
    return std::bit_cast<UIntOfSize<T>>(value);
}

template <PrimitiveType T>
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
void expect_float_without_nan_is_zero_copy(bool nullable) {
    using Value = CppType<T>;
    const std::vector<Value> values = {Value(1.25), Value(-2.5), Value(0), Value(9.75)};
    auto source = create_numeric_source<T>(
            values, nullable ? std::vector<UInt8> {0, 1, 0, 0} : std::vector<UInt8> {});
    TestEncoder<T> encoder(nullable);

    ASSERT_TRUE(encoder.encode(source, 1, 2).ok());
    EXPECT_EQ(source.values + 1, encoder.data());
    EXPECT_EQ(Value(-2.5), encoder.data()[0]);
    EXPECT_EQ(Value(0), encoder.data()[1]);
    if (nullable) {
        ASSERT_NE(nullptr, encoder.scratch.nullmap);
        EXPECT_EQ(1, encoder.scratch.nullmap[0]);
        EXPECT_EQ(nullptr, encoder.cell_at(0));
        EXPECT_EQ(source.values + 2, encoder.cell_at(1));
    } else {
        EXPECT_EQ(nullptr, encoder.scratch.nullmap);
    }
    EXPECT_EQ(0, encoder.scratch.bytes.allocated_bytes());
    EXPECT_EQ(values, std::vector<Value>(source.values, source.values + values.size()));
}

template <PrimitiveType T>
// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
void expect_nan_payloads_are_normalized_without_source_mutation() {
    using Value = CppType<T>;
    using UInt = UIntOfSize<Value>;
    const UInt nan_bits_1 =
            sizeof(Value) == sizeof(float) ? UInt(0x7fc00011U) : UInt(0x7ff8000000000011ULL);
    const UInt nan_bits_2 =
            sizeof(Value) == sizeof(float) ? UInt(0x7fa12345U) : UInt(0x7ff123456789abcdULL);
    const UInt nan_bits_3 =
            sizeof(Value) == sizeof(float) ? UInt(0xffc54321U) : UInt(0xfff8123456789abcULL);
    const std::vector<Value> values = {Value(1.5), from_bits<Value>(nan_bits_1),
                                       from_bits<Value>(nan_bits_2), Value(-8.25),
                                       from_bits<Value>(nan_bits_3)};
    std::vector<UInt> source_bits;
    for (Value value : values) {
        source_bits.push_back(bits_of(value));
    }

    auto source = create_numeric_source<T>(values, {0, 0, 1, 0, 0});
    TestEncoder<T> encoder(true);

    ASSERT_TRUE(encoder.encode(source, 0, values.size()).ok());
    ASSERT_NE(source.values, encoder.data());
    const auto* converted_values = encoder.data();
    const UInt quiet_nan_bits = bits_of(std::numeric_limits<Value>::quiet_NaN());
    EXPECT_EQ(bits_of(Value(1.5)), bits_of(converted_values[0]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[1]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[2]));
    EXPECT_EQ(bits_of(Value(-8.25)), bits_of(converted_values[3]));
    EXPECT_EQ(quiet_nan_bits, bits_of(converted_values[4]));
    EXPECT_EQ(nullptr, encoder.cell_at(2));

    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
    EXPECT_EQ(values.size(), encoder.copied_values());
}

} // namespace

TEST(ColumnDataConvertorTest, ConcurrentNullableBigIntSourceIsReadOnly) {
    const std::vector<int64_t> values = {10, 20, 30, 40};
    auto source = create_numeric_source<TYPE_BIGINT>(values, {0, 1, 0, 0});
    ASSERT_EQ(1, source.nested_column->use_count());

    const TabletColumn tablet_column =
            create_tablet_column(FieldType::OLAP_FIELD_TYPE_BIGINT, true);
    // The encoder is immutable, so one serves both threads; each brings its own
    // scratch. Neither may touch the source column.
    const auto encoder = create_column_data_convertor(tablet_column);
    ColumnStorageScratch data_scratch;
    ColumnStorageScratch row_binlog_scratch;
    std::barrier start(2);
    Status data_status;
    Status row_binlog_status;
    std::thread data_thread([&] {
        start.arrive_and_wait();
        data_status = encoder->encode(*source.typed_column.column, 0, values.size(), data_scratch);
    });
    std::thread row_binlog_thread([&] {
        start.arrive_and_wait();
        row_binlog_status =
                encoder->encode(*source.typed_column.column, 0, values.size(), row_binlog_scratch);
    });
    data_thread.join();
    row_binlog_thread.join();

    ASSERT_TRUE(data_status.ok()) << data_status;
    ASSERT_TRUE(row_binlog_status.ok()) << row_binlog_status;
    EXPECT_EQ(reinterpret_cast<const uint8_t*>(source.values), data_scratch.data);
    EXPECT_EQ(reinterpret_cast<const uint8_t*>(source.values), row_binlog_scratch.data);
    EXPECT_EQ(1, source.nested_column->use_count());
    EXPECT_EQ(values, std::vector<int64_t>(source.values, source.values + values.size()));
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(ColumnDataConvertorTest, BigIntNullableAndNonNullableAreZeroCopy) {
    for (bool nullable : {false, true}) {
        const std::vector<int64_t> values = {11, 22, 33, 44};
        auto source = create_numeric_source<TYPE_BIGINT>(
                values, nullable ? std::vector<UInt8> {0, 1, 0, 0} : std::vector<UInt8> {});
        TestEncoder<TYPE_BIGINT> encoder(nullable);

        ASSERT_TRUE(encoder.encode(source, 1, 2).ok());
        const auto source_use_count = source.nested_column->use_count();
        EXPECT_EQ(source.values + 1, encoder.data());
        EXPECT_EQ(values, std::vector<int64_t>(source.values, source.values + values.size()));
        EXPECT_EQ(source_use_count, source.nested_column->use_count());
        if (nullable) {
            EXPECT_EQ(nullptr, encoder.cell_at(0));
            EXPECT_EQ(source.values + 2, encoder.cell_at(1));
        } else {
            EXPECT_EQ(source.values + 1, encoder.cell_at(0));
        }
    }
}

TEST(ColumnDataConvertorTest, FloatAndDoubleWithoutNanAreZeroCopy) {
    expect_float_without_nan_is_zero_copy<TYPE_FLOAT>(false);
    expect_float_without_nan_is_zero_copy<TYPE_FLOAT>(true);
    expect_float_without_nan_is_zero_copy<TYPE_DOUBLE>(false);
    expect_float_without_nan_is_zero_copy<TYPE_DOUBLE>(true);
}

TEST(ColumnDataConvertorTest, FloatAndDoubleNanPayloadsDoNotMutateSource) {
    expect_nan_payloads_are_normalized_without_source_mutation<TYPE_FLOAT>();
    expect_nan_payloads_are_normalized_without_source_mutation<TYPE_DOUBLE>();
}

TEST(ColumnDataConvertorTest, OnlyCurrentFloatSliceIsInspectedAndCopied) {
    const auto nan_before = from_bits<float>(0x7fc00011U);
    const auto nan_after = from_bits<float>(0x7fa12345U);
    const std::vector<float> values = {nan_before, 1.0F, 2.0F, nan_after};
    const std::vector<uint32_t> source_bits = {bits_of(nan_before), bits_of(1.0F), bits_of(2.0F),
                                               bits_of(nan_after)};
    auto source = create_numeric_source<TYPE_FLOAT>(values);
    TestEncoder<TYPE_FLOAT> encoder(false);

    ASSERT_TRUE(encoder.encode(source, 1, 2).ok());
    EXPECT_EQ(source.values + 1, encoder.data());
    EXPECT_EQ(0, encoder.scratch.bytes.allocated_bytes());

    ASSERT_TRUE(encoder.encode(source, 3, 1).ok());
    ASSERT_NE(source.values + 3, encoder.data());
    EXPECT_EQ(1, encoder.copied_values());
    EXPECT_EQ(bits_of(std::numeric_limits<float>::quiet_NaN()), bits_of(encoder.data()[0]));
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
}

TEST(ColumnDataConvertorTest, ReusesFloatingPointBufferAcrossNanAndNonNanBatches) {
    const auto nan_1 = from_bits<double>(0x7ff8000000000011ULL);
    const auto nan_2 = from_bits<double>(0x7ff123456789abcdULL);
    const std::vector<double> values = {nan_1, 1.0, 2.0, 3.0, nan_2, 4.0};
    std::vector<uint64_t> source_bits;
    for (double value : values) {
        source_bits.push_back(bits_of(value));
    }
    auto source = create_numeric_source<TYPE_DOUBLE>(values);
    TestEncoder<TYPE_DOUBLE> encoder(false);

    ASSERT_TRUE(encoder.encode(source, 0, 2).ok());
    const auto* first_buffer = encoder.data();
    ASSERT_NE(source.values, first_buffer);
    const size_t first_capacity = encoder.scratch.bytes.capacity();

    ASSERT_TRUE(encoder.encode(source, 2, 2).ok());
    EXPECT_EQ(source.values + 2, encoder.data());
    EXPECT_EQ(0, encoder.copied_values());
    EXPECT_EQ(first_capacity, encoder.scratch.bytes.capacity());

    ASSERT_TRUE(encoder.encode(source, 4, 2).ok());
    EXPECT_EQ(first_buffer, encoder.data());
    EXPECT_EQ(first_capacity, encoder.scratch.bytes.capacity());
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source.values[i]));
    }
}

// NOLINTNEXTLINE(readability-function-cognitive-complexity) -- GTest assertions inflate it.
TEST(ColumnDataConvertorTest, ArrayWithNullableDoubleLeafDoesNotMutateSource) {
    const auto nan_1 = from_bits<double>(0x7ff8000000000011ULL);
    const auto nan_2 = from_bits<double>(0xfff8123456789abcULL);
    const std::vector<double> values = {nan_1, 7.5, nan_2};
    auto nested_values = ColumnFloat64::create();
    nested_values->get_data().assign(values.begin(), values.end());
    const double* source_values = nested_values->get_data().data();
    const std::vector<uint64_t> source_bits = {bits_of(source_values[0]), bits_of(source_values[1]),
                                               bits_of(source_values[2])};
    const std::vector<UInt8> null_map = {0, 1, 0};
    auto nested_null_map = ColumnUInt8::create();
    nested_null_map->get_data().assign(null_map.begin(), null_map.end());
    auto nullable_values =
            ColumnNullable::create(std::move(nested_values), std::move(nested_null_map));
    const std::vector<UInt64> array_offsets = {2, 3};
    auto offsets = ColumnOffset64::create();
    offsets->get_data().assign(array_offsets.begin(), array_offsets.end());
    ColumnPtr array_column = ColumnArray::create(std::move(nullable_values), std::move(offsets));
    const auto& col_array = assert_cast<const ColumnArray&>(*array_column);

    TabletColumn item_column(FieldAggregationMethod::OLAP_FIELD_AGGREGATION_NONE,
                             FieldType::OLAP_FIELD_TYPE_DOUBLE, true);
    item_column.set_name("item");
    item_column.set_length(sizeof(double));
    item_column.set_index_length(sizeof(double));

    // An array is just a container: ArrayColumnWriter writes the offsets itself
    // and hands the item slice to the item writer, so the leaf is what goes
    // through an encoder. Array rows [0, 2) cover items [0, 3).
    ColumnWithTypeAndName typed_items {
            col_array.get_data_ptr(),
            std::make_shared<DataTypeNullable>(std::make_shared<DataTypeFloat64>()), "item"};

    const auto item_encoder = create_column_data_convertor(item_column);
    ColumnStorageScratch scratch;
    ASSERT_TRUE(item_encoder->encode(*typed_items.column, 0, 3, scratch).ok());
    const auto* converted_values = reinterpret_cast<const double*>(scratch.data);
    const auto* converted_null_map = scratch.nullmap;
    ASSERT_NE(source_values, converted_values);
    EXPECT_EQ(bits_of(std::numeric_limits<double>::quiet_NaN()), bits_of(converted_values[0]));
    EXPECT_EQ(bits_of(7.5), bits_of(converted_values[1]));
    EXPECT_EQ(bits_of(std::numeric_limits<double>::quiet_NaN()), bits_of(converted_values[2]));
    ASSERT_NE(nullptr, converted_null_map);
    EXPECT_EQ(0, converted_null_map[0]);
    EXPECT_EQ(1, converted_null_map[1]);
    EXPECT_EQ(0, converted_null_map[2]);
    for (size_t i = 0; i < source_bits.size(); ++i) {
        EXPECT_EQ(source_bits[i], bits_of(source_values[i]));
    }
}

} // namespace doris
