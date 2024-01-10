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

#include "vec/sink/vtablet_validator.h"


#include <fmt/format.h>
#include <gen_cpp/FrontendService.h>
#include <google/protobuf/stubs/common.h>

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "common/compiler_util.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "service/brpc.h"
#include "util/binary_cast.hpp"
#include "util/brpc_client_cache.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_decimal.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris {
namespace stream_load {

Status OlapTableValidator::validate_data(RuntimeState* state, vectorized::Block* block,
                                      std::vector<char>& filter_bitmap, int* filtered_rows,
                                      bool* stop_processing) {
        for (int i = 0; i < _output_tuple_desc->slots().size(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        block->get_by_position(i).column =
                block->get_by_position(i).column->convert_to_full_column_if_const();
        const auto& column = block->get_by_position(i).column;

        fmt::memory_buffer error_prefix;
        fmt::format_to(error_prefix, "column_name[{}], ", desc->col_name());
        RETURN_IF_ERROR(_validate_column(state, desc->type(), desc->is_nullable(), column, i,
                                         filter_bitmap, stop_processing, error_prefix));
    }

    const auto rows = block->rows();
    auto count = 0;
    for (int i = 0; i < rows; ++i) {
        count += filter_bitmap[i];
    }
    *filtered_rows = count;
    return Status::OK();
}

template <bool is_min>
DecimalV2Value OlapTableValidator::_get_decimalv2_min_or_max(const TypeDescriptor& type) {
    std::map<std::pair<int, int>, DecimalV2Value>* pmap = nullptr;
    if constexpr (is_min) {
        pmap = &_min_decimalv2_val;
    } else {
        pmap = &_max_decimalv2_val;
    }

    // found
    auto iter = pmap->find({type.precision, type.scale});
    if (iter != pmap->end()) {
        return iter->second;
    }

    // save min or max DecimalV2Value for next time
    DecimalV2Value value;
    if constexpr (is_min) {
        value.to_min_decimal(type.precision, type.scale);
    } else {
        value.to_max_decimal(type.precision, type.scale);
    }
    pmap->emplace(std::pair<int, int> {type.precision, type.scale}, value);
    return value;
}

template <typename DecimalType, bool IsMin>
DecimalType OlapTableValidator::_get_decimalv3_min_or_max(const TypeDescriptor& type) {
    std::map<int, typename DecimalType::NativeType>* pmap = nullptr;
    if constexpr (std::is_same_v<DecimalType, vectorized::Decimal32>) {
        pmap = IsMin ? &_min_decimal32_val : &_max_decimal32_val;
    } else if constexpr (std::is_same_v<DecimalType, vectorized::Decimal64>) {
        pmap = IsMin ? &_min_decimal64_val : &_max_decimal64_val;
    } else {
        pmap = IsMin ? &_min_decimal128_val : &_max_decimal128_val;
    }

    // found
    auto iter = pmap->find(type.precision);
    if (iter != pmap->end()) {
        return iter->second;
    }

    typename DecimalType::NativeType value;
    if constexpr (IsMin) {
        value = vectorized::min_decimal_value<DecimalType>(type.precision);
    } else {
        value = vectorized::max_decimal_value<DecimalType>(type.precision);
    }
    pmap->emplace(type.precision, value);
    return value;
}

Status OlapTableValidator::_validate_column(RuntimeState* state, const TypeDescriptor& type,
                                        bool is_nullable, vectorized::ColumnPtr column,
                                        size_t slot_index, std::vector<char>& filter_bitmap,
                                        bool* stop_processing, fmt::memory_buffer& error_prefix,
                                        vectorized::IColumn::Permutation* rows) {
    DCHECK((rows == nullptr) || (rows->size() == column->size()));
    fmt::memory_buffer error_msg;
    auto set_invalid_and_append_error_msg = [&](int row) {
        filter_bitmap[row] = true;
        auto ret = state->append_error_msg_to_file([]() -> std::string { return ""; },
                                                   [&error_prefix, &error_msg]() -> std::string {
                                                       return fmt::to_string(error_prefix) +
                                                              fmt::to_string(error_msg);
                                                   },
                                                   stop_processing);
        error_msg.clear();
        return ret;
    };

    auto column_ptr = vectorized::check_and_get_column<vectorized::ColumnNullable>(*column);
    auto& real_column_ptr = column_ptr == nullptr ? column : (column_ptr->get_nested_column_ptr());
    auto null_map = column_ptr == nullptr ? nullptr : column_ptr->get_null_map_data().data();
    auto need_to_validate = [&null_map, &filter_bitmap](size_t j, size_t row) {
        return !filter_bitmap[row] && (null_map == nullptr || null_map[j] == 0);
    };
    const auto row_count = column->size();
    int invalid_count = 0;

    ssize_t last_invalid_row = -1;
    switch (type.type) {
    case TYPE_CHAR:
    case TYPE_VARCHAR:
    case TYPE_STRING: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());

        size_t limit = config::string_type_length_soft_limit_bytes;
        // when type.len is negative, std::min will return overflow value, so we need to check it
        if (type.len > 0) {
            limit = std::min(config::string_type_length_soft_limit_bytes, type.len);
        }

        auto* __restrict offsets = column_string->get_offsets().data();
        for (int j = 0; j < row_count; ++j) {
            invalid_count += (offsets[j] - offsets[j - 1]) > limit;
        }

        if (invalid_count) {
            for (size_t j = 0; j < row_count; ++j) {
                auto row = rows ? (*rows)[j] : j;
                if (row == last_invalid_row) {
                    continue;
                }
                if (need_to_validate(j, row)) {
                    auto str_val = column_string->get_data_at(j);
                    bool invalid = str_val.size > limit;
                    if (invalid) {
                        last_invalid_row = row;
                        if (str_val.size > type.len) {
                            fmt::format_to(error_msg, "{}",
                                           "the length of input is too long than schema. ");
                            fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                           str_val.to_prefix(32));
                            fmt::format_to(error_msg, "schema length: {}; ", type.len);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        } else if (str_val.size > limit) {
                            fmt::format_to(
                                    error_msg, "{}",
                                    "the length of input string is too long than vec schema. ");
                            fmt::format_to(error_msg, "first 32 bytes of input str: [{}] ",
                                           str_val.to_prefix(32));
                            fmt::format_to(error_msg, "schema length: {}; ", type.len);
                            fmt::format_to(error_msg, "limit length: {}; ", limit);
                            fmt::format_to(error_msg, "actual length: {}; ", str_val.size);
                        }
                        RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                    }
                }
            }
        }
        break;
    }
    case TYPE_JSONB: {
        const auto column_string =
                assert_cast<const vectorized::ColumnString*>(real_column_ptr.get());
        for (size_t j = 0; j < column->size(); ++j) {
            if (!filter_bitmap[j]) {
                if (is_nullable && column_ptr && column_ptr->is_null_at(j)) {
                    continue;
                }
                auto str_val = column_string->get_data_at(j);
                bool invalid = str_val.size == 0;
                if (invalid) {
                    error_msg.clear();
                    fmt::format_to(error_msg, "{}", "jsonb with size 0 is invalid");
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(j));
                }
            }
        }
        break;
    }
    case TYPE_DECIMALV2: {
        auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                assert_cast<const vectorized::ColumnDecimal<vectorized::Decimal128>*>(
                        real_column_ptr.get()));
        const auto& max_decimalv2 = _get_decimalv2_min_or_max<false>(type);
        const auto& min_decimalv2 = _get_decimalv2_min_or_max<true>(type);
        for (size_t j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (need_to_validate(j, row)) {
                auto dec_val = binary_cast<vectorized::Int128, DecimalV2Value>(
                        column_decimal->get_data()[j]);
                bool invalid = false;

                if (dec_val.greater_than_scale(type.scale)) {
                    auto code = dec_val.round(&dec_val, type.scale, HALF_UP);
                    column_decimal->get_data()[j] = dec_val.value();

                    if (code != E_DEC_OK) {
                        fmt::format_to(error_msg, "round one decimal failed.value={}; ",
                                       dec_val.to_string());
                        invalid = true;
                    }
                }
                if (dec_val > max_decimalv2 || dec_val < min_decimalv2) {
                    fmt::format_to(error_msg, "{}", "decimal value is not valid for definition");
                    fmt::format_to(error_msg, ", value={}", dec_val.to_string());
                    fmt::format_to(error_msg, ", precision={}, scale={}", type.precision,
                                   type.scale);
                    fmt::format_to(error_msg, ", min={}, max={}; ", min_decimalv2.to_string(),
                                   max_decimalv2.to_string());
                    invalid = true;
                }

                if (invalid) {
                    last_invalid_row = row;
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
                }
            }
        }
        break;
    }
    case TYPE_DECIMAL32: {
#define CHECK_VALIDATION_FOR_DECIMALV3(ColumnDecimalType, DecimalType)                            \
    auto column_decimal = const_cast<vectorized::ColumnDecimal<vectorized::ColumnDecimalType>*>(  \
            assert_cast<const vectorized::ColumnDecimal<vectorized::ColumnDecimalType>*>(         \
                    real_column_ptr.get()));                                                      \
    const auto& max_decimal = _get_decimalv3_min_or_max<vectorized::DecimalType, false>(type);    \
    const auto& min_decimal = _get_decimalv3_min_or_max<vectorized::DecimalType, true>(type);     \
    const auto* __restrict datas = column_decimal->get_data().data();                             \
    int invalid_count = 0;                                                                        \
    for (int j = 0; j < row_count; ++j) {                                                         \
        const auto dec_val = datas[j];                                                            \
        invalid_count += dec_val > max_decimal || dec_val < min_decimal;                          \
    }                                                                                             \
    if (invalid_count) {                                                                          \
        for (size_t j = 0; j < row_count; ++j) {                                                  \
            auto row = rows ? (*rows)[j] : j;                                                     \
            if (row == last_invalid_row) {                                                        \
                continue;                                                                         \
            }                                                                                     \
            if (need_to_validate(j, row)) {                                                       \
                auto dec_val = column_decimal->get_data()[j];                                     \
                bool invalid = false;                                                             \
                if (dec_val > max_decimal || dec_val < min_decimal) {                             \
                    fmt::format_to(error_msg, "{}", "decimal value is not valid for definition"); \
                    fmt::format_to(error_msg, ", value={}", dec_val);                             \
                    fmt::format_to(error_msg, ", precision={}, scale={}", type.precision,         \
                                   type.scale);                                                   \
                    fmt::format_to(error_msg, ", min={}, max={}; ", min_decimal, max_decimal);    \
                    invalid = true;                                                               \
                }                                                                                 \
                if (invalid) {                                                                    \
                    last_invalid_row = row;                                                       \
                    RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));                       \
                }                                                                                 \
            }                                                                                     \
        }                                                                                         \
    }
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal32, Decimal32);
        break;
    }
    case TYPE_DECIMAL64: {
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal64, Decimal64);
        break;
    }
    case TYPE_DECIMAL128I: {
        CHECK_VALIDATION_FOR_DECIMALV3(Decimal128I, Decimal128I);
        break;
    }
    case TYPE_ARRAY: {
        const auto column_array =
                assert_cast<const vectorized::ColumnArray*>(real_column_ptr.get());
        DCHECK(type.children.size() == 1);
        auto nested_type = type.children[0];
        const auto& offsets = column_array->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "ARRAY type failed: ");
        RETURN_IF_ERROR(_validate_column(state, nested_type, type.contains_nulls[0],
                                         column_array->get_data_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        break;
    }
    case TYPE_MAP: {
        const auto column_map = assert_cast<const vectorized::ColumnMap*>(real_column_ptr.get());
        DCHECK(type.children.size() == 2);
        auto key_type = type.children[0];
        auto val_type = type.children[1];
        const auto& offsets = column_map->get_offsets();
        vectorized::IColumn::Permutation permutation(offsets.back());
        for (size_t r = 0; r < offsets.size(); ++r) {
            for (size_t c = offsets[r - 1]; c < offsets[r]; ++c) {
                permutation[c] = rows ? (*rows)[r] : r;
            }
        }
        fmt::format_to(error_prefix, "MAP type failed: ");
        RETURN_IF_ERROR(_validate_column(state, key_type, type.contains_nulls[0],
                                         column_map->get_keys_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        RETURN_IF_ERROR(_validate_column(state, val_type, type.contains_nulls[1],
                                         column_map->get_values_ptr(), slot_index, filter_bitmap,
                                         stop_processing, error_prefix, &permutation));
        break;
    }
    case TYPE_STRUCT: {
        const auto column_struct =
                assert_cast<const vectorized::ColumnStruct*>(real_column_ptr.get());
        DCHECK(type.children.size() == column_struct->tuple_size());
        fmt::format_to(error_prefix, "STRUCT type failed: ");
        for (size_t sc = 0; sc < column_struct->tuple_size(); ++sc) {
            RETURN_IF_ERROR(_validate_column(state, type.children[sc], type.contains_nulls[sc],
                                             column_struct->get_column_ptr(sc), slot_index,
                                             filter_bitmap, stop_processing, error_prefix));
        }
        break;
    }
    default:
        break;
    }

    // Dispose the column should do not contain the NULL value
    // Only two case:
    // 1. column is nullable but the desc is not nullable
    // 2. desc->type is BITMAP
    if ((!is_nullable || type == TYPE_OBJECT) && column_ptr) {
        for (int j = 0; j < column->size(); ++j) {
            auto row = rows ? (*rows)[j] : j;
            if (row == last_invalid_row) {
                continue;
            }
            if (null_map[j] && !filter_bitmap[row]) {
                fmt::format_to(error_msg, "null value for not null column, type={}",
                               type.debug_string());
                last_invalid_row = row;
                RETURN_IF_ERROR(set_invalid_and_append_error_msg(row));
            }
        }
    }

    return Status::OK();
}

void OlapTableValidator::convert_to_dest_desc_block(doris::vectorized::Block* block) {
    for (int i = 0; i < _output_tuple_desc->slots().size() && i < block->columns(); ++i) {
        SlotDescriptor* desc = _output_tuple_desc->slots()[i];
        if (desc->is_nullable() != block->get_by_position(i).type->is_nullable()) {
            if (desc->is_nullable()) {
                block->get_by_position(i).type =
                        vectorized::make_nullable(block->get_by_position(i).type);
                block->get_by_position(i).column =
                        vectorized::make_nullable(block->get_by_position(i).column);
            } else {
                block->get_by_position(i).type = assert_cast<const vectorized::DataTypeNullable&>(
                                                         *block->get_by_position(i).type)
                                                         .get_nested_type();
                block->get_by_position(i).column = assert_cast<const vectorized::ColumnNullable&>(
                                                           *block->get_by_position(i).column)
                                                           .get_nested_column_ptr();
            }
        }
    }
}

} // namespace vectorized
} // namespace doris