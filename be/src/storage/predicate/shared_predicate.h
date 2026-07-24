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

#include <cstdint>
#include <memory>

#include "common/factory_creator.h"
#include "core/column/column_dictionary.h"
#include "storage/index/bloom_filter/bloom_filter.h"
#include "storage/index/inverted/inverted_index_reader.h"
#include "storage/predicate/column_predicate.h"

namespace doris {

// SharedPredicate only used on topn runtime predicate.
// Runtime predicate clones share one mutable predicate state, so updates are real-time while
// each storage reader can bind the predicate to its own dense column position.
// At the beginning nested predicate may be nullptr, in which case predicate always returns true.
class SharedPredicate final : public ColumnPredicate {
    ENABLE_FACTORY_CREATOR(SharedPredicate);

public:
    SharedPredicate(uint32_t column_id, std::string col_name)
            : ColumnPredicate(column_id, col_name, PrimitiveType::INVALID_TYPE),
              _state(std::make_shared<SharedState>()) {}
    SharedPredicate(const ColumnPredicate& other) = delete;
    SharedPredicate(const SharedPredicate& other, uint32_t column_id)
            : ColumnPredicate(other, column_id), _state(other._state) {}
    ~SharedPredicate() override = default;
    std::string debug_string() const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        fmt::memory_buffer debug_string_buffer;
        fmt::format_to(debug_string_buffer, "SharedPredicate({}, nested={})",
                       ColumnPredicate::debug_string(),
                       _state->nested ? _state->nested->debug_string() : "null");
        return fmt::to_string(debug_string_buffer);
    }
    std::shared_ptr<ColumnPredicate> clone(uint32_t column_id) const override {
        return SharedPredicate::create_shared(*this, column_id);
    }

    PredicateType type() const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            // topn filter is le or ge
            return PredicateType::LE;
        }
        return _state->nested->type();
    }
    PrimitiveType primitive_type() const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return PrimitiveType::INVALID_TYPE;
        }
        return _state->nested->primitive_type();
    }

    void set_nested(const std::shared_ptr<ColumnPredicate>& nested) {
        std::unique_lock<std::shared_mutex> lock(_state->mtx);
        _state->nested = nested;
    }

    Status evaluate(const IndexFieldNameAndTypePair& name_with_type, IndexIterator* iterator,
                    uint32_t num_rows, roaring::Roaring* bitmap) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return Status::OK();
        }
        return _state->nested->evaluate(name_with_type, iterator, num_rows, bitmap);
    }

    void evaluate_and(const IColumn& column, const uint16_t* sel, uint16_t size,
                      bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return;
        }
        return _state->nested->evaluate_and(column, sel, size, flags);
    }

    void evaluate_or(const IColumn& column, const uint16_t* sel, uint16_t size,
                     bool* flags) const override {
        DCHECK(false) << "should not reach here";
    }

    bool evaluate_and(const segment_v2::ZoneMap& zone_map) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return ColumnPredicate::evaluate_and(zone_map);
        }
        return _state->nested->evaluate_and(zone_map);
    }

    bool evaluate_del(const segment_v2::ZoneMap& zone_map) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return ColumnPredicate::evaluate_del(zone_map);
        }
        return _state->nested->evaluate_del(zone_map);
    }

    bool evaluate_and(const BloomFilter* bf) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return ColumnPredicate::evaluate_and(bf);
        }
        return _state->nested->evaluate_and(bf);
    }

    bool can_do_bloom_filter(bool ngram) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return ColumnPredicate::can_do_bloom_filter(ngram);
        }
        return _state->nested->can_do_bloom_filter(ngram);
    }

    void evaluate_vec(const IColumn& column, uint16_t size, bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            for (uint16_t i = 0; i < size; ++i) {
                flags[i] = true;
            }
            return;
        }
        _state->nested->evaluate_vec(column, size, flags);
    }

    void evaluate_and_vec(const IColumn& column, uint16_t size, bool* flags) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return;
        }
        _state->nested->evaluate_and_vec(column, size, flags);
    }

    std::string get_search_str() const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            DCHECK(false) << "should not reach here";
        }
        return _state->nested->get_search_str();
    }

    bool evaluate_and(ParquetPredicate::ColumnStat* statistic) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            // at the begining _nested will be null, so return true.
            return true;
        }
        return _state->nested->evaluate_and(statistic);
    }

    bool evaluate_and(ParquetPredicate::CachedPageIndexStat* statistic,
                      RowRanges* row_ranges) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);

        if (!_state->nested) {
            // at the begining _nested will be null, so return true.
            row_ranges->add(statistic->row_group_range);
            return true;
        }
        return _state->nested->evaluate_and(statistic, row_ranges);
    }

private:
    uint16_t _evaluate_inner(const IColumn& column, uint16_t* sel, uint16_t size) const override {
        std::shared_lock<std::shared_mutex> lock(_state->mtx);
        if (!_state->nested) {
            return size;
        }
        return _state->nested->evaluate(column, sel, size);
    }

    struct SharedState {
        mutable std::shared_mutex mtx;
        std::shared_ptr<ColumnPredicate> nested;
    };

    std::shared_ptr<SharedState> _state;
};

} //namespace doris
