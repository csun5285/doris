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

#include "storage/predicate/like_column_predicate.h"

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <string_view>

#include "exprs/function_context.h"
#include "storage/index/bloom_filter/bloom_filter.h"

namespace doris {
namespace {

TEST(LikeColumnPredicateTest, ClonePreservesPageNgramBloomFilter) {
    constexpr size_t bloom_filter_size = 512;

    std::unique_ptr<segment_v2::BloomFilter> page_bloom_filter;
    ASSERT_TRUE(segment_v2::BloomFilter::create(segment_v2::NGRAM_BLOOM_FILTER, &page_bloom_filter,
                                                bloom_filter_size)
                        .ok());

    std::unique_ptr<segment_v2::BloomFilter> query_bloom_filter;
    ASSERT_TRUE(segment_v2::BloomFilter::create(segment_v2::NGRAM_BLOOM_FILTER, &query_bloom_filter,
                                                bloom_filter_size)
                        .ok());
    constexpr std::string_view query_token = "missing";
    query_bloom_filter->add_bytes(query_token.data(), query_token.size());
    ASSERT_FALSE(page_bloom_filter->contains(*query_bloom_filter));

    auto like_state = std::make_shared<LikeState>();
    auto function_context = FunctionContext::create_context(nullptr, nullptr, {});
    function_context->set_function_state(FunctionContext::THREAD_LOCAL, like_state);

    std::string pattern = "%missing%";
    auto predicate = LikeColumnPredicate::create_shared(false, 1, "c1", function_context.get(),
                                                        StringRef(pattern));
    predicate->set_page_ng_bf(std::move(query_bloom_filter));
    ASSERT_FALSE(predicate->evaluate_and(page_bloom_filter.get()));

    auto cloned_predicate = predicate->clone(7);
    EXPECT_EQ(cloned_predicate->column_id(), 7);
    EXPECT_FALSE(cloned_predicate->evaluate_and(page_bloom_filter.get()));
}

} // namespace
} // namespace doris
