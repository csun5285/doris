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

#include <mutex>

#include "common/status.h"
#include "storage/segment/row_ranges.h"

namespace doris::segment_v2 {

// Row ranges of a whole segment that are computed once and then reused by every
// SegmentIterator reading a part of that segment.
class SharedRowRanges {
public:
    // Copies the ranges into `ranges`, computing them with `compute(RowRanges*)` on the first
    // call. Later callers wait for that call and get its result, including its error.
    template <typename Compute>
    Status get_or_compute(Compute&& compute, RowRanges* ranges) {
        std::lock_guard lock(_mutex);
        if (!_computed) {
            _status = compute(&_ranges);
            _computed = true;
        }
        RETURN_IF_ERROR(_status);
        *ranges = _ranges;
        return Status::OK();
    }

private:
    std::mutex _mutex;
    bool _computed = false;
    Status _status;
    RowRanges _ranges;
};

// The index results of one segment, shared by the scanners that each read a part of it, so
// splitting a segment among scanners does not repeat its index lookups. Every result covers the
// whole segment; each reader then intersects it with its own row ranges.
//
// A result is computed with the predicates of the first reader that asks. Runtime filters only
// add predicates and a TopN threshold only tightens, so a result computed with fewer of them is
// a superset that later readers still filter row by row, and one computed with more of them
// drops only rows that no reader returns.
struct SharedSegmentIndexResult {
    // Rows inside the key ranges, from the short key or primary key index.
    SharedRowRanges key_ranges;
    // Rows kept by the dictionary, bloom filter and zone map indexes of the predicates.
    SharedRowRanges condition_ranges;
};

} // namespace doris::segment_v2
