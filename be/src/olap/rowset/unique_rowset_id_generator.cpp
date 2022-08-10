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

#include "olap/rowset/unique_rowset_id_generator.h"

#include "util/doris_metrics.h"
#include "util/spinlock.h"
#include "util/stack_util.h"
#include "util/uid_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(rowset_count_generated_and_in_use, MetricUnit::ROWSETS);

UniqueRowsetIdGenerator::UniqueRowsetIdGenerator(const UniqueId& backend_uid)
        : _backend_uid(backend_uid), _inc_id(0) {
    REGISTER_HOOK_METRIC(rowset_count_generated_and_in_use, [this]() {
        std::lock_guard<SpinLock> l(_lock);
        return _valid_rowset_id_hi.size();
    });
}

UniqueRowsetIdGenerator::~UniqueRowsetIdGenerator() {
    DEREGISTER_HOOK_METRIC(rowset_count_generated_and_in_use);
}

// generate a unique rowset id and save it in a set to check whether it is valid in the future
RowsetId UniqueRowsetIdGenerator::next_id() {
    RowsetId rowset_id;
    rowset_id.init(_version, ++_inc_id, _backend_uid.hi, _backend_uid.lo);
#ifndef CLOUD_MODE
    {
        std::lock_guard<SpinLock> l(_lock);
        _valid_rowset_id_hi.insert(rowset_id.hi);
    }
#endif
    return rowset_id;
}

bool UniqueRowsetIdGenerator::id_in_use(const RowsetId& rowset_id) const {
    // if rowset_id == 1, then it is an old version rowsetid, not gc it
    // because old version rowset id is not generated by this code, so that not delete them
    if (rowset_id.version < _version) {
        return true;
    }
    if ((rowset_id.mi != _backend_uid.hi) || (rowset_id.lo != _backend_uid.lo)) {
        return false;
    }

    std::lock_guard<SpinLock> l(_lock);
    return _valid_rowset_id_hi.count(rowset_id.hi) == 1;
}

void UniqueRowsetIdGenerator::release_id(const RowsetId& rowset_id) {
    // Only release the rowsetid generated after this startup.
    // So we need to check version/mid/low part first
    if (rowset_id.version < _version) {
        return;
    }
    if ((rowset_id.mi != _backend_uid.hi) || (rowset_id.lo != _backend_uid.lo)) {
        return;
    }
    std::lock_guard<SpinLock> l(_lock);
    _valid_rowset_id_hi.erase(rowset_id.hi);
}

} // namespace doris
