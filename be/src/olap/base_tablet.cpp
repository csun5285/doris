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

#include "olap/base_tablet.h"

#include <fmt/format.h>
#include <glog/logging.h>

#include <ostream>
#include <vector>

#include "gutil/strings/substitute.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema_cache.h"
#include "util/doris_metrics.h"

namespace doris {
using namespace ErrorCode;

extern MetricPrototype METRIC_query_scan_bytes;
extern MetricPrototype METRIC_query_scan_rows;
extern MetricPrototype METRIC_query_scan_count;

BaseTablet::BaseTablet(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir)
        : _state(tablet_meta->tablet_state()), _tablet_meta(tablet_meta), _data_dir(data_dir) {
    _schema = TabletSchemaCache::instance()->insert(index_id(), _tablet_meta->tablet_schema());
    _gen_tablet_path();

    std::stringstream ss;
    ss << _tablet_meta->tablet_id() << "." << _tablet_meta->schema_hash() << "."
       << _tablet_meta->tablet_uid().to_string();
    _full_name = ss.str();

    _metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            strings::Substitute("Tablet.$0", tablet_id()),
            {{"tablet_id", std::to_string(tablet_id())}}, MetricEntityType::kTablet);

    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_bytes);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_rows);
    INT_COUNTER_METRIC_REGISTER(_metric_entity, query_scan_count);
    std::lock_guard lock(s_mtx);
    if (s_table_id_to_table_name.find(_tablet_meta->table_id()) != s_table_id_to_table_name.end()) {
        s_table_id_to_table_name.insert(
                std::make_pair(_tablet_meta->table_id(), _tablet_meta->table_name()));
    }
}

BaseTablet::~BaseTablet() {
    DorisMetrics::instance()->metric_registry()->deregister_entity(_metric_entity);
}

Status BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        LOG(WARNING) << "could not change tablet state from shutdown to " << state;
        return Status::Error<META_INVALID_ARGUMENT>();
    }
    _tablet_meta->set_tablet_state(state);
    _state = state;
    return Status::OK();
}

void BaseTablet::_gen_tablet_path() {
    if (_data_dir != nullptr && _tablet_meta != nullptr) {
        _tablet_path = fmt::format("{}/{}/{}/{}/{}", _data_dir->path(), DATA_PREFIX, shard_id(),
                                   tablet_id(), schema_hash());
    }
}

bool BaseTablet::set_tablet_schema_into_rowset_meta() {
    bool flag = false;
    for (RowsetMetaSharedPtr rowset_meta : _tablet_meta->all_mutable_rs_metas()) {
        if (!rowset_meta->tablet_schema()) {
            rowset_meta->set_tablet_schema(_schema);
            flag = true;
        }
    }
    return flag;
}

} /* namespace doris */
