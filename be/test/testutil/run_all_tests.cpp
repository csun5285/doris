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

#include <gtest/gtest.h>

#include "common/sync_point.h"
#include "olap/page_cache.h"
#include "olap/rowset/segment_v2/inverted_index_cache.h"
#include "olap/segment_loader.h"
#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "service/backend_options.h"
#include "util/cpu_info.h"
#include "util/disk_info.h"
#include "util/mem_info.h"

using namespace doris;

int main(int argc, char** argv) {
    doris::ExecEnv::GetInstance()->init_mem_tracker();
    doris::thread_context()->thread_mem_tracker_mgr->init();
    doris::TabletSchemaCache::create_global_schema_cache();
    doris::StoragePageCache::create_global_cache(1 << 30, 10);
    doris::SegmentLoader::create_global_instance(1000);
    std::string conf = std::string(getenv("DORIS_HOME")) + "/conf/be.conf";
    if (!doris::config::init(conf.c_str(), false)) {
        fprintf(stderr, "error read config file. \n");
        return -1;
    }
    doris::init_glog("be-test");
    ::testing::InitGoogleTest(&argc, argv);
    doris::CpuInfo::init();
    doris::DiskInfo::init();
    doris::MemInfo::init();
    doris::BackendOptions::init();
    auto sp = SyncPoint::get_instance();
    sp->set_call_back("TabletSchemaCache::insert1", [](auto&& args) {
        auto pair = try_any_cast<std::pair<TabletSchemaSPtr, bool>*>(args.back());
        pair->second = true;
    });
    sp->set_call_back("TabletSchemaCache::insert2", [](auto&& args) {
        auto pair = try_any_cast<std::pair<TabletSchemaSPtr, bool>*>(args.back());
        pair->second = true;
    });
    sp->enable_processing();
    return RUN_ALL_TESTS();
}
