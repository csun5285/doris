# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/cloud")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/cloud")

set(IO_FILES
    io/file_system_map.cpp
    io/local_file_reader.cpp
    io/local_file_system.cpp
    io/local_file_writer.cpp
    io/s3_file_reader.cpp
    io/s3_file_system.cpp
    io/s3_file_writer.cpp
    io/cloud_file_segment.cpp
    io/cloud_lru_file_cache.cpp
    io/cloud_file_cache.cpp
    io/cloud_file_cache_factory.cpp
    io/cached_remote_file_reader.cpp
    io/cloud_file_cache_profile.cpp
    io/tmp_file_mgr.cpp
    io/cached_remote_file_writer.cpp
    io/s3_file_write_bufferpool.cpp
)

set(OLAP_FILES
    olap/beta_rowset_writer.cpp
    olap/beta_rowset.cpp
    olap/olap_server.cpp 
    olap/segment.cpp
)

add_library(Cloud STATIC
cloud_base_compaction.cpp
cloud_cumulative_compaction.cpp
cloud_meta_mgr.cpp
cloud_schema_change.cpp
cloud_stream_load_executor.cpp
cloud_tablet_mgr.cpp
utils.cpp
${IO_FILES}
${OLAP_FILES}
)
