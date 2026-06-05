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

#include "storage/segment/storage_view.h"

#include "storage/key_coder.h"
#include "storage/types.h"

namespace doris::segment_v2 {

bool storage_view_is_null_at(const StorageView& view, size_t row_offset) {
    DCHECK_LT(row_offset, view.num_rows);
    return view.nullmap != nullptr && view.nullmap[row_offset] != 0;
}

Status storage_view_encode_full_key_ascending(const KeyCoder* coder, const StorageView& view,
                                               size_t row_offset, std::string* buf) {
    DCHECK_LT(row_offset, view.num_rows);
    if (storage_view_is_null_at(view, row_offset)) {
        buf->push_back(static_cast<char>(KeyConsts::KEY_NULL_FIRST_MARKER));
        return Status::OK();
    }
    if (UNLIKELY(coder == nullptr)) {
        return Status::NotSupported<false>("no KeyCoder available for storage_view encoding");
    }
    buf->push_back(static_cast<char>(KeyConsts::KEY_NORMAL_MARKER));
    coder->full_encode_ascending(view.data + row_offset * view.row_size, buf);
    return Status::OK();
}

Status storage_view_encode_short_key_ascending(const KeyCoder* coder, const StorageView& view,
                                                size_t row_offset, std::string* buf,
                                                size_t index_size) {
    DCHECK_LT(row_offset, view.num_rows);
    if (storage_view_is_null_at(view, row_offset)) {
        buf->push_back(static_cast<char>(KeyConsts::KEY_NULL_FIRST_MARKER));
        return Status::OK();
    }
    if (UNLIKELY(coder == nullptr)) {
        return Status::NotSupported<false>("no KeyCoder available for storage_view encoding");
    }
    buf->push_back(static_cast<char>(KeyConsts::KEY_NORMAL_MARKER));
    coder->encode_ascending(view.data + row_offset * view.row_size, index_size, buf);
    return Status::OK();
}

} // namespace doris::segment_v2
