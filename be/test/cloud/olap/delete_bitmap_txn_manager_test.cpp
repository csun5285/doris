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

#include "cloud/olap/delete_bitmap_txn_manager.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <chrono>
#include <memory>
#include <thread>

#include "common/status.h"
#include "common/sync_point.h"
#include "olap/olap_common.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/rowset.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

TEST(DeleteBitmapTxnManagerTest, normal) {
    auto sp = SyncPoint::get_instance();
    sp->enable_processing();
    sp->set_call_back("DeleteBitmapTxnManager::remove_expired_tablet_txn_info", [](auto&& args) {
        bool* pred = try_any_cast<bool*>(args.at(0));
        *pred = true;
    });

    DeleteBitmapTxnManager txn_manager(1024 * 1024);
    auto st = txn_manager.init();
    EXPECT_EQ(st, Status::OK());

    RowsetId rowset_id;
    rowset_id.init(1);
    DeleteBitmapPtr input_delete_bitmap = std::make_shared<DeleteBitmap>(222);
    input_delete_bitmap->add({rowset_id, 1, 2}, 10);

    RowsetIdUnorderedSet input_rowset_ids;
    input_rowset_ids.emplace(rowset_id);

    RowsetMetaSharedPtr rst_meta_ptr(new RowsetMeta());
    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_rowset_id(123);
    rst_meta_ptr->init_from_pb(rowset_meta_pb);
    RowsetSharedPtr input_rowset = std::make_shared<BetaRowset>(nullptr, "", rst_meta_ptr);
    std::shared_ptr<PartialUpdateInfo> input_partial_update_info =
            std::make_shared<PartialUpdateInfo>();

    // set txn_id1
    TTransactionId txn_id1 = 111;
    int64_t tablet_id1 = 222;
    int64_t txn_expiration1 =
            duration_cast<std::chrono::seconds>(std::chrono::system_clock::now().time_since_epoch())
                    .count();
    txn_manager.set_tablet_txn_info(txn_id1, tablet_id1, input_delete_bitmap, input_rowset_ids,
                                    input_rowset, txn_expiration1, input_partial_update_info);

    // set txn_id2
    TTransactionId txn_id2 = 112;
    int64_t tablet_id2 = 223;
    int64_t txn_expiration2 = 0;
    txn_manager.set_tablet_txn_info(txn_id2, tablet_id2, input_delete_bitmap, input_rowset_ids,
                                    input_rowset, txn_expiration2, input_partial_update_info);

    // get txn_id1
    RowsetSharedPtr out_rowset;
    DeleteBitmapPtr out_delete_bitmap;
    RowsetIdUnorderedSet out_rowset_ids;
    int64_t out_txn_expiration;
    std::shared_ptr<PartialUpdateInfo> out_partial_update_info;
    std::shared_ptr<PublishStatus> publish_status;
    st = txn_manager.get_tablet_txn_info(txn_id1, tablet_id1, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_EQ(st, Status::OK());
    EXPECT_EQ(out_rowset.get(), input_rowset.get());
    EXPECT_EQ(out_delete_bitmap.get(), input_delete_bitmap.get());
    EXPECT_TRUE(out_delete_bitmap->contains({rowset_id, 1, 2}, 10));
    EXPECT_EQ(out_delete_bitmap->cardinality(), 1);
    EXPECT_EQ(out_partial_update_info.get(), input_partial_update_info.get());
    EXPECT_EQ(out_txn_expiration, txn_expiration1);
    EXPECT_EQ(*publish_status, PublishStatus::INIT);

    // get txn_id2
    st = txn_manager.get_tablet_txn_info(txn_id2, tablet_id2, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_EQ(st, Status::OK());
    EXPECT_EQ(out_rowset.get(), input_rowset.get());
    EXPECT_EQ(out_delete_bitmap.get(), input_delete_bitmap.get());
    EXPECT_TRUE(out_delete_bitmap->contains({rowset_id, 1, 2}, 10));
    EXPECT_EQ(out_delete_bitmap->cardinality(), 1);
    EXPECT_EQ(out_partial_update_info.get(), input_partial_update_info.get());
    EXPECT_EQ(*publish_status, PublishStatus::INIT);

    // update txn_id1
    RowsetId rowset_id2;
    rowset_id2.init(2);
    input_delete_bitmap->add({rowset_id2, 1, 2}, 11);
    input_rowset_ids.emplace(rowset_id2);
    txn_manager.update_tablet_txn_info(txn_id1, tablet_id1, input_delete_bitmap, input_rowset_ids,
                                       PublishStatus::SUCCEED);

    st = txn_manager.get_tablet_txn_info(txn_id1, tablet_id1, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_EQ(st, Status::OK());
    EXPECT_EQ(out_rowset.get(), input_rowset.get());
    EXPECT_TRUE(out_delete_bitmap->contains({rowset_id, 1, 2}, 10));
    EXPECT_TRUE(out_delete_bitmap->contains({rowset_id2, 1, 2}, 11));
    EXPECT_EQ(out_delete_bitmap->cardinality(), 2);
    EXPECT_EQ(out_partial_update_info.get(), input_partial_update_info.get());
    EXPECT_EQ(out_txn_expiration, txn_expiration1);
    EXPECT_EQ(*publish_status, PublishStatus::SUCCEED);

    sp->set_call_back("DeleteBitmapTxnManager::remove_expired_tablet_txn_info", [](auto&& args) {
        bool* pred = try_any_cast<bool*>(args.at(0));
        *pred = false;
    });
    // remove expired txn
    txn_manager.remove_expired_tablet_txn_info();

    st = txn_manager.get_tablet_txn_info(txn_id1, tablet_id1, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_TRUE(st.is<ErrorCode::NOT_FOUND>());

    st = txn_manager.get_tablet_txn_info(txn_id2, tablet_id2, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_EQ(st, Status::OK());
}

TEST(DeleteBitmapTxnManagerTest, cache_miss) {
    DeleteBitmapTxnManager txn_manager(1);
    auto st = txn_manager.init();
    EXPECT_EQ(st, Status::OK());

    RowsetId rowset_id;
    rowset_id.init(1);
    DeleteBitmapPtr input_delete_bitmap = std::make_shared<DeleteBitmap>(222);
    input_delete_bitmap->add({rowset_id, 1, 2}, 10);

    RowsetIdUnorderedSet input_rowset_ids;
    input_rowset_ids.emplace(rowset_id);

    RowsetMetaSharedPtr rst_meta_ptr(new RowsetMeta());
    RowsetMetaPB rowset_meta_pb;
    rowset_meta_pb.set_rowset_id(123);
    rst_meta_ptr->init_from_pb(rowset_meta_pb);
    RowsetSharedPtr input_rowset = std::make_shared<BetaRowset>(nullptr, "", rst_meta_ptr);
    std::shared_ptr<PartialUpdateInfo> partial_update_info = std::make_shared<PartialUpdateInfo>();

    // set txn_id1
    TTransactionId txn_id1 = 111;
    int64_t tablet_id1 = 222;
    int64_t txn_expiration1 = 0;
    txn_manager.set_tablet_txn_info(txn_id1, tablet_id1, input_delete_bitmap, input_rowset_ids,
                                    input_rowset, txn_expiration1, partial_update_info);

    // set txn_id2
    TTransactionId txn_id2 = 112;
    int64_t tablet_id2 = 223;
    int64_t txn_expiration2 = 0;
    txn_manager.set_tablet_txn_info(txn_id2, tablet_id2, input_delete_bitmap, input_rowset_ids,
                                    input_rowset, txn_expiration2, partial_update_info);
    // get txn_id1
    RowsetSharedPtr out_rowset;
    DeleteBitmapPtr out_delete_bitmap;
    RowsetIdUnorderedSet out_rowset_ids;
    int64_t out_txn_expiration;
    std::shared_ptr<PartialUpdateInfo> out_partial_update_info;
    std::shared_ptr<PublishStatus> publish_status;
    st = txn_manager.get_tablet_txn_info(txn_id1, tablet_id1, &out_rowset, &out_delete_bitmap,
                                         &out_rowset_ids, &out_txn_expiration,
                                         &out_partial_update_info, &publish_status);
    EXPECT_EQ(st, Status::OK());
    EXPECT_EQ(out_rowset.get(), input_rowset.get());
    EXPECT_NE(out_delete_bitmap.get(), input_delete_bitmap.get());
    EXPECT_EQ(out_delete_bitmap->cardinality(), 0);
    EXPECT_EQ(*publish_status, PublishStatus::INIT);
}

} // namespace doris::cloud