#include "cloud/cloud_tablet_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cloud/utils.h"
#include "common/config.h"
#include "common/sync_point.h"
#include "olap/storage_engine.h"

namespace doris::cloud {

TEST(CloudTabletMgrTest, normal) {
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("CloudTabletMgr::get_tablet");
        sp->clear_call_back("CloudMetaMgr::get_tablet_meta");
        sp->clear_call_back("CloudMetaMgr::sync_tablet_rowsets");
    }};
    sp->enable_processing();

    bool cache_missed;

    sp->set_call_back("CloudTabletMgr::get_tablet", [&cache_missed](auto&& args) {
        cache_missed = (try_any_cast<Cache::Handle*>(args[0]) == nullptr);
    });
    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [](auto&& args) {
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto tablet_meta_p = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        *tablet_meta_p = std::move(tablet_meta);
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::tablet_cache_capacity = 1;
    config::tablet_cache_shards = 1;
    StorageEngine storage_engine({});

    Status st;
    TabletSharedPtr tablet;

    // test cache hit
    st = tablet_mgr()->get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr()->get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    st = tablet_mgr()->get_tablet(20001, &tablet); // evict tablet 20000
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr()->get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    // test cached value lifetime
    st = tablet_mgr()->get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    tablet_mgr()->erase_tablet(20000);
    ASSERT_EQ(20000, tablet->tablet_id());

    TabletSharedPtr tablet1;
    st = tablet_mgr()->get_tablet(20000, &tablet1); // erase tablet from tablet_map
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    ASSERT_EQ(20000, tablet->tablet_id());
}

TEST(CloudTabletMgrTest, concurrent) {
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("CloudMetaMgr::get_tablet_meta");
        sp->clear_call_back("CloudMetaMgr::sync_tablet_rowsets");
    }};
    sp->enable_processing();

    int load_tablet_cnt = 0;

    sp->set_call_back("CloudMetaMgr::get_tablet_meta", [&load_tablet_cnt](auto&& args) {
        ++load_tablet_cnt;
        std::this_thread::sleep_for(std::chrono::seconds(1));
        auto tablet_id = try_any_cast<int64_t>(args[0]);
        auto tablet_meta_p = try_any_cast<TabletMetaSharedPtr*>(args[1]);
        auto tablet_meta = std::make_shared<TabletMeta>();
        tablet_meta->_tablet_id = tablet_id;
        tablet_meta->_index_id = 10002;
        *tablet_meta_p = std::move(tablet_meta);
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });
    sp->set_call_back("CloudMetaMgr::sync_tablet_rowsets", [](auto&& args) {
        auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
        pair->second = true;
    });

    config::tablet_cache_capacity = 1;
    config::tablet_cache_shards = 1;
    StorageEngine storage_engine({});

    auto get_tablet = [&]() {
        for (int i = 0; i < 10000; ++i) {
            TabletSharedPtr tablet;
            ASSERT_EQ(tablet_mgr()->get_tablet(20000, &tablet), Status::OK());
            ASSERT_EQ(20000, tablet->tablet_id());
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.push_back(std::thread(get_tablet));
    }
    for (auto& th : threads) {
        th.join();
    }

    ASSERT_EQ(load_tablet_cnt, 1);
}

} // namespace doris::cloud
