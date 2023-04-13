#include "cloud/cloud_tablet_mgr.h"

#include <gtest/gtest.h>

#include <chrono>
#include <thread>

#include "cloud/meta_mgr.h"
#include "common/config.h"
#include "common/sync_point.h"

namespace doris::cloud {

class MockMetaMgr final : public MetaMgr {
public:
    MockMetaMgr() = default;
    ~MockMetaMgr() override = default;

    Status get_tablet_meta(int64_t tablet_id, TabletMetaSharedPtr* tablet_meta) override {
        TEST_SYNC_POINT_CALLBACK("MockMetaMgr::get_tablet_meta", {});
        auto tablet_meta1 = std::make_shared<TabletMeta>();
        tablet_meta1->_tablet_id = tablet_id;
        *tablet_meta = std::move(tablet_meta1);
        return Status::OK();
    }

    Status sync_tablet_rowsets(Tablet* tablet) override { return Status::OK(); }
};

TEST(CloudTabletMgrTest, normal) {
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("CloudTabletMgr::get_tablet");
        sp->clear_call_back("meta_mgr");
    }};
    sp->enable_processing();

    MockMetaMgr mock_meta_mgr;
    bool cache_missed;

    sp->set_call_back("CloudTabletMgr::get_tablet", [&cache_missed](auto&& args) {
        cache_missed = (try_any_cast<Cache::Handle*>(args[0]) == nullptr);
    });
    sp->set_call_back("meta_mgr", [&mock_meta_mgr](auto&& args) {
        auto pair = try_any_cast<std::pair<MetaMgr*, bool>*>(args.back());
        pair->second = true;
        pair->first = &mock_meta_mgr;
    });

    config::tablet_cache_capacity = 1;
    config::tablet_cache_shards = 1;
    CloudTabletMgr tablet_mgr;

    Status st;
    TabletSharedPtr tablet;

    // test cache hit
    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    st = tablet_mgr.get_tablet(20001, &tablet); // evict tablet 20000
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    // test cached value lifetime
    st = tablet_mgr.get_tablet(20000, &tablet);
    ASSERT_EQ(st, Status::OK());
    ASSERT_FALSE(cache_missed);

    tablet_mgr.erase_tablet(20000);
    ASSERT_EQ(20000, tablet->tablet_id());

    TabletSharedPtr tablet1;
    st = tablet_mgr.get_tablet(20000, &tablet1); // erase tablet from tablet_map
    ASSERT_EQ(st, Status::OK());
    ASSERT_TRUE(cache_missed);

    ASSERT_EQ(20000, tablet->tablet_id());
}

TEST(CloudTabletMgrTest, concurrent) {
    auto sp = SyncPoint::get_instance();
    Defer defer {[sp] {
        sp->clear_call_back("meta_mgr");
        sp->clear_call_back("MockMetaMgr::get_tablet_meta");
    }};
    sp->enable_processing();

    MockMetaMgr mock_meta_mgr;
    int load_tablet_cnt = 0;

    sp->set_call_back("meta_mgr", [&mock_meta_mgr](auto&& args) {
        auto pair = try_any_cast<std::pair<MetaMgr*, bool>*>(args.back());
        pair->second = true;
        pair->first = &mock_meta_mgr;
    });
    sp->set_call_back("MockMetaMgr::get_tablet_meta", [&load_tablet_cnt](auto&& args) {
        ++load_tablet_cnt;
        std::this_thread::sleep_for(std::chrono::seconds(1));
    });

    config::tablet_cache_capacity = 1;
    config::tablet_cache_shards = 1;
    CloudTabletMgr tablet_mgr;

    auto get_tablet = [&]() {
        for (int i = 0; i < 10000; ++i) {
            TabletSharedPtr tablet;
            ASSERT_EQ(tablet_mgr.get_tablet(20000, &tablet), Status::OK());
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
