#include <gtest/gtest.h>

#include "cloud/olap/storage_engine.h"
#include "olap/rowset/rowset_factory.h"
#include "olap/rowset/unique_rowset_id_generator.h"
#include "olap/tablet.h"

namespace doris {

static UniqueRowsetIdGenerator id_generator({0, 1});

static RowsetMetaSharedPtr create_rowset_meta(Version version) {
    auto rs_meta = std::make_shared<RowsetMeta>();
    rs_meta->set_rowset_id(id_generator.next_id());
    rs_meta->set_rowset_type(BETA_ROWSET);
    rs_meta->_rowset_meta_pb.set_start_version(version.first);
    rs_meta->_rowset_meta_pb.set_end_version(version.second);
    return rs_meta;
}

static RowsetSharedPtr create_rowset(Version version) {
    auto rs_meta = create_rowset_meta(version);
    RowsetSharedPtr rowset;
    RowsetFactory::create_rowset(nullptr, "", std::move(rs_meta), &rowset);
    return rowset;
}

TEST(CloudTabletTest, calc_missed_versions) {
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({0, 4}));
        rowsets.push_back(create_rowset({5, 5}));
        rowsets.push_back(create_rowset({8, 8}));
        rowsets.push_back(create_rowset({9, 9}));
        rowsets.push_back(create_rowset({13, 13}));
        auto tablet_meta = std::make_shared<TabletMeta>();
        Tablet tablet(std::move(tablet_meta), nullptr);
        tablet.cloud_add_rowsets(std::move(rowsets), false);

        ASSERT_EQ(tablet.cloud_calc_missed_versions(3), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(4), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(5), Versions {});
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{6, 6}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(7), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(8), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(9), (Versions {{6, 7}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(10), (Versions {{6, 7}, {10, 10}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(11), (Versions {{6, 7}, {10, 11}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(12), (Versions {{6, 7}, {10, 12}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(13), (Versions {{6, 7}, {10, 12}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(14), (Versions {{6, 7}, {10, 12}, {14, 14}}));
    }
    {
        auto tablet_meta = std::make_shared<TabletMeta>();
        Tablet tablet(std::move(tablet_meta), nullptr);
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{0, 6}}));
    }
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({5, 5}));
        auto tablet_meta = std::make_shared<TabletMeta>();
        Tablet tablet(std::move(tablet_meta), nullptr);
        tablet.cloud_add_rowsets(std::move(rowsets), false);

        ASSERT_EQ(tablet.cloud_calc_missed_versions(4), (Versions {{0, 4}}));
        ASSERT_EQ(tablet.cloud_calc_missed_versions(6), (Versions {{0, 4}, {6, 6}}));
    }
}

TEST(CloudTabletTest, add_rowsets) {
    StorageEngine storage_engine({});
    auto tablet_meta = std::make_shared<TabletMeta>();
    Tablet tablet(std::move(tablet_meta), nullptr);
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({0, 1}));
        rowsets.push_back(create_rowset({2, 2}));
        rowsets.push_back(create_rowset({3, 3}));
        rowsets.push_back(create_rowset({4, 4}));
        tablet.cloud_add_rowsets(rowsets, false);
        ASSERT_EQ(tablet._rs_version_map.size(), 4);
        ASSERT_EQ(tablet.tablet_meta()->all_rs_metas().size(), 4);
        Versions versions;
        tablet.capture_consistent_versions({0, 4}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));

        versions.clear();
        // add rowsets with same version and same rowset_id
        tablet.cloud_add_rowsets(rowsets, true);
        ASSERT_EQ(tablet._rs_version_map.size(), 4);
        ASSERT_EQ(tablet.tablet_meta()->all_rs_metas().size(), 4);
        tablet.capture_consistent_versions({0, 4}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 3}, {4, 4}}));
    }
    {
        std::vector<RowsetSharedPtr> rowsets;
        // add rowsets with same version but different rowset_id
        rowsets.push_back(create_rowset({2, 2}));
        rowsets.push_back(create_rowset({3, 3}));
        rowsets.push_back(create_rowset({4, 4}));
        tablet.cloud_add_rowsets(rowsets, true);
        ASSERT_EQ(tablet._rs_version_map.size(), 4);
        ASSERT_EQ(tablet.tablet_meta()->all_rs_metas().size(), 4);
        ASSERT_EQ(tablet._stale_rs_version_map.size(), 0);
        ASSERT_EQ(tablet.tablet_meta()->all_stale_rs_metas().size(), 0);
        Versions versions;
        tablet.capture_consistent_versions({2, 4}, &versions, false);
        ASSERT_EQ(versions, (Versions {{2, 2}, {3, 3}, {4, 4}}));

        std::vector<RowSetSplits> rs_splits;
        ASSERT_EQ(tablet.cloud_capture_rs_readers({2, 4}, &rs_splits), Status::OK());
        for (size_t i = 0; i < versions.size(); ++i) {
            ASSERT_EQ(rs_splits[i].rs_reader->rowset()->rowset_id(), rowsets[i]->rowset_id());
        }
    }
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({3, 4}));
        tablet.cloud_add_rowsets(std::move(rowsets), true);
        // [0-1][2-2][3-4]
        ASSERT_EQ(tablet._rs_version_map.size(), 3);
        ASSERT_EQ(tablet.tablet_meta()->all_rs_metas().size(), 3);
        // [3-3][4-4]
        ASSERT_EQ(tablet._stale_rs_version_map.size(), 2);
        ASSERT_EQ(tablet.tablet_meta()->all_stale_rs_metas().size(), 2);
        Versions versions;
        tablet.capture_consistent_versions({0, 4}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 4}}));
        versions.clear();
        tablet.capture_consistent_versions({0, 3}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 3}}));
    }
    {
        std::vector<RowsetSharedPtr> rowsets;
        rowsets.push_back(create_rowset({2, 5}));
        rowsets.push_back(create_rowset({6, 6}));
        rowsets.push_back(create_rowset({7, 7}));
        rowsets.push_back(create_rowset({8, 8}));
        tablet.cloud_add_rowsets(std::move(rowsets), true);
        // [0-1][2-5][6-6][7-7][8-8]
        ASSERT_EQ(tablet._rs_version_map.size(), 5);
        ASSERT_EQ(tablet.tablet_meta()->all_rs_metas().size(), 5);
        // [3-3][4-4][2-2][3-4]
        ASSERT_EQ(tablet._stale_rs_version_map.size(), 4);
        ASSERT_EQ(tablet.tablet_meta()->all_stale_rs_metas().size(), 4);
        Versions versions;
        tablet.capture_consistent_versions({0, 8}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 5}, {6, 6}, {7, 7}, {8, 8}}));
        versions.clear();
        tablet.capture_consistent_versions({0, 3}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 3}}));
        versions.clear();
        tablet.capture_consistent_versions({0, 4}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 2}, {3, 4}}));
        versions.clear();
        tablet.capture_consistent_versions({0, 5}, &versions, false);
        ASSERT_EQ(versions, (Versions {{0, 1}, {2, 5}}));
    }
}

} // namespace doris
