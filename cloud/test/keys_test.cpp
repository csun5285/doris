
// clang-format off
#include "common/util.h"
#include "meta-service/keys.h"

#include "gtest/gtest.h"

#include <cstring>
#include <iostream>
#include <random>
#include <variant>
#include <vector>
// clang-format on

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

// extern
namespace selectdb {
void encode_int64(int64_t val, std::string* b);
int decode_int64(std::string_view* in, int64_t* val);
void encode_bytes(std::string_view bytes, std::string* b);
int decode_bytes(std::string_view* in, std::string* out);
} // namespace selectdb

// clang-format off
// Possible key encoding schemas:
//
// 0x01 "instance" ${instance_id} -> InstanceInfoPB
// 
// 0x01 "txn" ${instance_id} "txn_label" ${db_id} ${label} -> TxnLabelPB ${version_timestamp}
// 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
// 0x01 "txn" ${instance_id} "txn_index" ${version_timestamp} -> TxnIndexPB
// 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> TxnRunningPB // creaet at begin, delete at commit
//
// 0x01 "version" ${instance_id} "partition" ${db_id} ${tbl_id} ${partition_id} -> VersionPB
// 
// 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "rowset_tmp" ${txn_id} ${rowset_id} -> RowsetMetaPB
// 0x01 "meta" ${instance_id} "tablet" ${table_id} ${tablet_id} -> TabletMetaPB
// 0x01 "meta" ${instance_id} "tablet_table" ${tablet_id} -> ${table_id}
// 0x01 "meta" ${instance_id} "tablet_tmp" ${table_id} ${tablet_id} -> TabletMetaPB
// 
// 0x01 "trash" ${instacne_id} "table" -> TableTrashPB
// 
// 0x01 "node_status" ${instance_id} "compute" ${backend_id} -> ComputeNodeStatusPB
// clang-format on

TEST(KeysTest, KeysTest) {
    using namespace selectdb;
    std::string instance_id = "instance_id_deadbeef";

    // rowset meta key
    // 0x01 "meta" ${instance_id} "rowset" ${tablet_id} ${version}
    {
        int64_t tablet_id = 10086;
        int64_t version = 100;
        MetaRowsetKeyInfo rowset_key {instance_id, tablet_id, version};
        std::string encoded_rowset_key0;
        meta_rowset_key(rowset_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;
        int64_t dec_version = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_rowset_prefix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_rowset_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_version), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);
        EXPECT_EQ(version, dec_version);

        std::get<2>(rowset_key) = version + 1;
        std::string encoded_rowset_key1;
        meta_rowset_key(rowset_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet meta key
    // 0x01 "meta" ${instance_id} "tablet" ${table_id} ${index_id} ${partition_id} ${tablet_id} -> TabletMetaPB
    {
        int64_t table_id = 10010;
        int64_t index_id = 10011;
        int64_t partition_id = 10012;
        int64_t tablet_id = 10086;
        MetaTabletKeyInfo tablet_key {instance_id, table_id, index_id, partition_id, tablet_id};
        std::string encoded_rowset_key0;
        meta_tablet_key(tablet_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_table_id = 0;
        int64_t dec_tablet_id = 0;
        int64_t dec_index_id = 0;
        int64_t dec_partition_id = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_tablet_prefix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_tablet_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(table_id, dec_table_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);

        std::get<2>(tablet_key) = tablet_id + 1;
        std::string encoded_rowset_key1;
        meta_tablet_key(tablet_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet table key
    // 0x01 "meta" ${instance_id} "tablet_table" ${tablet_id} -> ${table_id}
    {
        int64_t tablet_id = 10086;
        MetaTabletIdxKeyInfo tablet_tbl_key {instance_id, tablet_id};
        std::string encoded_rowset_key0;
        meta_tablet_idx_key(tablet_tbl_key, &encoded_rowset_key0);
        std::cout << hex(encoded_rowset_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_tablet_id = 0;

        std::string_view key_sv(encoded_rowset_key0);
        std::string dec_meta_prefix;
        std::string dec_tablet_prefix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_meta_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_tablet_prefix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_tablet_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(tablet_id, dec_tablet_id);

        std::get<1>(tablet_tbl_key) = tablet_id + 1;
        std::string encoded_rowset_key1;
        meta_tablet_idx_key(tablet_tbl_key, &encoded_rowset_key1);
        std::cout << hex(encoded_rowset_key1) << std::endl;

        ASSERT_GT(encoded_rowset_key1, encoded_rowset_key0);
    }

    // tablet schema key
    // 0x01 "meta" ${instance_id} "schema" ${index_id} ${schema_version} -> TabletSchemaPB
    {
        int64_t index_id = 10000;
        int32_t schema_version = 5;
        auto key = meta_schema_key({instance_id, index_id, schema_version});

        std::string dec_instance_id;
        int64_t dec_index_id = 0;
        int64_t dec_schema_version = 0;

        std::string_view key_sv(key);
        std::string dec_schema_prefix;
        std::string dec_schema_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_schema_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_schema_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_index_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_schema_version), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(index_id, dec_index_id);
        EXPECT_EQ(schema_version, dec_schema_version);
        EXPECT_EQ(dec_schema_prefix, "meta");
        EXPECT_EQ(dec_schema_infix, "schema");
    }

    // 0x01 "version" ${instance_id} "version_id" ${db_id} ${tbl_id} ${partition_id} -> ${version}
    {
        int64_t db_id = 11111;
        int64_t tablet_id = 10086;
        int64_t partition_id = 9998;
        VersionKeyInfo v_key {instance_id, db_id, tablet_id, partition_id};
        std::string encoded_version_key0;
        version_key(v_key, &encoded_version_key0);
        std::cout << "version key after encode: " << hex(encoded_version_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_table_id = 0;
        int64_t dec_partition_id = 0;

        std::string_view key_sv(encoded_version_key0);
        std::string dec_version_prefix;
        std::string dec_version_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_version_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0) << hex(key_sv);
        ASSERT_EQ(decode_int64(&key_sv, &dec_table_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_partition_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(tablet_id, dec_table_id);
        EXPECT_EQ(partition_id, dec_partition_id);

        std::get<3>(v_key) = partition_id + 1;
        std::string encoded_version_key1;
        version_key(v_key, &encoded_version_key1);
        std::cout << "version key after encode: " << hex(encoded_version_key1) << std::endl;

        ASSERT_GT(encoded_version_key1, encoded_version_key0);
    }
}

TEST(KeysTest, TxnKeysTest) {
    using namespace selectdb;
    std::string instance_id = "instance_id_deadbeef";

    // 0x01 "txn" ${instance_id} "txn_label" ${db_id} ${label} -> set<${version_timestamp}>
    {
        int64_t db_id = 12345678;
        std::string label = "label1xxx";
        TxnLabelKeyInfo index_key {instance_id, db_id, label};
        std::string encoded_txn_index_key0;
        txn_label_key(index_key, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        std::string dec_label;

        std::string_view key_sv(encoded_txn_index_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_label), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(label, dec_label);

        std::get<1>(index_key) = db_id + 1;
        std::string encoded_txn_index_key1;
        txn_label_key(index_key, &encoded_txn_index_key1);
        std::cout << hex(encoded_txn_index_key1) << std::endl;

        ASSERT_GT(encoded_txn_index_key1, encoded_txn_index_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_info" ${db_id} ${version_timestamp} -> TxnInfoPB
    {
        int64_t db_id = 12345678;
        int64_t txn_id = 10086;
        TxnInfoKeyInfo info_key {instance_id, db_id, txn_id};
        std::string encoded_txn_info_key0;
        txn_info_key(info_key, &encoded_txn_info_key0);
        std::cout << hex(encoded_txn_info_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_info_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<1>(info_key) = db_id + 1;
        std::string encoded_txn_info_key1;
        txn_info_key(info_key, &encoded_txn_info_key1);
        std::cout << hex(encoded_txn_info_key1) << std::endl;

        ASSERT_GT(encoded_txn_info_key1, encoded_txn_info_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_index" ${version_timestamp} -> TxnIndexPB
    {
        int64_t txn_id = 12343212453;
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_index_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<1>(txn_index_key_) = txn_id + 1;
        std::string encoded_txn_index_key1;
        txn_index_key(txn_index_key_, &encoded_txn_index_key1);
        std::cout << hex(encoded_txn_index_key1) << std::endl;

        ASSERT_GT(encoded_txn_index_key1, encoded_txn_index_key0);
    }

    // 0x01 "txn" ${instance_id} "txn_running" ${db_id} ${version_timestamp} -> ${table_id_list}
    {
        int64_t db_id = 98712345;
        int64_t txn_id = 12343212453;
        TxnRunningKeyInfo running_key {instance_id, db_id, txn_id};
        std::string encoded_txn_running_key0;
        txn_running_key(running_key, &encoded_txn_running_key0);
        std::cout << hex(encoded_txn_running_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_txn_running_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<2>(running_key) = txn_id + 1;
        std::string encoded_txn_running_key1;
        txn_running_key(running_key, &encoded_txn_running_key1);
        std::cout << hex(encoded_txn_running_key1) << std::endl;

        ASSERT_GT(encoded_txn_running_key1, encoded_txn_running_key0);
    }

    // 0x01 "txn" ${instance_id} "recycle_txn" ${db_id} ${version_timestamp} -> ${table_id_list}
    {
        int64_t db_id = 98712345;
        int64_t txn_id = 12343212453;
        RecycleTxnKeyInfo recycle_key {instance_id, db_id, txn_id};
        std::string encoded_recycle_txn_key0;
        txn_running_key(recycle_key, &encoded_recycle_txn_key0);
        std::cout << hex(encoded_recycle_txn_key0) << std::endl;

        std::string dec_instance_id;
        int64_t dec_db_id = 0;
        int64_t dec_txn_id = 0;

        std::string_view key_sv(encoded_recycle_txn_key0);
        std::string dec_txn_prefix;
        std::string dec_txn_infix;
        key_sv.remove_prefix(1); // Remove CLOUD_USER_KEY_SPACE01
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_prefix), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_instance_id), 0);
        ASSERT_EQ(decode_bytes(&key_sv, &dec_txn_infix), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_db_id), 0);
        ASSERT_EQ(decode_int64(&key_sv, &dec_txn_id), 0);

        EXPECT_EQ(instance_id, dec_instance_id);
        EXPECT_EQ(db_id, dec_db_id);
        EXPECT_EQ(txn_id, dec_txn_id);

        std::get<2>(recycle_key) = txn_id + 1;
        std::string encoded_recycle_txn_key1;
        txn_running_key(recycle_key, &encoded_recycle_txn_key1);
        std::cout << hex(encoded_recycle_txn_key1) << std::endl;

        ASSERT_GT(encoded_recycle_txn_key1, encoded_recycle_txn_key0);
    }
}

TEST(KeysTest, DecodeKeysTest) {
    using namespace selectdb;
    // clang-format off
    std::string key = "011074786e000110696e7374616e63655f69645f646561646265656600011074786e5f696e646578000112000000000000271310696e736572745f336664356164313264303035346139622d386337373664333231386336616462370001";
    // clang-format on
    auto pretty_key = prettify_key(key);
    ASSERT_TRUE(!pretty_key.empty()) << key;
    std::cout << "\n" << pretty_key << std::endl;

    pretty_key = prettify_key(key, true);
    ASSERT_TRUE(!pretty_key.empty()) << key;
    std::cout << "\n" << pretty_key << std::endl;
}

// vim: et tw=100 ts=4 sw=4 cc=80:
