
// clang-format off
#include <gen_cpp/olap_file.pb.h>
#include <cstddef>
#include "common/config.h"
#include "common/util.h"
#include "common/stopwatch.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/txn_kv.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "common/sync_point.h"

#include "gtest/gtest.h"
// clang-format on

using namespace selectdb;

std::shared_ptr<TxnKv> txn_kv;

void init_txn_kv() {
    config::fdb_cluster_file_path = "fdb.cluster";
    txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    int ret = txn_kv->init();
    ASSERT_EQ(ret, 0);
}

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    init_txn_kv();
    return RUN_ALL_TESTS();
}

TEST(TxnKvTest, GetVersionTest) {
    std::unique_ptr<Transaction> txn;
    std::string key;
    std::string val;
    int ret;
    {
        ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        key.push_back('\xfe');
        key.append(" unit_test_prefix ");
        key.append(" GetVersionTest ");
        txn->atomic_set_ver_value(key, "");
        ret = txn->commit();
        int64_t ver0 = txn->get_committed_version();
        ASSERT_GT(ver0, 0);

        ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, 0);
        ret = txn->get(key, &val);
        ASSERT_EQ(ret, 0);
        int64_t ver1 = txn->get_read_version();
        ASSERT_GE(ver1, ver0);

        int64_t ver2;
        int64_t txn_id;
        ret = get_txn_id_from_fdb_ts(val, &txn_id);
        ASSERT_EQ(ret, 0);
        ver2 = txn_id >> 10;

        std::cout << "ver0=" << ver0 << " ver1=" << ver1 << " ver2=" << ver2 << std::endl;
    }
}

TEST(TxnKvTest, ConflictTest) {
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "unit_test";
    std::string val, val1, val2;
    int ret = 0;

    // Historical data
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->put("unit_test", "xxxxxxxxxxxxx");
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    // txn1 begin
    ret = txn_kv->create_txn(&txn1);
    ASSERT_EQ(ret, 0);
    ret = txn1->get(key, &val1);
    ASSERT_EQ(ret, 0);
    std::cout << "val1=" << val1 << std::endl;

    // txn2 begin
    ret = txn_kv->create_txn(&txn2);
    ASSERT_EQ(ret, 0);
    ret = txn2->get(key, &val2);
    ASSERT_EQ(ret, 0);
    std::cout << "val2=" << val2 << std::endl;

    // txn2 commit
    val2 = "zzzzzzzzzzzzzzz";
    txn2->put(key, val2);
    ret = txn2->commit();
    EXPECT_EQ(ret, 0);

    // txn1 commit, intend to fail
    val1 = "yyyyyyyyyyyyyyy";
    txn1->put(key, val1);
    ret = txn1->commit();
    EXPECT_EQ(ret, -1);

    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = txn->get(key, &val);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(val, val2); // First wins
    std::cout << "final val=" << val << std::endl;
}

TEST(TxnKvTest, AtomicAddTest) {
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "counter";
    // clear counter
    int ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    txn->remove(key);
    ret = txn->commit();
    ASSERT_EQ(ret, 0);
    // txn1 atomic add
    ret = txn_kv->create_txn(&txn1);
    ASSERT_EQ(ret, 0);
    txn1->atomic_add(key, 10);
    // txn2 atomic add
    ret = txn_kv->create_txn(&txn2);
    ASSERT_EQ(ret, 0);
    txn2->atomic_add(key, 20);
    // txn1 commit success
    ret = txn1->commit();
    ASSERT_EQ(ret, 0);
    // txn2 commit success
    ret = txn2->commit();
    ASSERT_EQ(ret, 0);
    // Check counter val
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    std::string val;
    ret = txn->get(key, &val);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 30);

    // txn1 atomic add
    ret = txn_kv->create_txn(&txn1);
    ASSERT_EQ(ret, 0);
    txn1->atomic_add(key, 30);
    // txn2 get and put
    ret = txn_kv->create_txn(&txn2);
    ASSERT_EQ(ret, 0);
    ret = txn2->get(key, &val);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 30);
    *(int64_t*)val.data() = 100;
    txn2->put(key, val);
    // txn1 commit success
    ret = txn1->commit();
    ASSERT_EQ(ret, 0);
    // txn2 commit, intend to fail
    ret = txn2->commit();
    ASSERT_EQ(ret, -1);
    // Check counter val
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = txn->get(key, &val);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(val.size(), 8);
    ASSERT_EQ(*(int64_t*)val.data(), 60);
}

TEST(TxnKvTest, CompatibleGetTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    doris::TabletSchemaPB schema;
    schema.set_schema_version(1);
    for (int i = 0; i < 1000; ++i) {
        auto column = schema.add_column();
        column->set_unique_id(i);
        column->set_name("col" + std::to_string(i));
        column->set_type("VARCHAR");
        column->set_aggregation("NONE");
        column->set_length(100);
        column->set_index_length(80);
    }
    std::string instance_id = "compatible_get_test_" + std::to_string(::time(nullptr));
    auto key = meta_schema_key({instance_id, 10005, 1});
    auto val = schema.SerializeAsString();

    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ValueBuf val_buf;
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 1);
    txn->put(key, val);
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    // Check get
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 0);
    EXPECT_EQ(val_buf.version(), 0);
    doris::TabletSchemaPB saved_schema;
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }

    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    val_buf.remove(txn.get());
    ret = txn->commit();
    ASSERT_EQ(ret, 0);
    // Check remove
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 1);
}

TEST(TxnKvTest, PutLargeValueTest) {
    auto txn_kv = std::make_shared<MemTxnKv>();

    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->enable_processing();

    doris::TabletSchemaPB schema;
    schema.set_schema_version(1);
    for (int i = 0; i < 10000; ++i) {
        auto column = schema.add_column();
        column->set_unique_id(i);
        column->set_name("col" + std::to_string(i));
        column->set_type("VARCHAR");
        column->set_aggregation("NONE");
        column->set_length(100);
        column->set_index_length(80);
    }
    std::cout << "value size=" << schema.SerializeAsString().size() << std::endl;

    std::string instance_id = "put_large_value_" + std::to_string(::time(nullptr));
    auto key = meta_schema_key({instance_id, 10005, 1});
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    selectdb::put(txn.get(), key, schema, 1, 100);
    ret = txn->commit();
    ASSERT_EQ(ret, 0);

    // Check get
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ValueBuf val_buf;
    doris::TabletSchemaPB saved_schema;
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 0);
    std::cout << "num iterators=" << val_buf.iters_.size() << std::endl;
    EXPECT_EQ(val_buf.version(), 1);
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }
    // Check multi range get
    sp->set_call_back("memkv::Transaction::get", [](void* limit) { *((int*)limit) = 100; });
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 0);
    std::cout << "num iterators=" << val_buf.iters_.size() << std::endl;
    EXPECT_EQ(val_buf.version(), 1);
    ASSERT_TRUE(val_buf.to_pb(&saved_schema));
    ASSERT_EQ(saved_schema.column_size(), schema.column_size());
    for (size_t i = 0; i < saved_schema.column_size(); ++i) {
        auto& saved_col = saved_schema.column(i);
        auto& col = schema.column(i);
        EXPECT_EQ(saved_col.name(), col.name());
    }
    // Check keys
    auto& iters = val_buf.iters_;
    size_t i = 0;
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    for (auto&& it : iters) {
        it->reset();
        while (it->has_next()) {
            auto [k, _] = it->next();
            k.remove_prefix(1);
            fields.clear();
            ret = decode_key(&k, &fields);
            ASSERT_EQ(ret, 0);
            int64_t* suffix = std::get_if<int64_t>(&std::get<0>(fields.back()));
            ASSERT_TRUE(suffix);
            EXPECT_EQ(*suffix, (1L << 56) + i++);
        }
    }

    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    val_buf.remove(txn.get());
    ret = txn->commit();
    ASSERT_EQ(ret, 0);
    // Check remove
    ret = txn_kv->create_txn(&txn);
    ASSERT_EQ(ret, 0);
    ret = selectdb::get(txn.get(), key, &val_buf);
    ASSERT_EQ(ret, 1);
}

// vim: et tw=100 ts=4 sw=4 cc=80:
