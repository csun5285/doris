
// clang-format off
#include "common/config.h"
#include "common/util.h"
#include "common/stopwatch.h"
#include "meta-service/txn_kv.h"
#include "meta-service/doris_txn.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"
#include "gen_cpp/selectdb_cloud.pb.h"

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

TEST(TxnKvTest, MoreThan100KTest) {
    {
        int64_t txn_id = 22343212453;
        std::string instance_id = "test_more_than_100k";
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;

        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        InstanceInfoPB ipb;
        for (int i=0; i< 1000; i++) {
            ClusterPB pb;
            for (int j=0; j < 100; j++){
                pb.add_mysql_user_name("test-more-than-100k-value" + std::to_string(j));
            }
            ipb.add_clusters()->MergeFrom(pb);
        }

        std::string value = ipb.SerializeAsString();
        std::cout << "before put value.size=" << value.size() << std::endl;
        selectdb::put(encoded_txn_index_key0, txn.release(), ipb, 100);

        txn_kv->create_txn(&txn);
        InstanceInfoPB ipb1;
        selectdb::get(encoded_txn_index_key0, txn.release(), &ipb1);
        std::string value1 = ipb1.SerializeAsString();
        std::cout << "after get value.size=" << value1.size() << std::endl;
        ASSERT_EQ(value.size(), value1.size());
        ASSERT_EQ(ipb.clusters(0).mysql_user_name(0), "test-more-than-100k-value0");
        ASSERT_EQ(ipb1.clusters(0).mysql_user_name(0), "test-more-than-100k-value0");

        // test remove and get
        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);

        txn_kv->create_txn(&txn);
        InstanceInfoPB ipb3;
        // not found
        ASSERT_EQ(selectdb::get(encoded_txn_index_key0, txn.release(), &ipb3), 1);

        txn_kv->create_txn(&txn);
        std::cout << "before put default value.size=" << value.size() << std::endl;
        selectdb::put(encoded_txn_index_key0, txn.release(), ipb);
        txn_kv->create_txn(&txn);
        InstanceInfoPB ipb5;
        ASSERT_EQ(selectdb::get(encoded_txn_index_key0, txn.release(), &ipb5), 0);
        std::string value2 = ipb5.SerializeAsString();
        std::cout << "after get default value.size=" << value2.size() << std::endl;
        ASSERT_EQ(value.size(), value2.size());
        ASSERT_EQ(ipb5.clusters(0).mysql_user_name(0), "test-more-than-100k-value0");

        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
    }

    // performance
    {
        int64_t txn_id = 32343212453;
        std::string instance_id = "test_more_than_100k_performance";
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;


        InstanceInfoPB ipb;
        for (int i=0; i< 50; i++) {
            ClusterPB pb;
            for (int j=0; j < 50; j++){
                pb.add_mysql_user_name("test-more-than-100k-value" + std::to_string(j));
            }
            ipb.add_clusters()->MergeFrom(pb);
        }

        std::string value = ipb.SerializeAsString();
        // 72150
        std::cout << "performance before put value.size=" << value.size() << std::endl;

        std::unique_ptr<Transaction> txn;
        StopWatch sw;
        const int run_times = 100;
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            // 72150 / 100 = 722 pages
            selectdb::put(encoded_txn_index_key0, txn.release(), ipb, 100);

            txn_kv->create_txn(&txn);
            InstanceInfoPB ipb1;
            selectdb::get(encoded_txn_index_key0, txn.release(), &ipb1);
        }
        std::cout << "multi page run " << run_times << " times put and get use="
                  << sw.elapsed_us() / 1000  << " ms" << std::endl;

        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
        sw.reset();
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            // 72150 / 90*1000 = 1 pages
            selectdb::put(encoded_txn_index_key0, txn.release(), ipb);
            txn_kv->create_txn(&txn);
            InstanceInfoPB ipb3;
            selectdb::get(encoded_txn_index_key0, txn.release(), &ipb3);
        }
        std::cout << "one page run " << run_times <<  " times put and get use="
                  << sw.elapsed_us() / 1000 << " ms" << std::endl;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
    }

    // performance put
    {
        int64_t txn_id = 42343212453;
        std::string instance_id = "test_more_than_100k_performance_put";
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;


        InstanceInfoPB ipb;
        for (int i=0; i< 50; i++) {
            ClusterPB pb;
            for (int j=0; j < 50; j++){
                pb.add_mysql_user_name("test-more-than-100k-value" + std::to_string(j));
            }
            ipb.add_clusters()->MergeFrom(pb);
        }

        std::string value = ipb.SerializeAsString();
        // 72150
        std::cout << "performance_put before put value.size=" << value.size() << std::endl;

        std::unique_ptr<Transaction> txn;
        StopWatch sw;
        const int run_times = 100;
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            // 72150 / 100 = 722 pages
            selectdb::put(encoded_txn_index_key0, txn.release(), ipb, 100);
        }
        std::cout << "multi page run " << run_times << " times put use="
                  << sw.elapsed_us() / 1000  << " ms" << std::endl;

        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
        sw.reset();
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            // 72150 / 90*1000 = 1 pages
            selectdb::put(encoded_txn_index_key0, txn.release(), ipb);
        }
        std::cout << "one page run " << run_times <<  " times put use="
                  << sw.elapsed_us() / 1000 << " ms" << std::endl;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
    }

    // performance get
    {
        int64_t txn_id = 52343212453;
        std::string instance_id = "test_more_than_100k_performance_get";
        TxnIndexKeyInfo txn_index_key_ {instance_id, txn_id};
        std::string encoded_txn_index_key0;
        txn_index_key(txn_index_key_, &encoded_txn_index_key0);
        std::cout << hex(encoded_txn_index_key0) << std::endl;


        InstanceInfoPB ipb;
        for (int i=0; i< 50; i++) {
            ClusterPB pb;
            for (int j=0; j < 50; j++){
                pb.add_mysql_user_name("test-more-than-100k-value" + std::to_string(j));
            }
            ipb.add_clusters()->MergeFrom(pb);
        }

        std::string value = ipb.SerializeAsString();
        // 72150
        std::cout << "performance_get before put value.size=" << value.size() << std::endl;

        std::unique_ptr<Transaction> txn;
        txn_kv->create_txn(&txn);
        // 72150 / 100 = 722 pages
        selectdb::put(encoded_txn_index_key0, txn.release(), ipb, 100);
        StopWatch sw;
        const int run_times = 100;
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            InstanceInfoPB ipb1;
            selectdb::get(encoded_txn_index_key0, txn.release(), &ipb1);
        }
        std::cout << "multi page run " << run_times << " times get use="
                  << sw.elapsed_us() / 1000  << " ms" << std::endl;

        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
        txn_kv->create_txn(&txn);
        // 72150 / 90*1000 = 1 pages
        selectdb::put(encoded_txn_index_key0, txn.release(), ipb);
        sw.reset();
        for (int i=0; i < run_times; i++) {
            txn_kv->create_txn(&txn);
            InstanceInfoPB ipb3;
            selectdb::get(encoded_txn_index_key0, txn.release(), &ipb3);
        }
        std::cout << "one page run " << run_times <<  " times get use="
                  << sw.elapsed_us() / 1000 << " ms" << std::endl;
        txn_kv->create_txn(&txn);
        ASSERT_EQ(selectdb::remove(encoded_txn_index_key0, txn.release()), 0);
    }

}

// vim: et tw=100 ts=4 sw=4 cc=80:
