
// clang-format off
#include "common/config.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-service/meta_service.h"
#include "meta-service/txn_kv.h"

#include "brpc/closure_guard.h"
#include "brpc/controller.h"
#include "gtest/gtest.h"
// clang-format on

int main(int argc, char** argv) {
    selectdb::config::init(nullptr, true);
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(TxnIdConvert, TxnIdTest) {
    using namespace selectdb;

    // Correctness test
    {
        // 00000182a5ed173f0012
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x12", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id0;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id0);
        ASSERT_EQ(ret, 0);
        std::string str((char*)&txn_id0, sizeof(txn_id0));
        std::cout << "fdb_ts0: " << hex(ts) << " "
                  << "txn_id0: " << txn_id0 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed173f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x3f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id1;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id1);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id1, txn_id0);
        str = std::string((char*)&txn_id1, sizeof(txn_id1));
        std::cout << "fdb_ts1: " << hex(ts) << " "
                  << "txn_id1: " << txn_id1 << " hex: " << hex(str) << std::endl;

        // 00000182a5ed174f0013
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x00\x13", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id2;
        ret = get_txn_id_from_fdb_ts(ts, &txn_id2);
        ASSERT_EQ(ret, 0);
        ASSERT_GT(txn_id2, txn_id1);
        str = std::string((char*)&txn_id2, sizeof(txn_id2));
        std::cout << "fdb_ts2: " << hex(ts) << " "
                  << "txn_id2: " << txn_id2 << " hex: " << hex(str) << std::endl;
    }

    // Boundary test
    {
        //                 1024
        // 00000182a5ed174f0400
        std::string ts("\x00\x00\x01\x82\xa5\xed\x17\x4f\x04\x00", 10);
        ASSERT_EQ(ts.size(), 10);
        int64_t txn_id;
        int ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 2); // Exceed max seq

        //                 1023
        // 00000182a5ed174f03ff
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\xff", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        //                 0000
        // 00000182a5ed174f0000
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 10);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 0);

        // Insufficient length
        ts = std::string("\x00\x00\x01\x82\xa5\xed\x17\x4f\x03\x00", 9);
        ret = get_txn_id_from_fdb_ts(ts, &txn_id);
        ASSERT_EQ(ret, 1);
    }
}

TEST(MetaServiceTest, BeginTxnTest) {
    using namespace selectdb;
    config::fdb_cluster_file_path = "fdb.cluster";
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<FdbTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    int ret = txn_kv->init();
    ASSERT_EQ(ret, 0);

    // Add service
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv);
    brpc::Controller cntl;
    BeginTxnRequest req;
    BeginTxnResponse res;
    meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req, &res, nullptr);

    ASSERT_EQ(res.status().code(), MetaServiceCode::INVALID_ARGUMENT_ERR);
}


// vim: et tw=100 ts=4 sw=4 cc=80:
