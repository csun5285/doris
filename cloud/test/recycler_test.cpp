
#include "recycler/recycler.h"

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>
#include <gtest/gtest.h>

#include <chrono>
#include <memory>
#include <string>
#include <thread>

#include "common/config.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_service.h"
#include "mock_resource_manager.h"
#include "rate-limiter/rate_limiter.h"

static const std::string instance_id = "instance_id_recycle_test";
static constexpr int64_t table_id = 10086;
static int64_t current_time = 0;
static int64_t cnt = 0;

int main(int argc, char** argv) {
    auto conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog("recycler")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    using namespace std::chrono;
    current_time = duration_cast<seconds>(system_clock::now().time_since_epoch()).count();

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {

TEST(RecyclerTest, whitelist) {
    Recycler::InstanceFilter filter;
    filter.reset("", "");
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_FALSE(filter.filter_out("instance2"));
    filter.reset("", "instance1,instance2");
    EXPECT_TRUE(filter.filter_out("instance1"));
    EXPECT_TRUE(filter.filter_out("instance2"));
    EXPECT_FALSE(filter.filter_out("instance3"));
    filter.reset("instance1,instance2", "");
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_FALSE(filter.filter_out("instance2"));
    EXPECT_TRUE(filter.filter_out("instance3"));
    filter.reset("instance1", "instance1"); // whitelist overrides blacklist
    EXPECT_FALSE(filter.filter_out("instance1"));
    EXPECT_TRUE(filter.filter_out("instance2"));
}

static int create_prepared_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                                  const std::string& resource_id, int64_t tablet_id,
                                  int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    RecycleRowsetKeyInfo key_info {instance_id, tablet_id, rowset_id};
    recycle_rowset_key(key_info, &key);

    RecycleRowsetPB rowset_pb;
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_tmp_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                             const std::string& resource_id, int64_t txn_id, int64_t tablet_id,
                             int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    MetaRowsetTmpKeyInfo key_info {instance_id, txn_id, tablet_id};
    meta_rowset_tmp_key(key_info, &key);

    doris::RowsetMetaPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_committed_rowset(TxnKv* txn_kv, ObjStoreAccessor* accessor,
                                   const std::string& resource_id, int64_t tablet_id,
                                   int num_segments = 1) {
    ++cnt;

    std::string key;
    std::string val;

    char rowset_id[50];
    snprintf(rowset_id, sizeof(rowset_id), "%048ld", cnt);

    MetaRowsetKeyInfo key_info {instance_id, tablet_id, cnt};
    meta_rowset_key(key_info, &key);

    doris::RowsetMetaPB rowset_pb;
    rowset_pb.set_rowset_id(0); // useless but required
    rowset_pb.set_rowset_id_v2(rowset_id);
    rowset_pb.set_num_segments(num_segments);
    rowset_pb.set_tablet_id(tablet_id);
    rowset_pb.set_resource_id(resource_id);
    rowset_pb.set_creation_time(current_time);
    rowset_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }

    for (int i = 0; i < num_segments; ++i) {
        auto path = fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, i);
        accessor->put_object(path, path);
    }
    return 0;
}

static int create_tablet(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t index_id,
                         int64_t partition_id, int64_t tablet_id) {
    std::string key;
    std::string val;

    MetaTabletKeyInfo key_info {instance_id, table_id, index_id, partition_id, tablet_id};
    meta_tablet_key(key_info, &key);

    doris::TabletMetaPB tablet_pb;
    tablet_pb.set_index_id(index_id);
    tablet_pb.set_partition_id(partition_id);
    tablet_pb.set_tablet_id(tablet_id);
    tablet_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int create_prepared_partiton(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t partition_id,
                                    const std::vector<int64_t>& index_ids) {
    std::string key;
    std::string val;

    RecyclePartKeyInfo key_info {instance_id, partition_id};
    recycle_partition_key(key_info, &key);

    RecyclePartitionPB partition_pb;

    partition_pb.set_table_id(table_id);
    for (auto index_id : index_ids) {
        partition_pb.add_index_id(index_id);
    }
    partition_pb.set_creation_time(current_time);
    partition_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int create_prepared_index(TxnKv* txn_kv, ObjStoreAccessor* accessor, int64_t index_id) {
    std::string key;
    std::string val;

    RecycleIndexKeyInfo key_info {instance_id, index_id};
    recycle_index_key(key_info, &key);

    RecycleIndexPB index_pb;

    index_pb.set_table_id(table_id);
    index_pb.set_creation_time(current_time);
    index_pb.SerializeToString(&val);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int get_txn_info(std::shared_ptr<TxnKv> txn_kv, std::string instance_id, int64_t db_id,
                        int64_t txn_id, TxnInfoPB& txn_info_pb) {
    std::string txn_inf_key;
    std::string txn_inf_val;
    TxnInfoKeyInfo txn_inf_key_info {instance_id, db_id, txn_id};

    LOG(INFO) << instance_id << "|" << db_id << "|" << txn_id;

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn_info_key(txn_inf_key_info, &txn_inf_key);
    LOG(INFO) << "txn_inf_key:" << hex(txn_inf_key);
    int ret = txn->get(txn_inf_key, &txn_inf_val);
    if (ret != 0) {
        LOG(WARNING) << "txn->get failed, ret=" << ret;
        return -2;
    }

    if (!txn_info_pb.ParseFromString(txn_inf_val)) {
        LOG(WARNING) << "ParseFromString failed";
        return -3;
    }
    LOG(INFO) << "txn_info_pb" << txn_info_pb.DebugString();
    if (txn->commit() != 0) {
        LOG(WARNING) << "txn->commit failed, ret=" << ret;
        return -4;
    }
    return 0;
}

static int create_instance(const std::string& internal_stage_id,
                           const std::string& external_stage_id, InstanceInfoPB& instance_info) {
    // create internal stage
    {
        std::string s3_prefix = "internal_prefix";
        std::string stage_prefix = fmt::format("{}/stage/root/{}/", s3_prefix, internal_stage_id);
        ObjectStoreInfoPB object_info;
        object_info.set_id(internal_stage_id); // test use accessor_map_ in recycle
        object_info.set_ak("ak");
        object_info.set_sk("sk");
        object_info.set_bucket("bucket");
        object_info.set_endpoint("endpoint");
        object_info.set_region("region");
        // in real case, this is {s3_prefix}
        object_info.set_prefix(stage_prefix);
        object_info.set_provider(ObjectStoreInfoPB::OSS);

        StagePB internal_stage;
        internal_stage.set_type(StagePB::INTERNAL);
        internal_stage.set_stage_id(internal_stage_id);
        ObjectStoreInfoPB internal_object_info;
        internal_object_info.set_prefix(stage_prefix);
        internal_object_info.set_id("0");
        internal_stage.mutable_obj_info()->CopyFrom(internal_object_info);

        instance_info.add_obj_info()->CopyFrom(object_info);
        instance_info.add_stages()->CopyFrom(internal_stage);
    }

    // create external stage
    {
        ObjectStoreInfoPB object_info;
        object_info.set_id(external_stage_id);
        object_info.set_ak("ak1");
        object_info.set_sk("sk1");
        object_info.set_bucket("bucket1");
        object_info.set_endpoint("endpoint1");
        object_info.set_region("region1");
        object_info.set_prefix("external_prefix");
        object_info.set_provider(ObjectStoreInfoPB::OSS);

        StagePB external_stage;
        external_stage.set_type(StagePB::EXTERNAL);
        external_stage.set_stage_id(external_stage_id);
        external_stage.mutable_obj_info()->CopyFrom(object_info);

        instance_info.add_obj_info()->CopyFrom(object_info);
        instance_info.add_stages()->CopyFrom(external_stage);
    }

    instance_info.set_instance_id(instance_id);
    return 0;
}

static int create_copy_job(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                           StagePB::StageType stage_type, CopyJobPB::JobStatus job_status,
                           std::vector<ObjectFilePB> object_files, int64_t timeout_time,
                           int64_t start_time = 0, int64_t finish_time = 0) {
    std::string key;
    std::string val;
    CopyJobKeyInfo key_info {instance_id, stage_id, table_id, "copy_id", 0};
    copy_job_key(key_info, &key);

    CopyJobPB copy_job;
    copy_job.set_stage_type(stage_type);
    copy_job.set_job_status(job_status);
    copy_job.set_timeout_time_ms(timeout_time);
    if (start_time != 0) {
        copy_job.set_start_time_ms(start_time);
    }
    if (finish_time != 0) {
        copy_job.set_finish_time_ms(finish_time);
    }
    for (const auto& file : object_files) {
        copy_job.add_object_files()->CopyFrom(file);
    }
    copy_job.SerializeToString(&val);

    std::vector<std::string> file_keys;
    std::string file_val;
    CopyFilePB copy_file;
    copy_file.set_copy_id("copy_id");
    copy_file.set_group_id(0);
    file_val = copy_file.SerializeAsString();

    // create job files
    for (const auto& file : object_files) {
        CopyFileKeyInfo file_info {instance_id, stage_id, table_id, file.relative_path(),
                                   file.etag()};
        std::string file_key;
        copy_file_key(file_info, &file_key);
        file_keys.push_back(file_key);
    }

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    txn->put(key, val);
    for (const auto& file_key : file_keys) {
        txn->put(file_key, file_val);
    }
    if (txn->commit() != 0) {
        return -1;
    }
    return 0;
}

static int copy_job_exists(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                           bool* exist) {
    std::string key;
    std::string val;
    CopyJobKeyInfo key_info {instance_id, stage_id, table_id, "copy_id", 0};
    copy_job_key(key_info, &key);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    auto ret = txn->get(key, &val);
    if (ret < 0) {
        return ret;
    }
    *exist = ret == 0;
    return 0;
}

static int create_object_files(ObjStoreAccessor* accessor,
                               std::vector<ObjectFilePB>* object_files) {
    for (auto& file : *object_files) {
        auto key = file.relative_path();
        if (accessor->put_object(key, key) != 0) {
            return -1;
        }
        file.set_etag("");
    }
    return 0;
}

static int get_copy_file_num(TxnKv* txn_kv, const std::string& stage_id, int64_t table_id,
                             int* file_num) {
    *file_num = 0;
    std::string key0;
    std::string key1;
    CopyFileKeyInfo key_info0 {instance_id, stage_id, table_id, "", ""};
    CopyFileKeyInfo key_info1 {instance_id, stage_id, table_id + 1, "", ""};
    copy_file_key(key_info0, &key0);
    copy_file_key(key_info1, &key1);

    std::unique_ptr<Transaction> txn;
    if (txn_kv->create_txn(&txn) != 0) {
        return -1;
    }
    std::unique_ptr<RangeGetIterator> it;
    do {
        if (txn->get(key0, key1, &it) != 0) {
            return -1;
        }
        while (it->has_next()) {
            it->next();
            ++(*file_num);
        }
        key0.push_back('\x00');
    } while (it->more());
    return 0;
}

TEST(RecyclerTest, recycle_empty) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_empty");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_empty");

    InstanceRecycler recycler(txn_kv, instance);

    recycler.recycle_rowsets();
}

TEST(RecyclerTest, recycle_rowsets) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_rowsets");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_rowsets", 10010, 5);
    }

    recycler.recycle_rowsets();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tmp_rowsets) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tmp_rowsets");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tmp_rowsets");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    int64_t txn_id_base = 114115;
    int64_t tablet_id_base = 10015;
    for (int i = 0; i < 100; ++i) {
        for (int j = 0; j < 5; ++j) {
            create_tmp_rowset(txn_kv.get(), accessor.get(), "recycle_tmp_rowsets", txn_id_base + i,
                              tablet_id_base + j, 5);
        }
    }

    recycler.recycle_tmp_rowsets();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    for (auto& path : existed_segments) {
        std::cout << path << std::endl;
    }
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_tablet) {
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_tablet");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_tablet");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    create_tablet(txn_kv.get(), accessor.get(), 20010, 30010, 10020);
    for (int i = 0; i < 500; ++i) {
        create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }
    for (int i = 0; i < 500; ++i) {
        create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_tablet", 10020);
    }

    ASSERT_EQ(0, recycler.recycle_tablet(10020));

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_indexes) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_indexes");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_indexes");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    create_prepared_index(txn_kv.get(), accessor.get(), 20015);
    int64_t tablet_id_base = 10100;
    for (int i = 0; i < 100; ++i) {
        int64_t tablet_id = tablet_id_base + i;
        create_tablet(txn_kv.get(), accessor.get(), 20015, 30015, tablet_id);
        for (int j = 0; j < 10; ++j) {
            create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id);
        }
        for (int j = 0; j < 10; ++j) {
            create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_indexes", tablet_id);
        }
    }

    recycler.recycle_indexes();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, recycle_partitions) {
    config::retention_seconds = 0;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    InstanceInfoPB instance;
    instance.set_instance_id(instance_id);
    auto obj_info = instance.add_obj_info();
    obj_info->set_id("recycle_partitions");
    obj_info->set_ak(config::test_s3_ak);
    obj_info->set_sk(config::test_s3_sk);
    obj_info->set_endpoint(config::test_s3_endpoint);
    obj_info->set_region(config::test_s3_region);
    obj_info->set_bucket(config::test_s3_bucket);
    obj_info->set_prefix("recycle_partitions");

    InstanceRecycler recycler(txn_kv, instance);

    auto accessor = recycler.accessor_map_.begin()->second;
    std::vector<int64_t> index_ids {20200, 20201, 20202, 20203, 20204};
    create_prepared_partiton(txn_kv.get(), accessor.get(), 30020, index_ids);
    int64_t tablet_id_base = 10100;
    for (auto index_id : index_ids) {
        for (int i = 0; i < 20; ++i) {
            int64_t tablet_id = tablet_id_base + i;
            create_tablet(txn_kv.get(), accessor.get(), index_id, 30020, tablet_id);
            for (int j = 0; j < 10; ++j) {
                create_prepared_rowset(txn_kv.get(), accessor.get(), "recycle_partitions",
                                       tablet_id);
            }
            for (int j = 0; j < 10; ++j) {
                create_committed_rowset(txn_kv.get(), accessor.get(), "recycle_partitions",
                                        tablet_id);
            }
        }
    }

    recycler.recycle_partitions();

    // check rowset does not exist on s3
    std::vector<std::string> existed_segments;
    ASSERT_EQ(0, accessor->list("data/", &existed_segments));
    ASSERT_TRUE(existed_segments.empty());
}

TEST(RecyclerTest, abort_timeout_txn) {
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 666;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("abort_timeout_txn");
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance);
    sleep(1);
    recycler.abort_timeout_txn();
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);
}

TEST(RecyclerTest, recycle_expired_txn_label) {
    config::label_keep_max_second = 0;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    auto rs = std::make_shared<MockResourceManager>(txn_kv);
    auto rl = std::make_shared<RateLimiter>();
    auto meta_service = std::make_unique<MetaServiceImpl>(txn_kv, rs, rl);
    ASSERT_EQ(txn_kv->init(), 0);

    int64_t db_id = 888;
    int64_t table_id = 1234;
    int64_t txn_id = -1;
    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id2");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("recycle_expired_txn_label");
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    InstanceRecycler recycler(txn_kv, instance);
    sleep(1);
    recycler.abort_timeout_txn();
    TxnInfoPB txn_info_pb;
    get_txn_info(txn_kv, mock_instance, db_id, txn_id, txn_info_pb);
    ASSERT_EQ(txn_info_pb.status(), TxnStatusPB::TXN_STATUS_ABORTED);

    {
        brpc::Controller cntl;
        BeginTxnRequest req;

        req.set_cloud_unique_id("test_cloud_unique_id3");
        TxnInfoPB txn_info_pb;
        txn_info_pb.set_db_id(db_id);
        txn_info_pb.set_label("recycle_expired_txn_label");
        txn_info_pb.add_table_ids(table_id);
        txn_info_pb.set_timeout_ms(1);
        req.mutable_txn_info()->CopyFrom(txn_info_pb);
        BeginTxnResponse res;
        meta_service->begin_txn(reinterpret_cast<::google::protobuf::RpcController*>(&cntl), &req,
                                &res, nullptr);
        txn_id = res.txn_id();
        ASSERT_GT(txn_id, -1);
        ASSERT_EQ(res.status().code(), MetaServiceCode::OK);
    }
}

void create_object_file_pb(std::string prefix, std::vector<ObjectFilePB>* object_files) {
    for (int i = 0; i < 10; ++i) {
        ObjectFilePB object_file;
        // create object in S3, pay attention to the relative path
        object_file.set_relative_path(prefix + "/obj_" + std::to_string(i));
        object_file.set_etag("");
        object_files->push_back(object_file);
    }
}

TEST(RecyclerTest, recycle_copy_jobs) {
    using namespace std::chrono;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);

    // create internal/external stage
    std::string internal_stage_id = "internal";
    std::string external_stage_id = "external";
    std::string nonexist_internal_stage_id = "non_exist_internal";
    std::string nonexist_external_stage_id = "non_exist_external";

    InstanceInfoPB instance_info;
    create_instance(internal_stage_id, external_stage_id, instance_info);
    InstanceRecycler recycler(txn_kv, instance_info);
    auto internal_accessor = recycler.accessor_map_.find(internal_stage_id)->second;

    // create internal stage copy job with finish status
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("0", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 0, StagePB::INTERNAL, CopyJobPB::FINISH,
                        object_files, 0);
    }
    // create internal stage copy job and files with loading status which is timeout
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("5", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 5, StagePB::INTERNAL, CopyJobPB::LOADING,
                        object_files, 0);
    }
    // create internal stage copy job and files with loading status which is not timeout
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("6", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        create_copy_job(txn_kv.get(), internal_stage_id, 6, StagePB::INTERNAL, CopyJobPB::LOADING,
                        object_files, 9963904963479L);
    }
    // create internal stage copy job with deleted stage id
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb("8", &object_files);
        ASSERT_EQ(create_object_files(internal_accessor.get(), &object_files), 0);
        ASSERT_EQ(0, create_copy_job(txn_kv.get(), nonexist_internal_stage_id, 8, StagePB::INTERNAL,
                                     CopyJobPB::FINISH, object_files, 0));
    }
    // ----- external stage ----
    // <table_id, timeout_time, start_time, finish_time, job_status>
    std::vector<std::tuple<int, int64_t, int64_t, int64_t, CopyJobPB::JobStatus>>
            external_copy_jobs;
    uint64_t current_time =
            duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    int64_t expire_time = current_time - config::copy_job_max_retention_second * 1000 - 1000;
    int64_t not_expire_time = current_time - config::copy_job_max_retention_second * 1000 / 2;
    // create external stage copy job with start time not expired and no finish time
    external_copy_jobs.emplace_back(1, 0, 9963904963479L, 0, CopyJobPB::FINISH);
    // create external stage copy job with start time expired and no finish time
    external_copy_jobs.emplace_back(2, 0, expire_time, 0, CopyJobPB::FINISH);
    // create external stage copy job with start time not expired and finish time not expired
    external_copy_jobs.emplace_back(9, 0, expire_time, not_expire_time, CopyJobPB::FINISH);
    // create external stage copy job with start time expired and finish time expired
    external_copy_jobs.emplace_back(10, 0, expire_time, expire_time + 1, CopyJobPB::FINISH);
    // create external stage copy job and files with loading status which is timeout
    external_copy_jobs.emplace_back(3, 0, 0, 0, CopyJobPB::LOADING);
    // create external stage copy job and files with loading status which is not timeout
    external_copy_jobs.emplace_back(4, 9963904963479L, 0, 0, CopyJobPB::LOADING);
    for (const auto& [table_id, timeout_time, start_time, finish_time, job_status] :
         external_copy_jobs) {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb(external_stage_id + "_" + std::to_string(table_id), &object_files);
        create_copy_job(txn_kv.get(), external_stage_id, table_id, StagePB::EXTERNAL, job_status,
                        object_files, timeout_time, start_time, finish_time);
    }
    // create external stage copy job with deleted stage id
    {
        std::vector<ObjectFilePB> object_files;
        create_object_file_pb(nonexist_external_stage_id + "_7", &object_files);
        ASSERT_EQ(0, create_copy_job(txn_kv.get(), nonexist_external_stage_id, 7, StagePB::EXTERNAL,
                                     CopyJobPB::FINISH, object_files, 0));
    }
    {
        // <stage_id, table_id>
        std::vector<std::tuple<std::string, int>> stage_table_files;
        stage_table_files.emplace_back(internal_stage_id, 0);
        stage_table_files.emplace_back(nonexist_internal_stage_id, 8);
        stage_table_files.emplace_back(external_stage_id, 1);
        stage_table_files.emplace_back(external_stage_id, 2);
        stage_table_files.emplace_back(external_stage_id, 9);
        stage_table_files.emplace_back(external_stage_id, 10);
        stage_table_files.emplace_back(external_stage_id, 3);
        stage_table_files.emplace_back(external_stage_id, 4);
        stage_table_files.emplace_back(external_stage_id, 9);
        stage_table_files.emplace_back(nonexist_external_stage_id, 7);
        // check copy files
        for (const auto& [stage_id, table_id] : stage_table_files) {
            int file_num = 0;
            ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num));
            ASSERT_EQ(10, file_num);
        }
    }

    recycler.recycle_copy_jobs();

    // check object files
    std::vector<std::tuple<std::shared_ptr<ObjStoreAccessor>, std::string, int>>
            prefix_and_files_list;
    prefix_and_files_list.emplace_back(internal_accessor, "0/", 0);
    prefix_and_files_list.emplace_back(internal_accessor, "5/", 10);
    prefix_and_files_list.emplace_back(internal_accessor, "6/", 10);
    prefix_and_files_list.emplace_back(internal_accessor, "8/", 10);
    for (const auto& [accessor, relative_path, file_num] : prefix_and_files_list) {
        std::vector<std::string> object_files;
        ASSERT_EQ(0, accessor->list(relative_path, &object_files));
        ASSERT_EQ(file_num, object_files.size());
    }

    // check fdb kvs
    // <stage_id, table_id, expected_files, expected_job_exists>
    std::vector<std::tuple<std::string, int, int, bool>> stage_table_files;
    stage_table_files.emplace_back(internal_stage_id, 0, 0, false);
    stage_table_files.emplace_back(nonexist_internal_stage_id, 8, 0, false);
    stage_table_files.emplace_back(internal_stage_id, 5, 0, false);
    stage_table_files.emplace_back(internal_stage_id, 6, 10, true);
    stage_table_files.emplace_back(external_stage_id, 1, 10, true);
    stage_table_files.emplace_back(external_stage_id, 2, 0, false);
    stage_table_files.emplace_back(external_stage_id, 9, 10, true);
    stage_table_files.emplace_back(external_stage_id, 10, 0, false);
    stage_table_files.emplace_back(external_stage_id, 3, 0, false);
    stage_table_files.emplace_back(external_stage_id, 4, 10, true);
    stage_table_files.emplace_back(nonexist_external_stage_id, 7, 0, false);
    for (const auto& [stage_id, table_id, files, expected_job_exists] : stage_table_files) {
        // std::cout << "table_id=" << table_id << std::endl;
        // check copy files
        int file_num = 0;
        ASSERT_EQ(0, get_copy_file_num(txn_kv.get(), stage_id, table_id, &file_num));
        ASSERT_EQ(files, file_num);
        // check copy jobs
        bool exist = false;
        ASSERT_EQ(0, copy_job_exists(txn_kv.get(), stage_id, table_id, &exist));
        ASSERT_EQ(expected_job_exists, exist);
    }
}

TEST(RecyclerTest, recycle_stage) {
    [[maybe_unused]] auto sp = SyncPoint::get_instance();
    sp->clear_all_call_backs();
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    ASSERT_EQ(txn_kv->init(), 0);

    std::string stage_prefix = "prefix/stage/bob/bc9fff5e-5f91-4168-8eaa-0afd6667f7ef";
    ObjectStoreInfoPB object_info;
    object_info.set_id("obj_id");
    object_info.set_ak("ak");
    object_info.set_sk("sk");
    object_info.set_bucket("bucket");
    object_info.set_endpoint("endpoint");
    object_info.set_region("region");
    object_info.set_prefix(stage_prefix);
    object_info.set_provider(ObjectStoreInfoPB::OSS);
    InstanceInfoPB instance;
    instance.set_instance_id(mock_instance);
    instance.add_obj_info()->CopyFrom(object_info);

    InstanceRecycler recycler(txn_kv, instance);
    auto accessor = recycler.accessor_map_.begin()->second;
    for (int i = 0; i < 10; ++i) {
        accessor->put_object(std::to_string(i) + ".csv", "abc");
    }
    sp->set_call_back("recycle_stage:get_accessor", [&recycler](void* ret) {
        *reinterpret_cast<std::shared_ptr<ObjStoreAccessor>*>(ret) =
                recycler.accessor_map_.begin()->second;
    });
    sp->enable_processing();

    std::string key;
    std::string val;
    RecycleStageKeyInfo key_info {mock_instance, "stage_id"};
    recycle_stage_key(key_info, &key);
    StagePB stage;
    stage.add_mysql_user_name("user_name");
    stage.add_mysql_user_id("user_id");
    stage.mutable_obj_info()->set_id("1");
    stage.mutable_obj_info()->set_prefix(stage_prefix);
    RecycleStagePB recycle_stage;
    recycle_stage.set_instance_id(mock_instance);
    recycle_stage.mutable_stage()->CopyFrom(stage);
    val = recycle_stage.SerializeAsString();
    std::unique_ptr<Transaction> txn;
    ASSERT_EQ(0, txn_kv->create_txn(&txn));
    txn->put(key, val);
    ASSERT_EQ(0, txn->commit());
    ASSERT_EQ(0, txn_kv->create_txn(&txn));
    ASSERT_EQ(0, txn->get(key, &val));

    // recycle stage
    recycler.recycle_stage();
    std::vector<std::string> files;
    ASSERT_EQ(0, accessor->list("", &files));
    ASSERT_EQ(0, files.size());
    ASSERT_EQ(0, txn_kv->create_txn(&txn));
    ASSERT_EQ(1, txn->get(key, &val));
}

TEST(RecyclerTest, multi_recycler) {
    config::recycle_concurrency = 2;
    config::recycle_interval_seconds = 2;
    config::recycle_job_lease_expired_ms = 3;
    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);
    {
        for (int i = 0; i < 10; ++i) {
            InstanceInfoPB instance;
            instance.set_instance_id(std::to_string(i));
            auto obj_info = instance.add_obj_info();
            obj_info->set_id("multi_recycler_test");
            obj_info->set_ak(config::test_s3_ak);
            obj_info->set_sk(config::test_s3_sk);
            obj_info->set_endpoint(config::test_s3_endpoint);
            obj_info->set_region(config::test_s3_region);
            obj_info->set_bucket(config::test_s3_bucket);
            obj_info->set_prefix("multi_recycler_test");
            InstanceKeyInfo key_info {std::to_string(i)};
            std::string key;
            instance_key(key_info, &key);
            std::string val = instance.SerializeAsString();
            std::unique_ptr<Transaction> txn;
            ASSERT_EQ(0, mem_kv->create_txn(&txn));
            txn->put(key, val);
            ASSERT_EQ(0, txn->commit());
        }
    }
    Recycler r1;
    r1.txn_kv_ = mem_kv;
    r1.ip_port_ = "r1:p1";
    r1.start(false);
    Recycler r2;
    r2.txn_kv_ = mem_kv;
    r2.ip_port_ = "r2:p2";
    r2.start(false);

    std::this_thread::sleep_for(std::chrono::seconds(5));
    r1.join();
    r2.join();

    for (int i = 0; i < 10; ++i) {
        JobRecycleKeyInfo key_info {std::to_string(i)};
        JobRecyclePB job_info;
        std::string key;
        std::string val;
        job_recycle_key(key_info, &key);
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(0, mem_kv->create_txn(&txn));
        int ret = txn->get(key, &val);
        if (ret == 0) {
            ASSERT_EQ(true, job_info.ParseFromString(val));
        }
        ASSERT_EQ(JobRecyclePB::IDLE, job_info.status());
        std::cout << "host: " << job_info.ip_port() << " finish recycle job of instance_id: " << i
                  << std::endl;
    }
}

} // namespace selectdb
