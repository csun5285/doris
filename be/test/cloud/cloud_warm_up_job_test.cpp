#include <gen_cpp/BackendService_types.h>
#include <gtest/gtest.h>

#include "cloud/cloud_warm_up_manager.h"

namespace doris::cloud {

TEST(WarmUpManager, base) {
    EXPECT_TRUE(CloudWarmUpManager::instance()->check_and_set_job_id(1));
    EXPECT_FALSE(CloudWarmUpManager::instance()->check_and_set_job_id(2));
    EXPECT_FALSE(CloudWarmUpManager::instance()->check_and_set_batch_id(2, 0));
    EXPECT_TRUE(CloudWarmUpManager::instance()->check_and_set_batch_id(1, 0));
    std::vector<TJobMeta> job_metas;
    for (int i = 0; i < 10; i++) {
        TJobMeta job_meta;
        job_meta.__set_download_type(TDownloadType::S3);
        job_meta.__set_tablet_ids({0, 1, 2, 3, 4});
        job_metas.push_back(std::move(job_meta));
    }
    CloudWarmUpManager::instance()->add_job(job_metas);
    {
        auto [job_id, batch_id, _pending_job_size, _finish_job_size] =
                CloudWarmUpManager::instance()->get_current_job_state();
        EXPECT_EQ(job_id, 1);
        EXPECT_EQ(batch_id, 0);
        EXPECT_EQ(_pending_job_size, 10);
        EXPECT_EQ(_finish_job_size, 0);
    }
    EXPECT_FALSE(CloudWarmUpManager::instance()->check_and_set_batch_id(1, 1));
    CloudWarmUpManager::instance()->consumer_job();
    {
        auto [job_id, batch_id, _pending_job_size, _finish_job_size] =
                CloudWarmUpManager::instance()->get_current_job_state();
        EXPECT_EQ(job_id, 1);
        EXPECT_EQ(batch_id, 0);
        EXPECT_EQ(_pending_job_size, 9);
        EXPECT_EQ(_finish_job_size, 1);
    }
    for (int i = 0; i < 9; i++) {
        CloudWarmUpManager::instance()->consumer_job();
    }
    {
        auto [job_id, batch_id, _pending_job_size, _finish_job_size] =
                CloudWarmUpManager::instance()->get_current_job_state();
        EXPECT_EQ(job_id, 1);
        EXPECT_EQ(batch_id, 0);
        EXPECT_EQ(_pending_job_size, 0);
        EXPECT_EQ(_finish_job_size, 10);
    }
    EXPECT_TRUE(CloudWarmUpManager::instance()->check_and_set_batch_id(1, 1));
    EXPECT_FALSE(CloudWarmUpManager::instance()->clear_job(2));
    EXPECT_TRUE(CloudWarmUpManager::instance()->clear_job(1));
}

} // namespace doris::cloud
