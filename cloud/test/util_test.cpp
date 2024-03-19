
// clang-format off
#include <chrono>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>
#include "common/config.h"
#include "common/logging.h"
#include "common/util.h"
#include "common/sync_point.h"

#include "gtest/gtest.h"
#include "recycler/util.h"
#include "recycler/recycler.h"
// clang-format on

using namespace selectdb;

int main(int argc, char** argv) {
    auto conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog("util")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

template <typename... Func>
auto task_wrapper(Func... funcs) -> std::function<int()> {
    return [funcs...]() {
        return [](std::initializer_list<int> numbers) {
            int i = 0;
            for (int num : numbers) {
                if (num != 0) {
                    i = num;
                }
            }
            return i;
        }({funcs()...});
    };
}

TEST(UtilTest, stage_wrapper) {
    std::function<int()> func1 = []() { return 0; };
    std::function<int()> func2 = []() { return -1; };
    std::function<int()> func3 = []() { return 0; };
    auto f = task_wrapper(func1, func2, func3);
    ASSERT_EQ(-1, f());

    f = task_wrapper(func1, func3);
    ASSERT_EQ(0, f());
}

TEST(UtilTest, normal) {
    std::unique_ptr<SimpleThreadPool> s3_producer_pool =
            std::make_unique<SimpleThreadPool>(config::recycle_pool_parallelism);
    s3_producer_pool->start();
    // test normal execute
    {
        SyncExecutor<int> sync_executor(s3_producer_pool.get(), "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(3, res.size());
        ASSERT_EQ(finished, true);
        std::for_each(res.begin(), res.end(), [](auto&& n) { ASSERT_EQ(1, n); });
    }
    // test when error happen
    {
        SyncExecutor<int> sync_executor(s3_producer_pool.get(), "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        sync_executor._stop_token = true;
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
        ASSERT_EQ(0, res.size());
    }
    {
        SyncExecutor<int> sync_executor(s3_producer_pool.get(), "normal test",
                                        [](int k) { return k == -1; });
        auto f1 = []() { return 1; };
        auto cancel = []() { return -1; };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(cancel);
        bool finished = true;
        std::vector<int> res = sync_executor.when_all(&finished);
        ASSERT_EQ(finished, false);
    }
    // test string_view
    {
        SyncExecutor<std::string_view> sync_executor(
                s3_producer_pool.get(), "normal test",
                [](const std::string_view k) { return k.empty(); });
        std::string s = "Hello World";
        auto f1 = [&s]() { return std::string_view(s); };
        sync_executor.add(f1);
        sync_executor.add(f1);
        sync_executor.add(f1);
        bool finished = true;
        std::vector<std::string_view> res = sync_executor.when_all(&finished);
        ASSERT_EQ(3, res.size());
        std::for_each(res.begin(), res.end(), [&s](auto&& n) { ASSERT_EQ(s, n); });
    }
}