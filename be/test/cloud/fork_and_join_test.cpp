#include <gtest/gtest.h>

#include "cloud/utils.h"

namespace doris::cloud {

TEST(ForkAndJoinTest, normal) {
    std::vector<std::function<Status()>> tasks;
    std::atomic_int cnt {0};
    for (int i = 1; i <= 100; ++i) {
        tasks.push_back([i, &cnt] {
            bthread_usleep(10000);
            cnt += i;
            return Status::OK();
        });
    }
    ASSERT_TRUE(bthread_fork_and_join(tasks, 8).ok());
    EXPECT_EQ(cnt, 5050);
    cnt = 0;
    ASSERT_TRUE(bthread_fork_and_join(tasks, 23).ok());
    EXPECT_EQ(cnt, 5050);
    cnt = 0;
    ASSERT_TRUE(bthread_fork_and_join(tasks, 99).ok());
    EXPECT_EQ(cnt, 5050);
    cnt = 0;
    ASSERT_TRUE(bthread_fork_and_join(tasks, 100).ok());
    EXPECT_EQ(cnt, 5050);
}

TEST(ForkAndJoinTest, abnormal) {
    std::vector<std::function<Status()>> tasks;
    std::atomic_int cnt {0};
    for (int i = 1; i <= 100; ++i) {
        tasks.push_back([i, &cnt] {
            bthread_usleep(10000);
            cnt += i;
            return i % 11 ? Status::InternalError("error") : Status::OK();
        });
    }
    ASSERT_FALSE(bthread_fork_and_join(tasks, 99).ok());
    EXPECT_EQ(cnt, 5050);
    cnt = 0;
    ASSERT_FALSE(bthread_fork_and_join(tasks, 100).ok());
    EXPECT_EQ(cnt, 5050);
    cnt = 0;
    ASSERT_FALSE(bthread_fork_and_join(tasks, 8).ok());
    EXPECT_LT(cnt, 5050);
    cnt = 0;
    ASSERT_FALSE(bthread_fork_and_join(tasks, 23).ok());
    EXPECT_LT(cnt, 5050);
}

} // namespace doris::cloud
