#include "common/stopwatch.h"

#include <gtest/gtest.h>
#include <chrono>
#include <thread>

using namespace selectdb;

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(StopWatchTest, SimpleTest) {
    {
        StopWatch s;
        s.start();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000);

        s.pause();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 1500);

        s.resume();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 2500);

        s.reset();
        std::this_thread::sleep_for(std::chrono::microseconds(1000));
        ASSERT_TRUE(s.elapsed_us() >= 1000 && s.elapsed_us() < 1500);
    }
}