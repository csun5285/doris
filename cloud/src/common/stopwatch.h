#pragma once
#include <chrono>

namespace selectdb {

class StopWatch {
public:
    StopWatch() {
        running_ = true;
        start_ = std::chrono::steady_clock::now();
    };
    ~StopWatch() = default;

    void start() {
        if (!running_) {
            start_ = std::chrono::steady_clock::now();
            running_ = true;
        }
    }

    void pause() {
        if (running_) {
            elapsed_ = elapsed_ + (std::chrono::steady_clock::now() - start_);
            running_ = false;
        }
    }

    void resume() {
        if (!running_) {
            start_ = std::chrono::steady_clock::now();
            running_ = true;
        }
    }

    void reset() {
        start_ = std::chrono::steady_clock::now();
        elapsed_ = std::chrono::steady_clock::duration {0};
        running_ = true;
    }

    int64_t elapsed_us() const {
        if (!running_) {
            return std::chrono::duration_cast<std::chrono::microseconds>(elapsed_).count();
        }

        auto end = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::microseconds>(elapsed_ + (end - start_))
                .count();
    }

private:
    std::chrono::steady_clock::time_point start_;
    std::chrono::steady_clock::duration elapsed_ {0};
    bool running_ {false};
};

} // namespace selectdb