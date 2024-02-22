#pragma once
#include <bthread/countdown_event.h>
#include <fmt/core.h>

#include <atomic>
#include <ctime>
#include <functional>
#include <future>
#include <memory>
#include <string>

#include "common/simple_thread_pool.h"
#include "gen_cpp/selectdb_cloud.pb.h"

namespace selectdb {
class TxnKv;

template <typename T>
class SyncExecutor {
public:
    SyncExecutor(
            SimpleThreadPool* pool, std::string name_tag,
            std::function<bool(const T&)> cancel = [](const T& /**/) { return false; })
            : _pool(pool), _cancel(std::move(cancel)), _name_tag(std::move(name_tag)) {}
    auto add(std::function<T()> callback) -> SyncExecutor<T>& {
        auto task = std::make_unique<Task>(std::move(callback), _cancel, _count);
        _count.add_count();
        // The actual task logic would be wrapped by one promise and passed to the threadpool.
        // The result would be returned by the future once the task is finished.
        // Or the task would be invalid if the whole task is cancelled.
        int r = _pool->submit([this, t = task.get()]() { (*t)(_stop_token); });
        CHECK(r == 0);
        _res.emplace_back(std::move(task));
        return *this;
    }
    std::vector<T> when_all(bool* finished) {
        timespec current_time;
        auto current_time_second = time(nullptr);
        current_time.tv_sec = current_time_second + 300;
        current_time.tv_nsec = 0;
        auto msg = fmt::format("{} has already taken 5 min", _name_tag);
        while (0 != _count.timed_wait(current_time)) {
            current_time.tv_sec += 300;
            LOG(WARNING) << msg;
        }
        *finished = !_stop_token;
        std::vector<T> res;
        res.reserve(_res.size());
        for (auto& task : _res) {
            if (!task->valid()) {
                *finished = false;
                return res;
            }
            res.emplace_back((*task).get());
        }
        return res;
    }
    void reset() {
        _res.clear();
        _stop_token = false;
    }

private:
    class Task {
    public:
        Task(std::function<T()> callback, std::function<bool(const T&)> cancel,
             bthread::CountdownEvent& count)
                : _callback(std::move(callback)),
                  _cancel(std::move(cancel)),
                  _count(count),
                  _fut(_pro.get_future()) {}
        void operator()(std::atomic_bool& stop_token) {
            std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                                  [&](int*) { _count.signal(); });
            if (stop_token) {
                _valid = false;
                return;
            }
            T t = _callback();
            // We'll return this task result to user even if this task return error
            // So we don't set _valid to false here
            if (_cancel(t)) {
                stop_token = true;
            }
            _pro.set_value(std::move(t));
        }
        bool valid() { return _valid; }
        T get() { return _fut.get(); }

    private:
        // It's guarantted that the valid function can only be called inside SyncExecutor's `when_all()` function
        // and only be called when the _count.timed_wait function returned. So there would be no data race for
        // _valid then it doesn't need to be one atomic bool.
        bool _valid = true;
        std::function<T()> _callback;
        std::function<bool(const T&)> _cancel;
        std::promise<T> _pro;
        bthread::CountdownEvent& _count;
        std::future<T> _fut;
    };
    std::vector<std::unique_ptr<Task>> _res;
    // use CountdownEvent to do periodically log using CountdownEvent::time_wait()
    bthread::CountdownEvent _count {0};
    std::atomic_bool _stop_token {false};
    SimpleThreadPool* _pool;
    std::function<bool(const T&)> _cancel;
    std::string _name_tag;
};

/**
 * Get all instances, include DELETED instance
 * @return 0 for success, otherwise error
 */
int get_all_instances(TxnKv* txn_kv, std::vector<InstanceInfoPB>& res);

/**
 *
 * @return 0 for success
 */
int prepare_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 int64_t interval_ms);

void finish_instance_recycle_job(TxnKv* txn_kv, std::string_view key,
                                 const std::string& instance_id, const std::string& ip_port,
                                 bool success, int64_t ctime_ms);

/**
 *
 * @return 0 for success, 1 if job should be aborted, negative for other errors
 */
int lease_instance_recycle_job(TxnKv* txn_kv, std::string_view key, const std::string& instance_id,
                               const std::string& ip_port);

inline std::string segment_path(int64_t tablet_id, const std::string& rowset_id,
                                int64_t segment_id) {
    return fmt::format("data/{}/{}_{}.dat", tablet_id, rowset_id, segment_id);
}

inline std::string inverted_index_path(int64_t tablet_id, const std::string& rowset_id,
                                       int64_t segment_id, int64_t index_id) {
    return fmt::format("data/{}/{}_{}_{}.idx", tablet_id, rowset_id, segment_id, index_id);
}

inline std::string rowset_path_prefix(int64_t tablet_id, const std::string& rowset_id) {
    return fmt::format("data/{}/{}_", tablet_id, rowset_id);
}

inline std::string tablet_path_prefix(int64_t tablet_id) {
    return fmt::format("data/{}/", tablet_id);
}

} // namespace selectdb
