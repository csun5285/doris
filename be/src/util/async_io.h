#pragma once

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>
#include <atomic>
#include <chrono>
#include <cstddef>

#include "cloud/io/file_reader.h"
#include "cloud/io/file_system.h"
#include "common/logging.h"
#include "olap/olap_define.h"
#include "priority_thread_pool.hpp"
#include "runtime/threadlocal.h"
#include "util/metrics.h"
#include "util/priority_work_stealing_thread_pool.hpp"
#include "util/runtime_profile.h"

namespace doris {

struct AsyncIOCtx {
    int nice;
};

/**
 * Separate task from bthread to pthread, specific for IO task.
 */
class AsyncIO {
public:
    AsyncIO() {
        _local_io_thread_pool = new PriorityThreadPool(config::async_local_io_thread_pool_thread_num,
                                                 config::async_local_io_thread_pool_queue_size, "async_local_io_thread_pool");
        _remote_io_thread_pool = new PriorityThreadPool(config::async_remote_io_thread_pool_thread_num,
                                       config::async_remote_io_thread_pool_queue_size, "async_remote_io_thread_pool");
    }

    ~AsyncIO() {
        SAFE_DELETE(_local_io_thread_pool);
        SAFE_DELETE(_remote_io_thread_pool);
    }

    AsyncIO& operator=(const AsyncIO&) = delete;
    AsyncIO(const AsyncIO&) = delete;

    static AsyncIO& instance() {
        static AsyncIO instance;
        return instance;
    }

    // This function should run on the bthread, and it will put the task into
    // thread_pool and release the bthread_worker at cv.wait. When the task is completed,
    // the bthread will continue to execute.
    static void run_task(const std::function<void()>& fn, io::FileSystemType file_type, io::IOState* state = nullptr) {
        DCHECK(bthread_self() != 0);
        AsyncIOStatisticsTemplete task_stats;
        ++task_stats.task_total;
        {
            SCOPED_RAW_TIMER(&task_stats.total_use_timer_ns);
            bthread::CountdownEvent event;

            AsyncIOCtx* ctx = static_cast<AsyncIOCtx*>(bthread_getspecific(btls_io_ctx_key));
            int nice = 18;
            if (ctx != nullptr) { nice = ctx->nice; }

            auto task_wait_time = std::chrono::steady_clock::now();
            auto task_wake_up_time = std::chrono::steady_clock::now();
            PriorityThreadPool::Task task;
            task.priority = nice;
            task.work_function = [&] {
                task_stats.task_wait_worker_timer_ns =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - task_wait_time).count();
                {
                    SCOPED_RAW_TIMER(&task_stats.task_exec_timer_ns);
                    fn();
                }
                task_wake_up_time = std::chrono::steady_clock::now();
                event.signal();
            };
            auto wait_for_putting_queue = std::chrono::steady_clock::now();
            if (file_type == io::FileSystemType::S3) {
                AsyncIO::instance().remote_io_thread_pool()->offer(task);
            } else {
                AsyncIO::instance().local_io_thread_pool()->offer(task);
            }
            task_stats.wait_for_putting_queue = 
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - wait_for_putting_queue).count(); 
            if (int ec = event.wait(); ec != 0) [[unlikely]] {
                LOG(FATAL) << "Failed to wait for task to complete";
            }
            task_stats.task_wake_up_timer_ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - task_wake_up_time).count();
        }
        if (state && state->stats) {
            if (file_type == io::FileSystemType::S3) {
                state->stats->async_io_stat.remote_total_use_timer_ns += task_stats.total_use_timer_ns;
                state->stats->async_io_stat.remote_task_exec_timer_ns += task_stats.task_exec_timer_ns;
                state->stats->async_io_stat.remote_task_wait_worker_timer_ns += task_stats.task_wait_worker_timer_ns;
                state->stats->async_io_stat.remote_task_wake_up_timer_ns += task_stats.task_wake_up_timer_ns;
                state->stats->async_io_stat.remote_task_total += task_stats.task_total;
                state->stats->async_io_stat.remote_wait_for_putting_queue += task_stats.wait_for_putting_queue;
            } else {
                state->stats->async_io_stat.local_total_use_timer_ns += task_stats.total_use_timer_ns;
                state->stats->async_io_stat.local_task_exec_timer_ns += task_stats.task_exec_timer_ns;
                state->stats->async_io_stat.local_task_wait_worker_timer_ns += task_stats.task_wait_worker_timer_ns;
                state->stats->async_io_stat.local_task_wake_up_timer_ns += task_stats.task_wake_up_timer_ns;
                state->stats->async_io_stat.local_task_total += task_stats.task_total;
                state->stats->async_io_stat.local_wait_for_putting_queue += task_stats.wait_for_putting_queue;
            }
        }

    }

    inline static bthread_key_t btls_io_ctx_key;

    static void io_ctx_key_deleter(void* d) { delete static_cast<AsyncIOCtx*>(d); }

private:
    PriorityThreadPool* _local_io_thread_pool = nullptr;
    PriorityThreadPool* _remote_io_thread_pool = nullptr;

    PriorityThreadPool* local_io_thread_pool() { return _local_io_thread_pool; }
    PriorityThreadPool* remote_io_thread_pool() { return _remote_io_thread_pool; }
    std::atomic<int> _local_queue_id{0};
    std::atomic<int> _remote_queue_id{0};
};

} // end namespace doris
