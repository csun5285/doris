// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <bthread/bthread.h>
#include <bthread/countdown_event.h>

#include <atomic>
#include <chrono>
#include <cstddef>

#include "common/logging.h"
#include "common/signal_handler.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "olap/olap_define.h"
#include "runtime/threadlocal.h"
#include "util/metrics.h"
#include "util/runtime_profile.h"
#include "work_thread_pool.hpp"

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
        _local_io_thread_pool = new PriorityThreadPool(
                config::async_local_io_thread_pool_thread_num,
                config::async_local_io_thread_pool_queue_size, "async_local_io_thread_pool");
        _remote_io_thread_pool = new PriorityThreadPool(
                config::async_remote_io_thread_pool_thread_num,
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
    static void run_task(const std::function<void()>& fn, io::FileSystemType file_type,
                         io::AsyncIOStatistics* stats = nullptr) {
        DCHECK(bthread_self() != 0);
        struct TaskStats {
            int64_t total_use_timer_ns = 0;
            int64_t task_wait_worker_timer_ns = 0;
            int64_t task_wake_up_timer_ns = 0;
            int64_t task_exec_timer_ns = 0;
            int64_t task_total = 0;
            int64_t wait_for_putting_queue = 0;
        };
        TaskStats task_stats;
        ++task_stats.task_total;
        {
            SCOPED_RAW_TIMER(&task_stats.total_use_timer_ns);
            bthread::CountdownEvent event;

            AsyncIOCtx* ctx = static_cast<AsyncIOCtx*>(bthread_getspecific(btls_io_ctx_key));
            int nice = ctx != nullptr ? ctx->nice : 18;

            doris::signal::BthreadSignalCtx* sig_ctx =
                    static_cast<doris::signal::BthreadSignalCtx*>(
                            bthread_getspecific(doris::signal::btls_signal_key));
            uint64_t query_id_hi = sig_ctx != nullptr ? sig_ctx->query_id_hi : 0;
            uint64_t query_id_lo = sig_ctx != nullptr ? sig_ctx->query_id_lo : 0;

            auto task_wait_time = std::chrono::steady_clock::now();
            auto task_wake_up_time = std::chrono::steady_clock::now();
            PriorityThreadPool::Task task;
            task.priority = nice;
            task.work_function = [&] {
                task_stats.task_wait_worker_timer_ns =
                        std::chrono::duration_cast<std::chrono::nanoseconds>(
                                std::chrono::steady_clock::now() - task_wait_time)
                                .count();
                {
                    SCOPED_RAW_TIMER(&task_stats.task_exec_timer_ns);
                    doris::signal::query_id_hi = query_id_hi;
                    doris::signal::query_id_lo = query_id_lo;
                    fn();
                }
                task_wake_up_time = std::chrono::steady_clock::now();
                event.signal();
            };
            auto wait_for_putting_queue = std::chrono::steady_clock::now();
            if (file_type == io::FileSystemType::LOCAL) {
                AsyncIO::instance().local_io_thread_pool()->offer(task);
            } else {
                AsyncIO::instance().remote_io_thread_pool()->offer(task);
            }
            task_stats.wait_for_putting_queue =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - wait_for_putting_queue)
                            .count();
            if (int ec = event.wait(); ec != 0) [[unlikely]] {
                LOG(FATAL) << "Failed to wait for task to complete";
            }
            task_stats.task_wake_up_timer_ns =
                    std::chrono::duration_cast<std::chrono::nanoseconds>(
                            std::chrono::steady_clock::now() - task_wake_up_time)
                            .count();
        }
        if (stats) {
            if (file_type != io::FileSystemType::LOCAL) {
                stats->remote_total_use_timer_ns += task_stats.total_use_timer_ns;
                stats->remote_task_exec_timer_ns += task_stats.task_exec_timer_ns;
                stats->remote_task_wait_worker_timer_ns += task_stats.task_wait_worker_timer_ns;
                stats->remote_task_wake_up_timer_ns += task_stats.task_wake_up_timer_ns;
                stats->remote_task_total += task_stats.task_total;
                stats->remote_wait_for_putting_queue += task_stats.wait_for_putting_queue;
            } else {
                stats->local_total_use_timer_ns += task_stats.total_use_timer_ns;
                stats->local_task_exec_timer_ns += task_stats.task_exec_timer_ns;
                stats->local_task_wait_worker_timer_ns += task_stats.task_wait_worker_timer_ns;
                stats->local_task_wake_up_timer_ns += task_stats.task_wake_up_timer_ns;
                stats->local_task_total += task_stats.task_total;
                stats->local_wait_for_putting_queue += task_stats.wait_for_putting_queue;
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
    std::atomic<int> _local_queue_id {0};
    std::atomic<int> _remote_queue_id {0};
};

} // end namespace doris
