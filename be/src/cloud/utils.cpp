#include "cloud/utils.h"

#include <bthread/bthread.h>
#include <bthread/condition_variable.h>

#include "common/sync_point.h"
#include "olap/storage_engine.h"

namespace doris::cloud {

DataDir* cloud_data_dir() {
#ifdef BE_TEST
    return nullptr;
#endif
    return StorageEngine::instance()->get_stores().front();
}

MetaMgr* meta_mgr() {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("meta_mgr", (MetaMgr*)nullptr);
    return StorageEngine::instance()->meta_mgr();
}

CloudTabletMgr* tablet_mgr() {
    static CloudTabletMgr tablet_mgr;
    return &tablet_mgr;
}

io::FileSystemSPtr latest_fs() {
    return StorageEngine::instance()->latest_fs();
}

static void* run_bthread_work(void* arg) {
    auto f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

Status bthread_fork_and_join(const std::vector<std::function<Status()>>& tasks, int concurrency) {
    Status status; // Guard by mtx
    int count = 0; // Guard by mtx
    bthread::Mutex mtx;
    bthread::ConditionVariable condv;
    if (tasks.empty()) return status;
    for (auto it = tasks.begin(); it != tasks.end() - 1; ++it) {
        {
            std::unique_lock lock(mtx);
            while (status.ok() && count >= concurrency) {
                condv.wait(lock);
            }
            if (!status.ok()) break;
            ++count;
        }
        auto fn = new std::function<void()>([&, &task = *it] {
            auto st = task();
            {
                std::lock_guard lock(mtx);
                --count;
                if (!st.ok()) std::swap(st, status);
                condv.notify_one();
            }
        });
        bthread_t bid;
        if (bthread_start_background(&bid, nullptr, run_bthread_work, fn) != 0) {
            run_bthread_work(fn);
        }
    }
    if (status.ok()) {
        auto st = tasks.back()(); // Do last task inplace
        if (!st.ok()) {
            std::unique_lock lock(mtx);
            std::swap(st, status);
        }
    }
    std::unique_lock lock(mtx);
    while (count > 0) { // Wait until all running tasks have done
        condv.wait(lock);
    }
    return status;
}

} // namespace doris::cloud
