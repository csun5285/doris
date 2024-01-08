#pragma once

#include <condition_variable>
#include <deque>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/status.h"
#include "gen_cpp/BackendService.h"

namespace doris::cloud {

enum class DownloadType {
    BE,
    S3,
};

struct JobMeta {
    JobMeta(const TJobMeta& meta);
    DownloadType download_type;
    std::string be_ip;
    int32_t brpc_port;
    std::vector<int64_t> tablet_ids;
};

class CloudWarmUpManager {
public:
    static CloudWarmUpManager* instance() {
        static CloudWarmUpManager s_manager;
        return &s_manager;
    }

    CloudWarmUpManager();
    ~CloudWarmUpManager();

    Status check_and_set_job_id(int64_t job_id);

    Status check_and_set_batch_id(int64_t job_id, int64_t batch_id, bool* retry = nullptr);

    void add_job(const std::vector<TJobMeta>& job_metas);

#ifdef BE_TEST
    void consumer_job();
#endif

    std::tuple<int64_t, int64_t, int64_t, int64_t> get_current_job_state();

    Status clear_job(int64_t job_id);

private:
    std::mutex _mtx;
    std::condition_variable _cond;
    int64_t _cur_job_id {0};
    int64_t _cur_batch_id {-1};
    std::deque<JobMeta> _pending_job_metas;
    std::vector<JobMeta> _finish_job;
    std::thread _download_thread;
    bool _closed {false};
    void handle_jobs();
};

} // namespace doris::cloud
