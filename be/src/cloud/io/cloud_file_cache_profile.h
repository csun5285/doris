#pragma once

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "olap/olap_common.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"

namespace doris {
namespace io {

struct AtomicStatistics {
    std::atomic<int64_t> num_io_bytes_read_from_cache = 0;
    std::atomic<int64_t> num_io_bytes_read_from_remote = 0;
};

struct FileCacheProfile;

struct FileCacheMetric {
    FileCacheMetric(int64_t table_id, FileCacheProfile* profile)
            : profile(profile), table_id(table_id) {}

    FileCacheMetric(int64_t table_id, int64_t partition_id, FileCacheProfile* profile)
            : profile(profile), table_id(table_id), partition_id(partition_id) {}

    void register_entity();
    void deregister_entity() const {
        DorisMetrics::instance()->metric_registry()->deregister_entity(entity);
    }
    void update_table_metrics() const;
    void update_partition_metrics() const;

    FileCacheMetric& operator=(const FileCacheMetric&) = delete;
    FileCacheMetric(const FileCacheMetric&) = delete;
    FileCacheProfile* profile = nullptr;
    int64_t table_id = -1;
    int64_t partition_id = -1;
    std::shared_ptr<MetricEntity> entity;
    IntAtomicCounter* num_io_bytes_read_total = nullptr;
    IntAtomicCounter* num_io_bytes_read_from_cache = nullptr;
    IntAtomicCounter* num_io_bytes_read_from_remote = nullptr;
};

struct FileCacheProfile {
    static FileCacheProfile& instance() {
        static FileCacheProfile s_profile;
        return s_profile;
    }

    FileCacheProfile() {
        OlapReaderStatistics stats;
        update(0, 0, &stats);
    }

    // avoid performance impact, use https to control
    inline static std::atomic<bool> s_enable_profile = true;

    static void set_enable_profile(bool flag) {
        // if enable_profile = false originally, set true, it will clear the count
        if (!s_enable_profile && flag) {
            std::lock_guard lock(instance()._mtx);
            instance()._profile.clear();
        }
        s_enable_profile.store(flag, std::memory_order_release);
    }

    void update(int64_t table_id, int64_t partition_id, OlapReaderStatistics* stats);

    void deregister_metric(int64_t table_id, int64_t partition_id);
    std::mutex _mtx;
    // use shared_ptr for concurrent
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<AtomicStatistics>>>
            _profile;
    std::unordered_map<int64_t, std::shared_ptr<FileCacheMetric>> _table_metrics;
    std::unordered_map<int64_t, std::unordered_map<int64_t, std::shared_ptr<FileCacheMetric>>>
            _partition_metrics;
    std::shared_ptr<AtomicStatistics> report(int64_t table_id);
    std::shared_ptr<AtomicStatistics> report(int64_t table_id, int64_t partition_id);
};

} // namespace io
} // namespace doris