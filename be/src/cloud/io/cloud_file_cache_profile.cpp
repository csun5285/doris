#include "cloud/io/cloud_file_cache_profile.h"

#include <memory>

#include "olap/base_tablet.h"

namespace doris {
namespace io {

DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_total, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_cache, MetricUnit::OPERATIONS);
DEFINE_COUNTER_METRIC_PROTOTYPE_2ARG(num_io_bytes_read_from_remote, MetricUnit::OPERATIONS);

std::shared_ptr<AtomicStatistics> FileCacheProfile::report(int64_t table_id) {
    std::shared_ptr<AtomicStatistics> stats = std::make_shared<AtomicStatistics>();
    if (_profile.count(table_id) == 1) {
        std::lock_guard lock(_mtx);
        auto& table_stats = _profile[table_id];
        stats->num_io_bytes_read_from_cache += table_stats->num_io_bytes_read_from_cache;
        stats->num_io_bytes_read_from_remote += table_stats->num_io_bytes_read_from_remote;
    }
    return stats;
}

void FileCacheProfile::update(int64_t table_id, OlapReaderStatistics* stats) {
    if (!s_enable_profile.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<AtomicStatistics> count;
    std::shared_ptr<FileCacheMetric> table_metric;
    {
        std::lock_guard lock(_mtx);
        if (_profile.count(table_id) < 1) {
            _profile[table_id] = std::make_shared<AtomicStatistics>();
            table_metric = std::make_shared<FileCacheMetric>(table_id, this);
            _table_metrics[table_id] = table_metric;
        }
        count = _profile[table_id];
    }
    if (table_metric) [[unlikely]] {
        table_metric->register_entity();
    }
    count->num_io_bytes_read_from_cache += stats->file_cache_stats.bytes_read_from_local;
    count->num_io_bytes_read_from_remote += stats->file_cache_stats.bytes_read_from_remote;
}

void FileCacheProfile::deregister_metric(int64_t table_id) {
    if (!s_enable_profile.load(std::memory_order_acquire)) {
        return;
    }
    std::shared_ptr<FileCacheMetric> table_metric;
    {
        std::lock_guard lock(_mtx);
        _profile.erase(table_id);
        table_metric = _table_metrics[table_id];
        _table_metrics.erase(table_id);
    }

    table_metric->deregister_entity();
}

void FileCacheMetric::register_entity() {
    std::string table_id_str = std::to_string(table_id);
    entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("cloud_file_cache"),
            {{"table_id", table_id_str}, {"table_name", BaseTablet::get_table_name(table_id)}});
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_total);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_cache);
    INT_ATOMIC_COUNTER_METRIC_REGISTER(entity, num_io_bytes_read_from_remote);
    entity->register_hook("cloud_file_cache",
                          std::bind(&FileCacheMetric::update_table_metrics, this));
}

void FileCacheMetric::update_table_metrics() const {
    auto stats = profile->report(table_id);
    num_io_bytes_read_from_cache->set_value(stats->num_io_bytes_read_from_cache);
    num_io_bytes_read_from_remote->set_value(stats->num_io_bytes_read_from_remote);
    num_io_bytes_read_total->set_value(stats->num_io_bytes_read_from_cache +
                                       stats->num_io_bytes_read_from_remote);
}

} // namespace io
} // namespace doris
