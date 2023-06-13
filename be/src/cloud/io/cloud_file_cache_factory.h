#pragma once

#include <vector>

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_file_cache_fwd.h"
#include "cloud/io/cloud_file_cache_settings.h"
namespace doris {
namespace io {
/**
 * Creates a FileCache object for cache_base_path.
 */
class FileCacheFactory {
public:
    static FileCacheFactory& instance();

    Status create_file_cache(const std::string& cache_base_path,
                             FileCacheSettings file_cache_settings);

    CloudFileCachePtr get_by_path(const Key& key);

    std::vector<CloudFileCache::QueryContextHolderPtr> get_query_context_holders(
            const TUniqueId& query_id);

    Status reload_file_cache();

    void set_read_only();

    size_t get_total_cache_size() const { return total_cache_size; }

    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<std::unique_ptr<CloudFileCache>> _caches;
    size_t total_cache_size = 0;
};

} // namespace io
} // namespace doris
