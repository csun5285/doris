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
                             const FileCacheSettings& file_cache_settings);

    CloudFileCachePtr get_by_path(const IFileCache::Key& key);
    std::vector<IFileCache::QueryContextHolderPtr> get_query_context_holders(
            const TUniqueId& query_id);
    FileCacheFactory() = default;
    FileCacheFactory& operator=(const FileCacheFactory&) = delete;
    FileCacheFactory(const FileCacheFactory&) = delete;

private:
    std::vector<std::unique_ptr<IFileCache>> _caches;
};

} // namespace io
} // namespace doris
