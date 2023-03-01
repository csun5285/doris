// clang-format off
#include "cloud/io/cloud_file_cache_factory.h"

#include "cloud/io/cloud_file_cache.h"
#include "cloud/io/cloud_lru_file_cache.h"
#include "common/config.h"
#include "cloud/io/local_file_system.h"

#include <cstddef>
#include <sys/statfs.h>

// clang-format on
namespace doris {
namespace io {

FileCacheFactory& FileCacheFactory::instance() {
    static FileCacheFactory ret;
    return ret;
}

Status FileCacheFactory::create_file_cache(const std::string& cache_base_path,
                                           const FileCacheSettings& file_cache_settings) {
    if (config::clear_file_cache) {
        auto fs = global_local_filesystem();
        bool res = false;
        fs->exists(cache_base_path, &res);
        if (res) {
            fs->delete_directory(cache_base_path);
            fs->create_directory(cache_base_path);
        }
    }

    std::unique_ptr<IFileCache> cache =
            std::make_unique<LRUFileCache>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    _caches.push_back(std::move(cache));

    struct statfs stat;
    if (statfs(cache_base_path.c_str(), &stat) < 0) {
        LOG_ERROR("").tag("file cache path", cache_base_path).tag("error", strerror(errno));
        return Status::IOError("{} statfs error {}", cache_base_path, strerror(errno));
    }
    size_t disk_total_size = static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize);
    if (disk_total_size < file_cache_settings.total_size) {
        return Status::InternalError("the {} disk size from statfs {} is smaller than config {}",
                                     cache_base_path, disk_total_size,
                                     file_cache_settings.total_size);
    }

    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    return Status::OK();
}

CloudFileCachePtr FileCacheFactory::get_by_path(const IFileCache::Key& key) {
    return _caches[IFileCache::KeyHash()(key) % _caches.size()].get();
}

std::vector<IFileCache::QueryContextHolderPtr> FileCacheFactory::get_query_context_holders(
        const TUniqueId& query_id) {
    std::vector<IFileCache::QueryContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

} // namespace io
} // namespace doris
