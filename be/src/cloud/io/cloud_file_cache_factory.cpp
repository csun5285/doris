// clang-format off
#include "cloud/io/cloud_file_cache_factory.h"

#include "cloud/io/cloud_file_cache.h"
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
                                           FileCacheSettings file_cache_settings) {
    if (config::clear_file_cache) {
        auto fs = global_local_filesystem();
        bool res = false;
        fs->exists(cache_base_path, &res);
        if (res) {
            fs->delete_directory(cache_base_path);
            fs->create_directory(cache_base_path);
        }
    }

    auto fs = global_local_filesystem();
    bool res = false;
    RETURN_IF_ERROR(fs->exists(cache_base_path, &res));
    if (!res) {
        fs->create_directory(cache_base_path);
    } else if (config::clear_file_cache) {
        fs->delete_directory(cache_base_path);
        fs->create_directory(cache_base_path);
    }

    struct statfs stat;
    if (statfs(cache_base_path.c_str(), &stat) < 0) {
        LOG_ERROR("").tag("file cache path", cache_base_path).tag("error", strerror(errno));
        return Status::IOError("{} statfs error {}", cache_base_path, strerror(errno));
    }
    size_t disk_total_size = static_cast<size_t>(stat.f_blocks) * static_cast<size_t>(stat.f_bsize);
    if (disk_total_size < file_cache_settings.total_size) {
        file_cache_settings = calc_settings(disk_total_size * 0.9, file_cache_settings.max_query_cache_size);
    }
    std::unique_ptr<CloudFileCache> cache =
            std::make_unique<CloudFileCache>(cache_base_path, file_cache_settings);
    RETURN_IF_ERROR(cache->initialize());
    _caches.push_back(std::move(cache));

    LOG(INFO) << "[FileCache] path: " << cache_base_path
              << " total_size: " << file_cache_settings.total_size;
    total_cache_size += file_cache_settings.total_size;
    return Status::OK();
}

CloudFileCachePtr FileCacheFactory::get_by_path(const Key& key) {
    return _caches[CloudFileCache::KeyHash()(key) % _caches.size()].get();
}

std::vector<CloudFileCache::QueryContextHolderPtr> FileCacheFactory::get_query_context_holders(
        const TUniqueId& query_id) {
    std::vector<CloudFileCache::QueryContextHolderPtr> holders;
    for (const auto& cache : _caches) {
        holders.push_back(cache->get_query_context_holder(query_id));
    }
    return holders;
}

Status FileCacheFactory::reload_file_cache() {
    for (auto& cache : _caches) {
        RETURN_IF_ERROR(cache->reinitialize());
    }
    CloudFileCache::set_read_only(false);
    return Status::OK();
}

void FileCacheFactory::set_read_only() {
    CloudFileCache::set_read_only(true);
}

} // namespace io
} // namespace doris
