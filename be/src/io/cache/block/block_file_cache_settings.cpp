#include "io/cache/block/block_file_cache_settings.h"

#include "common/config.h"

namespace doris::io {

FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size) {
    io::FileCacheSettings settings;
    settings.total_size = total_size;
    settings.max_file_block_size = FILE_CACHE_MAX_FILE_BLOCK_SIZE;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.total_size / io::percentage[3];
    settings.disposable_queue_size = per_size * io::percentage[1];
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * io::percentage[2];
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.total_size - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_block_size,
                     io::REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS);
    return settings;
}

} // namespace doris::io