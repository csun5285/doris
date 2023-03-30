#include "cloud/io/cloud_file_cache_settings.h"

#include "common/config.h"

namespace doris::io {

FileCacheSettings calc_settings(size_t total_size, size_t max_query_cache_size) {
    io::FileCacheSettings settings;
    settings.total_size = total_size;
    settings.max_file_segment_size = config::file_cache_max_file_segment_size;
    settings.max_query_cache_size = max_query_cache_size;
    size_t per_size = settings.total_size / io::percentage[3];
    settings.disposable_queue_size = per_size * io::percentage[1];
    settings.disposable_queue_elements =
            std::max(settings.disposable_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);

    settings.index_queue_size = per_size * io::percentage[2];
    settings.index_queue_elements =
            std::max(settings.index_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);

    settings.query_queue_size =
            settings.total_size - settings.disposable_queue_size - settings.index_queue_size;
    settings.query_queue_elements =
            std::max(settings.query_queue_size / settings.max_file_segment_size,
                     io::FILE_CACHE_QUEUE_DEFAULT_ELEMENTS);
    return settings;
}

} // namespace doris::io