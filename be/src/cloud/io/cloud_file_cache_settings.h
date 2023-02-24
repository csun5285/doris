#pragma once

#include "cloud/io/cloud_file_cache_fwd.h"

namespace doris {
namespace io {
// [query:disposable:index:total]
constexpr std::array<size_t, 4> percentage {17, 2, 1, 20};
static_assert(percentage[0] + percentage[1] + percentage[2] == percentage[3]);
struct FileCacheSettings {
    size_t total_size {0};
    size_t disposable_queue_size {0};
    size_t disposable_queue_elements {0};
    size_t index_queue_size {0};
    size_t index_queue_elements {0};
    size_t query_queue_size {0};
    size_t query_queue_elements {0};
    size_t max_file_segment_size {0};
    size_t max_query_cache_size {0};
};

} // namespace io
} // namespace doris
