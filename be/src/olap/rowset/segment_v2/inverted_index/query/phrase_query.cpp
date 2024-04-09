#include "phrase_query.h"

#include <charconv>

namespace doris::segment_v2 {

Status PhraseQuery::parser_slop(std::string& query, int32_t& slop) {
    auto is_digits = [](const std::string_view& str) {
        return std::all_of(str.begin(), str.end(), [](unsigned char c) { return std::isdigit(c); });
    };

    size_t last_space_pos = query.find_last_of(' ');
    if (last_space_pos != std::string::npos) {
        size_t tilde_pos = last_space_pos + 1;
        if (tilde_pos < query.size() - 1 && query[tilde_pos] == '~') {
            size_t slop_pos = tilde_pos + 1;
            std::string_view slop_str(query.data() + slop_pos, query.size() - slop_pos);
            if (is_digits(slop_str)) {
                auto result = std::from_chars(slop_str.begin(), slop_str.end(), slop);
                if (result.ec != std::errc()) {
                    return Status::Error<doris::ErrorCode::INVERTED_INDEX_INVALID_PARAMETERS>(
                            "PhraseQuery parser failed: {}", query);
                }
                query = query.substr(0, last_space_pos);
            }
        }
    }
    return Status::OK();
}

} // namespace doris::segment_v2