
#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <type_traits>

namespace selectdb {

bool init_glog(const char* basename);

/// Wrap a glog stream and tag on the log. usage:
///   LOG_INFO("here is an info for a {} query", query_type).tag("query_id", queryId);
#define LOG_INFO(...) ::selectdb::TaggableLogger(LOG(INFO), ##__VA_ARGS__)
#define LOG_WARNING(...) ::selectdb::TaggableLogger(LOG(WARNING), ##__VA_ARGS__)
#define LOG_ERROR(...) ::selectdb::TaggableLogger(LOG(ERROR), ##__VA_ARGS__)
#define LOG_FATAL(...) ::selectdb::TaggableLogger(LOG(FATAL), ##__VA_ARGS__)

class TaggableLogger {
public:
    template <typename... Args>
    TaggableLogger(std::ostream& stream, std::string_view fmt, Args&&... args) : stream_(stream) {
        if constexpr (sizeof...(args) == 0) {
            stream_ << fmt;
        } else {
            stream_ << fmt::format(fmt, std::forward<Args>(args)...);
        }
    };

    template <typename V>
    TaggableLogger& tag(std::string_view key, const V& value) {
        stream_ << ' ' << key << '=';
        if constexpr (std::is_convertible_v<V, std::string_view>) {
            stream_ << '"' << value << '"';
        } else {
            stream_ << value;
        }
        return *this;
    }

private:
    std::ostream& stream_;
};

} // namespace selectdb

// To keep it simple and practical, we don't actually need so many VLOG levels.
// Using `VLOG(${number})` is confusing and hard to desid in most cases, all we
// need is a complementary debug level to glog's default 4 levels of logging.
// "One VLOG level to rule them all!"
#define DEBUG 5
// VLOG_DEBUG is alias of VLOG(DEBUG) I.O.W VLOG(5)
#define VLOG_DEBUG VLOG(DEBUG)
