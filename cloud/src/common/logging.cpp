
// clang-format off
#include "logging.h"
#include "config.h"

#include <glog/logging.h>
#include <glog/vlog_is_on.h>

#include <iostream>
#include <mutex>
// clang-format on

namespace selectdb {

/**
 * @param basename the basename of log file
 * @return true for success
 */
bool init_glog(const char* basename) {
    static std::mutex mtx;
    static bool inited = false;
    std::lock_guard<std::mutex> logging_lock(mtx);
    if (inited) return true;

    FLAGS_alsologtostderr = false;
    // Don't log to stderr except fatal level
    // so fatal log can output to be.out .
    FLAGS_stderrthreshold = google::ERROR;

    // Set glog log dir
    FLAGS_log_dir = config::log_dir;
    // Buffer log messages for at most this many seconds
    FLAGS_logbufsecs = 1;
    // Set log roll mode
    // Candidates: day, hour, size
    FLAGS_log_split_method = "size";
    // Sets the maximum log file size (in MB).
    FLAGS_max_log_size = config::log_size_mb;
    // Set roll num
    FLAGS_log_filenum_quota = config::log_filenum_quota;

    // Set warn log roll num
    FLAGS_warn_log_filenum_quota = config::warn_log_filenum_quota;
    // clang-format off
    // set log level
    std::string& loglevel = config::log_level;
    // Can be 0 1 2 3 ... the larger the higher level for logging,
    // corrensponding to INFO WARNING ERROR FATAL
    // const int GLOG_INFO = 0, GLOG_WARNING = 1, GLOG_ERROR = 2, GLOG_FATAL = 3, NUM_SEVERITIES = 4;
    auto tolower = [](std::string s) { for (auto& i : s) i |= 0x20; return s; };
    FLAGS_minloglevel = tolower(loglevel) == "info"  ? 0
                      : tolower(loglevel) == "warn"  ? 1
                      : tolower(loglevel) == "error" ? 2 
                      : tolower(loglevel) == "fatal" ? 3
                      :                                0; // Default INFO
    // clang-format on

    // Log messages at a level <= this flag are buffered.
    // Log messages at a higher level are flushed immediately.
    FLAGS_logbuflevel = config::log_immediate_flush ? -1 : 0;

    // Set verbose modules
    FLAGS_v = -1;
    for (auto& i : config::log_verbose_modules) {
        if (i.empty()) continue;
        google::SetVLOGLevel(i.c_str(), config::log_verbose_level);
    }
    google::InitGoogleLogging(basename);
    inited = true;
    return true;
}

} // namespace selectdb
