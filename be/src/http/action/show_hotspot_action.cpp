#include "show_hotspot_action.h"

#include <string>

#include "cloud/cloud_tablet_mgr.h"
#include "cloud/utils.h"
#include "http/http_channel.h"
#include "http/http_request.h"

namespace doris {
namespace {

enum class Metrics {
    READ_BLOCK = 0,
    WRITE = 1,
    COMPACTION = 2,
};

Status check_param(HttpRequest* req, int& top_n, Metrics& metrics) {
    const std::string TOPN_PARAM = "topn";

    auto& topn_str = req->param(TOPN_PARAM);
    if (!topn_str.empty()) {
        try {
            top_n = std::stoi(topn_str);
        } catch (const std::exception& e) {
            return Status::InternalError("convert topn failed, {}", e.what());
        }
    }

    const std::string METRICS_PARAM = "metrics";
    auto& metrics_str = req->param(METRICS_PARAM);
    if (metrics_str.empty()) {
        return Status::InternalError("metrics must be specified");
    }

    if (metrics_str == "read_block") {
        metrics = Metrics::READ_BLOCK;
    } else if (metrics_str == "write") {
        metrics = Metrics::WRITE;
    } else if (metrics_str == "compaction") {
        metrics = Metrics::COMPACTION;
    } else {
        return Status::InternalError("unknown metrics: {}", metrics_str);
    }

    return Status::OK();
}

struct TabletCounter {
    int64_t tablet_id {0};
    int64_t count {0};
};

struct Comparator {
    constexpr bool operator()(const TabletCounter& lhs, const TabletCounter& rhs) const {
        return lhs.count > rhs.count;
    }
};

using MinHeap = std::priority_queue<TabletCounter, std::vector<TabletCounter>, Comparator>;

} // namespace

void ShowHotspotAction::handle(HttpRequest* req) {
    int topn = 0;
    Metrics metrics;
    auto st = check_param(req, topn, metrics);
    if (!st.ok()) [[unlikely]] {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string());
        return;
    }

    auto tablets = cloud::tablet_mgr()->get_weak_tablets();

    std::vector<TabletCounter> buffer;
    buffer.reserve(tablets.size());

    std::function<int64_t(const Tablet&)> count_fn;
    switch (metrics) {
    case Metrics::READ_BLOCK:
        count_fn = [](auto&& t) { return t.read_block_count.load(std::memory_order_relaxed); };
        break;
    case Metrics::WRITE:
        count_fn = [](auto&& t) { return t.write_count.load(std::memory_order_relaxed); };
        break;
    case Metrics::COMPACTION:
        count_fn = [](auto&& t) { return t.compaction_count.load(std::memory_order_relaxed); };
        break;
    }

    for (auto&& t : tablets) {
        if (auto tablet = t.lock(); tablet) {
            buffer.push_back({tablet->tablet_id(), count_fn(*tablet)});
        }
    }

    if (topn <= 0) {
        topn = tablets.size();
    }

    MinHeap min_heap;
    for (auto&& counter : buffer) {
        min_heap.push(counter);
        if (min_heap.size() > topn) {
            min_heap.pop();
        }
    }

    buffer.resize(0);
    while (!min_heap.empty()) {
        buffer.push_back(min_heap.top());
        min_heap.pop();
    }

    std::string res;
    res.reserve(buffer.size() * 20);
    // Descending order
    std::for_each(buffer.rbegin(), buffer.rend(), [&res](auto&& counter) {
        res += fmt::format("{} {}\n", counter.tablet_id, counter.count);
    });

    HttpChannel::send_reply(req, HttpStatus::OK, res);
}

} // namespace doris
