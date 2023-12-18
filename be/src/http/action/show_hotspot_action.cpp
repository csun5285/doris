#include "show_hotspot_action.h"

#include <string>

#include "cloud/cloud_tablet_mgr.h"
#include "cloud/utils.h"
#include "http/http_channel.h"
#include "http/http_request.h"

namespace doris {
namespace {

Status check_param(HttpRequest* req, int& top_n) {
    const std::string TOPN_PARAM = "topn";

    auto& topn_str = req->param(TOPN_PARAM);
    if (!topn_str.empty()) {
        try {
            top_n = std::stoi(topn_str);
        } catch (const std::exception& e) {
            return Status::InternalError("convert topn failed, {}", e.what());
        }
    }

    return Status::OK();
}

struct TabletReadCount {
    int64_t tablet_id {0};
    int64_t count {0};
};

struct Comparator {
    constexpr bool operator()(const TabletReadCount& lhs, const TabletReadCount& rhs) const {
        return lhs.count > rhs.count;
    }
};

using MinHeap = std::priority_queue<TabletReadCount, std::vector<TabletReadCount>, Comparator>;

} // namespace

void ShowHotspotAction::handle(HttpRequest* req) {
    int topn = 0;
    auto st = check_param(req, topn);
    if (!st.ok()) [[unlikely]] {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, st.to_string());
        return;
    }

    auto tablets = cloud::tablet_mgr()->get_weak_tablets();

    std::vector<TabletReadCount> buffer;
    buffer.reserve(tablets.size());
    for (auto&& t : tablets) {
        if (auto tablet = t.lock(); tablet) {
            buffer.push_back({tablet->tablet_id(),
                              tablet->read_block_count.load(std::memory_order_relaxed)});
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
