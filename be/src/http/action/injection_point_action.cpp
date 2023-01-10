#include "http/action/injection_point_action.h"

#include <chrono>

#include "common/sync_point.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/http_status.h"
#include "olap/rowset/rowset.h"
#include "util/stack_util.h"

namespace doris {

// TODO(cyx): Provide an object pool
// `suite_map` won't be modified after `register_suites`
static std::map<std::string, std::function<void()>> suite_map;

// only call once
void InjectionPointAction::register_suites() {
    suite_map.emplace("test_compaction", [] {
        auto sp = SyncPoint::get_instance();
        sp->set_call_back("new_cumulative_point", [](auto&& args) {
            auto output_rowset = try_any_cast<Rowset*>(args[0]);
            auto last_cumulative_point = try_any_cast<int64_t>(args[1]);
            auto pair = try_any_cast<std::pair<int64_t, bool>*>(args.back());
            pair->first = output_rowset->start_version() == last_cumulative_point
                                  ? output_rowset->end_version() + 1
                                  : last_cumulative_point;
            pair->second = true;
        });
    });
}

void set_sleep(const std::string& point, HttpRequest* req) {
    int duration = 0;
    auto& duration_str = req->param("duration");
    if (!duration_str.empty()) {
        try {
            duration = std::stoi(duration_str);
        } catch (const std::exception& e) {
            HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST,
                                    "invalid duration: " + duration_str);
            return;
        }
    }
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [duration](auto&& args) {
        std::this_thread::sleep_for(std::chrono::milliseconds(duration));
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return(const std::string& point, HttpRequest* req) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [](auto&& args) {
        try {
            auto pred = try_any_cast<bool*>(args.back());
            *pred = true;
        } catch (const std::bad_any_cast& e) {
            LOG_FIRST_N(ERROR, 1) << "failed to process `return` callback\n" << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return_ok(const std::string& point, HttpRequest* req) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [](auto&& args) {
        try {
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            pair->first = Status::OK();
            pair->second = true;
        } catch (const std::bad_any_cast& e) {
            LOG_FIRST_N(ERROR, 1) << "failed to process `return_ok` callback\n"
                                  << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void set_return_error(const std::string& point, HttpRequest* req) {
    auto sp = SyncPoint::get_instance();
    sp->set_call_back(point, [](auto&& args) {
        try {
            auto pair = try_any_cast<std::pair<Status, bool>*>(args.back());
            pair->first = Status::InternalError("injected error");
            pair->second = true;
        } catch (const std::bad_any_cast& e) {
            LOG_FIRST_N(ERROR, 1) << "failed to process `return_error` callback\n"
                                  << get_stack_trace();
        }
    });
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void handle_set(HttpRequest* req) {
    auto& point = req->param("name");
    if (point.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty point name");
        return;
    }
    auto& behavior = req->param("behavior");
    if (behavior.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty behavior");
        return;
    }
    if (behavior == "sleep") {
        set_sleep(point, req);
        return;
    } else if (behavior == "return") {
        set_return(point, req);
        return;
    } else if (behavior == "return_ok") {
        set_return_ok(point, req);
        return;
    } else if (behavior == "return_error") {
        set_return_error(point, req);
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "unknown behavior: " + behavior);
    return;
}

void handle_clear(HttpRequest* req) {
    auto& point = req->param("name");
    if (point.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty point name");
        return;
    }
    auto sp = SyncPoint::get_instance();
    if (point == "all") {
        sp->clear_all_call_backs();
        HttpChannel::send_reply(req, HttpStatus::OK, "OK");
        return;
    }
    sp->clear_call_back(point);
    HttpChannel::send_reply(req, HttpStatus::OK, "OK");
}

void handle_suite(HttpRequest* req) {
    auto& suite = req->param("name");
    if (suite.empty()) {
        HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "empty suite name");
        return;
    }
    if (auto it = suite_map.find(suite); it != suite_map.end()) {
        it->second(); // set injection callbacks
        HttpChannel::send_reply(req, HttpStatus::OK, "OK");
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, "unknown suite: " + suite);
}

void InjectionPointAction::handle(HttpRequest* req) {
    LOG(INFO) << req->debug_string();
    auto& op = req->param("op");
    if (op == "set") {
        handle_set(req);
        return;
    } else if (op == "clear") {
        handle_clear(req);
        return;
    } else if (op == "apply_suite") {
        handle_suite(req);
        return;
    }
    HttpChannel::send_reply(req, HttpStatus::BAD_REQUEST, "unknown op: " + op);
}

} // namespace doris
