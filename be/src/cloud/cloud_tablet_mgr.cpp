#include "cloud/cloud_tablet_mgr.h"

#include <bthread/countdown_event.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <variant>

#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/sync_point.h"
#include "olap/tablet_meta.h"

namespace doris::cloud {

// port from
// https://github.com/golang/groupcache/blob/master/singleflight/singleflight.go
template <typename Key, typename Val>
class SingleFlight {
public:
    SingleFlight() = default;

    SingleFlight(const SingleFlight&) = delete;
    void operator=(const SingleFlight&) = delete;

    using Loader = std::function<std::shared_ptr<Val>(const Key&)>;

    // Do executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    std::shared_ptr<Val> Do(const Key& key, Loader loader) {
        std::unique_lock lock(_call_map_mtx);

        auto it = _call_map.find(key);
        if (it != _call_map.end()) {
            auto call = it->second;
            lock.unlock();
            if (int ec = call->event.wait(); ec != 0) {
                throw std::system_error(std::error_code(ec, std::system_category()),
                                        "CountdownEvent wait failed");
            }
            return call->val;
        }
        auto call = std::make_shared<Call>();
        _call_map.emplace(key, call);
        lock.unlock();

        call->val = loader(key);
        call->event.signal();

        lock.lock();
        _call_map.erase(key);
        lock.unlock();

        return call->val;
    }

private:
    // `Call` is an in-flight or completed `Do` call
    struct Call {
        bthread::CountdownEvent event;
        std::shared_ptr<Val> val;
    };

    std::mutex _call_map_mtx;
    std::unordered_map<Key, std::shared_ptr<Call>> _call_map;
};

static SingleFlight<int64_t, std::variant<Status, TabletSharedPtr>> s_singleflight_load_tablet;

// tablet_id -> cached tablet
// This map owns all cached tablets. The lifetime of tablet can be longer than the LRU handle.
// It's also used for scenarios where users want to access the tablet by `tablet_id` without changing the LRU order.
// TODO(cyx): multi shard to increase concurrency
class CloudTabletMgr::TabletMap {
public:
    void put(TabletSharedPtr tablet) {
        std::lock_guard lock(_mtx);
        _map[tablet->tablet_id()] = std::move(tablet);
    }

    void erase(Tablet* tablet) {
        std::lock_guard lock(_mtx);
        auto it = _map.find(tablet->tablet_id());
        if (it != _map.end() && it->second.get() == tablet) {
            _map.erase(it);
        }
    }

    TabletSharedPtr get(int64_t tablet_id) {
        std::lock_guard lock(_mtx);
        if (auto it = _map.find(tablet_id); it != _map.end()) {
            return it->second;
        }
        return nullptr;
    }

    size_t size() { return _map.size(); }

    void traverse(std::function<void(const TabletSharedPtr&)> visitor) {
        std::lock_guard lock(_mtx);
        for (auto& [_, tablet] : _map) {
            visitor(tablet);
        }
    }

private:
    std::mutex _mtx;
    std::unordered_map<int64_t, TabletSharedPtr> _map;
};

CloudTabletMgr::CloudTabletMgr()
        : _cache(new_lru_cache("TabletCache", config::tablet_cache_capacity, LRUCacheType::NUMBER,
                               config::tablet_cache_shards)),
          _tablet_map(std::make_shared<TabletMap>()) {}

CloudTabletMgr::~CloudTabletMgr() = default;

Status CloudTabletMgr::get_tablet(int64_t tablet_id, TabletSharedPtr* tablet) {
    // LRU value type
    struct Value {
        TabletSharedPtr tablet;
        std::shared_ptr<TabletMap> tablet_map;
    };

    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str);
    auto handle = _cache->lookup(key);
    TEST_SYNC_POINT_CALLBACK("CloudTabletMgr::get_tablet", handle);

    if (handle == nullptr) {
        auto load_tablet = [this, &key](int64_t tablet_id)
                -> std::shared_ptr<std::variant<Status, TabletSharedPtr>> {
            auto res = std::make_shared<std::variant<Status, TabletSharedPtr>>();

            TabletMetaSharedPtr tablet_meta;
            auto st = meta_mgr()->get_tablet_meta(tablet_id, &tablet_meta);
            if (!st.ok()) {
                *res = std::move(st);
                return res;
            }
            auto tablet = std::make_shared<Tablet>(std::move(tablet_meta), cloud::cloud_data_dir());
            auto value = new Value();
            value->tablet = tablet;
            value->tablet_map = _tablet_map;
            st = meta_mgr()->sync_tablet_rowsets(tablet.get());
            // ignore failure here because we will sync this tablet before query
            if (!st.ok()) {
                LOG_WARNING("sync tablet {} failed", tablet_id).error(st);
            }
            static auto deleter = [](const CacheKey& key, void* value) {
                auto value1 = reinterpret_cast<Value*>(value);
                // tablet has been evicted, release it from `tablet_map`
                value1->tablet_map->erase(value1->tablet.get());
                delete value1;
            };

            auto handle = _cache->insert(key, value, 1, deleter);
            _tablet_map->put(std::move(tablet));
            *res = std::shared_ptr<Tablet>(value->tablet.get(),
                                           [this, handle](...) { _cache->release(handle); });
            return res;
        };

        auto res = s_singleflight_load_tablet.Do(tablet_id, std::move(load_tablet));
        if (auto st = std::get_if<Status>(res.get())) {
            return *st;
        }
        *tablet = std::get<TabletSharedPtr>(*res);
        return Status::OK();
    }

    Tablet* tablet1 = reinterpret_cast<Value*>(_cache->value(handle))->tablet.get();
    *tablet = std::shared_ptr<Tablet>(tablet1, [this, handle](...) { _cache->release(handle); });
    return Status::OK();
}

void CloudTabletMgr::erase_tablet(int64_t tablet_id) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str.data(), tablet_id_str.size());
    _cache->erase(key);
}

void CloudTabletMgr::vacuum_stale_rowsets() {
    LOG_INFO("begin to vacuum stale rowsets");
    std::vector<int64_t> tablets_to_vacuum;
    {
        std::lock_guard lock(_vacuum_set_mtx);
        tablets_to_vacuum = std::vector<int64_t>(_vacuum_set.begin(), _vacuum_set.end());
    }
    int num_vacuumed = 0;
    for (int64_t tablet_id : tablets_to_vacuum) {
        auto tablet = _tablet_map->get(tablet_id);
        if (!tablet) continue;
        num_vacuumed += tablet->cloud_delete_expired_stale_rowsets();
        {
            std::shared_lock tablet_rlock(tablet->get_header_lock());
            if (!tablet->has_stale_rowsets()) {
                std::lock_guard lock(_vacuum_set_mtx);
                _vacuum_set.erase(tablet_id);
            }
        }
    }
    LOG_INFO("finish vacuum stale rowsets").tag("num_vacuumed", num_vacuumed);
}

void CloudTabletMgr::add_to_vacuum_set(int64_t tablet_id) {
    std::lock_guard lock(_vacuum_set_mtx);
    _vacuum_set.insert(tablet_id);
}

std::vector<std::weak_ptr<Tablet>> CloudTabletMgr::get_weak_tablets() {
    std::vector<std::weak_ptr<Tablet>> weak_tablets;
    weak_tablets.reserve(_tablet_map->size());
    _tablet_map->traverse([&weak_tablets](auto& t) { weak_tablets.push_back(t); });
    return weak_tablets;
}

uint64_t CloudTabletMgr::get_rowset_nums() {
    uint64_t n = 0;
    auto tablets = get_weak_tablets();
    for (auto& t : tablets) {
        if (auto tablet = t.lock()) {
            n += tablet->version_count();
        }
    }
    return n;
}

uint64_t CloudTabletMgr::get_segment_nums() {
    uint64_t n = 0;
    auto tablets = get_weak_tablets();
    for (auto& t : tablets) {
        if (auto tablet = t.lock()) {
            n += tablet->segment_count();
        }
    }
    return n;
}

void CloudTabletMgr::sync_tablets() {
    LOG_INFO("begin to sync tablets");
    using namespace std::chrono;
    int64_t last_sync_time_bound =
            duration_cast<seconds>(system_clock::now().time_since_epoch()).count() -
            config::tablet_sync_interval_seconds;

    auto weak_tablets = get_weak_tablets();

    // sort by last_sync_time
    static auto cmp = [](const auto& a, const auto& b) { return a.first < b.first; };
    std::multiset<std::pair<int64_t, std::weak_ptr<Tablet>>, decltype(cmp)> sync_time_tablet_set(
            cmp);

    for (auto& weak_tablet : weak_tablets) {
        if (auto tablet = weak_tablet.lock()) {
            if (tablet->tablet_state() != TABLET_RUNNING) {
                continue;
            }
            int64_t last_sync_time = tablet->last_sync_time();
            if (last_sync_time <= last_sync_time_bound) {
                sync_time_tablet_set.emplace(last_sync_time, weak_tablet);
            }
        }
    }

    int num_sync = 0;
    for (auto& [_, weak_tablet] : sync_time_tablet_set) {
        if (auto tablet = weak_tablet.lock()) {
            if (tablet->last_sync_time() > last_sync_time_bound) {
                continue;
            }
            auto st = tablet->cloud_sync_meta();
            if (!st) {
                LOG_WARNING("failed to sync tablet meta {}", tablet->tablet_id()).error(st);
            }
            st = tablet->cloud_sync_rowsets();
            if (!st) {
                LOG_WARNING("failed to sync tablet rowsets {}", tablet->tablet_id()).error(st);
            }
            ++num_sync;
        }
    }
    LOG_INFO("finish sync tablets").tag("num_sync", num_sync);
}

Status CloudTabletMgr::get_topn_tablets_to_compact(int n, CompactionType compaction_type,
                                                   const std::function<bool(Tablet*)>& filter_out,
                                                   std::vector<TabletSharedPtr>* tablets,
                                                   int64_t* max_score) {
    *max_score = 0;
    // clang-format off
    auto score = [compaction_type](Tablet* t) {
        return compaction_type == CompactionType::BASE_COMPACTION ? t->get_cloud_base_compaction_score()
               : compaction_type == CompactionType::CUMULATIVE_COMPACTION ? t->get_cloud_cumu_compaction_score()
               : 0;
    };

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();

    auto last_compaction_time_ms = [type = compaction_type](Tablet* t) {
        return type == CompactionType::BASE_COMPACTION ? t->last_base_compaction_success_time()
               : type == CompactionType::CUMULATIVE_COMPACTION ? t->last_cumu_compaction_success_time()
               : 0;
    };
    auto skip = [&now, type = compaction_type, &last_compaction_time_ms](Tablet* t) {
        // We don't schedule tablets that are recently successfully scheduled
        int64_t interval = type == CompactionType::BASE_COMPACTION ? config::base_compaction_interval_seconds_since_last_operation * 1000
                           : type == CompactionType::CUMULATIVE_COMPACTION ? 1 * 1000
                           : 0;
        return  now - last_compaction_time_ms(t) < interval;
    };
    // We don't schedule tablets that are disabled for compaction
    auto disable = [](Tablet* t) { return t->tablet_meta()->tablet_schema()->disable_auto_compaction(); };

    auto [num_filtered, num_disabled, num_skipped] = std::make_tuple(0, 0, 0);

    auto weak_tablets = get_weak_tablets();
    std::vector<std::pair<TabletSharedPtr, int64_t>> buf;
    buf.reserve(n + 1);
    for (auto& weak_tablet : weak_tablets) {
        auto t = weak_tablet.lock();
        if (t == nullptr) continue;

        int64_t s = score(t.get());
        *max_score = std::max(*max_score, s);

        if (filter_out(t.get())) { ++num_filtered; continue; }
        if (disable(t.get())) { ++num_disabled; continue; }
        if (skip(t.get())) { ++num_skipped; continue; }

        buf.push_back({std::move(t), s});
        std::sort(buf.begin(), buf.end(), [](auto& a, auto& b) { return a.second > b.second; });
        if (buf.size() > n) buf.pop_back();
    }

    // log every 10 sec with default config
    LOG_EVERY_N(INFO, 1000) << "get_topn_compaction_score, n=" << n << " type=" << compaction_type
               << " num_tablets=" << weak_tablets.size() << " num_skipped=" << num_skipped
               << " num_disabled=" << num_disabled << " num_filtered=" << num_filtered
               << " max_score=" << *max_score
               << " tablets=[" << [&buf] { std::stringstream ss; for (auto& i : buf) ss << i.first->tablet_id() << ":" << i.second << ","; return ss.str(); }() << "]"
               ;
    // clang-format on

    tablets->clear();
    tablets->reserve(n + 1);
    for (auto& [t, _] : buf) tablets->emplace_back(std::move(t));

    return Status::OK();
}

} // namespace doris::cloud
