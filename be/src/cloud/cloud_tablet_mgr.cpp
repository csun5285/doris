#include "cloud/cloud_tablet_mgr.h"

#include <bthread/countdown_event.h>
#include <glog/logging.h>

#include <algorithm>
#include <sstream>
#include <variant>

#include "cloud/meta_mgr.h"
#include "cloud/olap/storage_engine.h"
#include "cloud/utils.h"
#include "common/config.h"
#include "common/status.h"
#include "common/sync_point.h"
#include "io/cache/block/block_file_cache_downloader.h"
#include "io/fs/s3_file_system.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "util/wait_group.h"

namespace doris::cloud {

// port from
// https://github.com/golang/groupcache/blob/master/singleflight/singleflight.go
template <typename Key, typename Val>
class SingleFlight {
public:
    SingleFlight() = default;

    SingleFlight(const SingleFlight&) = delete;
    void operator=(const SingleFlight&) = delete;

    using Loader = std::function<Val(const Key&)>;

    // Do executes and returns the results of the given function, making
    // sure that only one execution is in-flight for a given key at a
    // time. If a duplicate comes in, the duplicate caller waits for the
    // original to complete and receives the same results.
    Val load(const Key& key, Loader loader) {
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
    // `Call` is an in-flight or completed `load` call
    struct Call {
        bthread::CountdownEvent event;
        Val val;
    };

    std::mutex _call_map_mtx;
    std::unordered_map<Key, std::shared_ptr<Call>> _call_map;
};

static SingleFlight<int64_t, TabletSharedPtr> s_singleflight_load_tablet;

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
        // According to the implementation of `LRUCache`, `deleter` may be called after a tablet
        // with same tablet id insert into cache and `TabletMap`. So we MUST check if the tablet
        // instance to be erased is the same one in the map.
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
        : _tablet_map(std::make_unique<TabletMap>()),
          _cache(new_lru_cache("TabletCache", config::tablet_cache_capacity, LRUCacheType::NUMBER,
                               config::tablet_cache_shards)) {
    for (size_t i = 0; i < mgr_size; i++) {
        _ovarlap_rowsets_mgrs[i].consumer =
                std::thread(&CloudTabletMgr::OverlapRowsetsMgr::handle_overlap_rowsets,
                            &_ovarlap_rowsets_mgrs[i]);
    }
}

CloudTabletMgr::~CloudTabletMgr() {
    std::for_each(_ovarlap_rowsets_mgrs.begin(), _ovarlap_rowsets_mgrs.end(),
                  [](OverlapRowsetsMgr& mgr) {
                      {
                          std::lock_guard lock(mgr.mtx);
                          mgr.closed = true;
                      }
                      mgr.cond.notify_all();
                  });
    std::for_each(_ovarlap_rowsets_mgrs.begin(), _ovarlap_rowsets_mgrs.end(),
                  [](OverlapRowsetsMgr& mgr) {
                      if (mgr.consumer.joinable()) {
                          mgr.consumer.join();
                      }
                  });
}

Status CloudTabletMgr::get_tablet(int64_t tablet_id, TabletSharedPtr* tablet, bool warmup_data) {
    TEST_SYNC_POINT_RETURN_WITH_VALUE("CloudTabletMgr::get_tablet_2", Status::OK(), tablet_id,
                                      tablet);
    // LRU value type. `Value`'s lifetime MUST NOT be longer than `CloudTabletMgr`
    struct Value {
        // FIXME(plat1ko): The ownership of tablet seems to belong to 'TabletMap', while `Value`
        // only requires a reference.
        TabletSharedPtr tablet;
        TabletMap& tablet_map;
    };

    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str);
    auto handle = _cache->lookup(key);
    TEST_SYNC_POINT_CALLBACK("CloudTabletMgr::get_tablet", handle);

    if (handle == nullptr) {
        auto load_tablet = [this, &key, warmup_data](int64_t tablet_id) -> TabletSharedPtr {
            TabletMetaSharedPtr tablet_meta;
            auto st = meta_mgr()->get_tablet_meta(tablet_id, &tablet_meta);
            if (!st.ok()) {
                LOG(WARNING) << "failed to tablet " << tablet_id << ": " << st;
                return nullptr;
            }

            auto tablet = std::make_shared<Tablet>(std::move(tablet_meta), cloud::cloud_data_dir(),
                                                   tablet_meta->compaction_policy());
            auto value = std::make_unique<Value>(Value {
                    .tablet = tablet,
                    .tablet_map = *_tablet_map,
            });
            // MUST sync stats to let compaction scheduler work correctly
            st = meta_mgr()->sync_tablet_rowsets(tablet.get(), warmup_data);
            if (!st.ok()) {
                LOG(WARNING) << "failed to sync tablet " << tablet_id << ": " << st;
                return nullptr;
            }

            static auto deleter = [](const CacheKey& key, void* value) {
                auto value1 = reinterpret_cast<Value*>(value);
                // tablet has been evicted, release it from `tablet_map`
                value1->tablet_map.erase(value1->tablet.get());
                delete value1;
            };

            auto handle = _cache->insert(key, value.release(), 1, deleter);
            auto ret = std::shared_ptr<Tablet>(
                    tablet.get(), [cache = _cache.get(), handle](...) { cache->release(handle); });
            _tablet_map->put(std::move(tablet));
            return ret;
        };

        *tablet = s_singleflight_load_tablet.load(tablet_id, std::move(load_tablet));
        if (*tablet == nullptr) {
            return Status::InternalError("failed to get tablet {}", tablet_id);
        }
        return Status::OK();
    }

    Tablet* tablet1 = reinterpret_cast<Value*>(_cache->value(handle))->tablet.get();
    *tablet = std::shared_ptr<Tablet>(
            tablet1, [cache = _cache.get(), handle](...) { cache->release(handle); });
    return Status::OK();
}

void CloudTabletMgr::erase_tablet(int64_t tablet_id) {
    auto tablet_id_str = std::to_string(tablet_id);
    CacheKey key(tablet_id_str.data(), tablet_id_str.size());
    _cache->erase(key);
}

void CloudTabletMgr::vacuum_stale_rowsets() {
    LOG_INFO("begin to vacuum stale rowsets");
    std::vector<TabletSharedPtr> tablets_to_vacuum;
    tablets_to_vacuum.reserve(_tablet_map->size());
    _tablet_map->traverse([&tablets_to_vacuum](auto& t) {
        if (t->has_stale_rowsets()) {
            tablets_to_vacuum.push_back(t);
        }
    });
    int num_vacuumed = 0;
    for (auto& t : tablets_to_vacuum) {
        num_vacuumed += t->cloud_delete_expired_stale_rowsets();
    }
    LOG_INFO("finish vacuum stale rowsets").tag("num_vacuumed", num_vacuumed);
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
            ++num_sync;
            auto st = tablet->cloud_sync_meta();
            if (!st) {
                LOG_WARNING("failed to sync tablet meta {}", tablet->tablet_id()).error(st);
                if (st.is<ErrorCode::NOT_FOUND>()) continue;
            }
            st = tablet->cloud_sync_rowsets(-1);
            if (!st) {
                LOG_WARNING("failed to sync tablet rowsets {}", tablet->tablet_id()).error(st);
            }
        }
    }
    LOG_INFO("finish sync tablets").tag("num_sync", num_sync);
}

Status CloudTabletMgr::get_topn_tablets_to_compact(int n, CompactionType compaction_type,
                                                   const std::function<bool(Tablet*)>& filter_out,
                                                   std::vector<TabletSharedPtr>* tablets,
                                                   int64_t* max_score) {
    DCHECK(compaction_type == CompactionType::BASE_COMPACTION ||
           compaction_type == CompactionType::CUMULATIVE_COMPACTION);
    *max_score = 0;
    int64_t max_score_tablet_id = 0;
    // clang-format off
    auto score = [compaction_type](Tablet* t) {
        return compaction_type == CompactionType::BASE_COMPACTION ? t->get_cloud_base_compaction_score()
               : compaction_type == CompactionType::CUMULATIVE_COMPACTION ? t->get_cloud_cumu_compaction_score()
               : 0;
    };

    using namespace std::chrono;
    auto now = duration_cast<milliseconds>(system_clock::now().time_since_epoch()).count();
    auto skip = [now, compaction_type](Tablet* t) {
        if (compaction_type == CompactionType::BASE_COMPACTION) {
            return now - t->last_base_compaction_success_time() < config::base_compaction_interval_seconds_since_last_operation * 1000;
        }
        // If tablet has too many rowsets but not be compacted for a long time, compaction should be performed
        // regardless of whether there is a load job recently.
        return now - t->last_cumu_no_suitable_version_ms() < config::min_compaction_failure_interval_ms ||
               (now - t->last_load_time_ms > config::cu_compaction_freeze_interval_seconds * 1000
               && now - t->last_cumu_compaction_success_time() < config::cumu_compaction_interval_seconds * 1000
               && t->fetch_add_approximate_num_rowsets(0) < config::max_tablet_version_num / 2);
    };
    // We don't schedule tablets that are disabled for compaction
    //auto disable = [](Tablet* t) { return t->tablet_meta()->tablet_schema()->disable_auto_compaction(); };

    auto [num_filtered, num_disabled, num_skipped] = std::make_tuple(0, 0, 0);

    auto weak_tablets = get_weak_tablets();
    std::vector<std::pair<TabletSharedPtr, int64_t>> buf;
    buf.reserve(n + 1);
    for (auto& weak_tablet : weak_tablets) {
        auto t = weak_tablet.lock();
        if (t == nullptr) continue;

        int64_t s = score(t.get());
        if (s <= 0) continue;

        if (s > *max_score) {
            max_score_tablet_id = t->tablet_id();
            *max_score = s;
        }

        if (filter_out(t.get())) { ++num_filtered; continue; }
        //if (disable(t.get())) { ++num_disabled; continue; }
        if (skip(t.get())) { ++num_skipped; continue; }

        buf.push_back({std::move(t), s});
        std::sort(buf.begin(), buf.end(), [](auto& a, auto& b) { return a.second > b.second; });
        if (buf.size() > n) buf.pop_back();
    }

    // log every 10 sec with default config
    LOG_EVERY_N(INFO, 1000) << "get_topn_compaction_score, n=" << n << " type=" << compaction_type
               << " num_tablets=" << weak_tablets.size() << " num_skipped=" << num_skipped
               << " num_disabled=" << num_disabled << " num_filtered=" << num_filtered
               << " max_score=" << *max_score << " max_score_tablet=" << max_score_tablet_id
               << " tablets=[" << [&buf] { std::stringstream ss; for (auto& i : buf) ss << i.first->tablet_id() << ":" << i.second << ","; return ss.str(); }() << "]"
               ;
    // clang-format on

    tablets->clear();
    tablets->reserve(n + 1);
    for (auto& [t, _] : buf) tablets->emplace_back(std::move(t));

    return Status::OK();
}

void CloudTabletMgr::OverlapRowsetsMgr::handle_overlap_rowsets() {
    while (true) {
        RowsetSharedPtr download_rowset;
        Version download_version;
        int64_t tablet_id;
        std::vector<RowsetSharedPtr> overlap_rowsets;
        {
            std::unique_lock lock(mtx);
            if (!closed && tablet_id_to_prepare_overlap_rowsets.empty()) {
                cond.wait(lock, [this]() -> bool {
                    return closed || !tablet_id_to_prepare_overlap_rowsets.empty();
                });
            }
            DCHECK(tablet_id_to_prepare_overlap_rowsets.size() ==
                   tablet_id_to_new_version_to_overlap_rowset.size())
                    << "tablet_id_to_prepare_overlap_rowsets "
                    << tablet_id_to_prepare_overlap_rowsets.size()
                    << "tablet_id_to_new_version_to_overlap_rowset "
                    << tablet_id_to_new_version_to_overlap_rowset.size();
            if (closed) break;
            auto iter = tablet_id_to_prepare_overlap_rowsets.begin();
            DCHECK(tablet_id_to_new_version_to_overlap_rowset.find(iter->first) !=
                   tablet_id_to_new_version_to_overlap_rowset.end());
            auto& new_version_to_overlap_rowset =
                    tablet_id_to_new_version_to_overlap_rowset[iter->first];
            DCHECK(new_version_to_overlap_rowset.size() == iter->second.size())
                    << "prepare_overlap_rowsets " << iter->second.size()
                    << "new_version_to_overlap_rowset " << new_version_to_overlap_rowset.size();
            tablet_id = iter->first;
            auto map_iter = iter->second.rbegin();
            download_version = map_iter->first;
            DCHECK(new_version_to_overlap_rowset.find(download_version) !=
                   new_version_to_overlap_rowset.end());
            download_rowset = map_iter->second;
            downloading_rowset_id = download_rowset->rowset_id();
            overlap_rowsets = std::move(new_version_to_overlap_rowset[download_version]);
            iter->second.erase(download_version);
            new_version_to_overlap_rowset.erase(download_version);
            if (iter->second.empty()) {
                tablet_id_to_new_version_to_overlap_rowset.erase(iter->first);
                tablet_id_to_prepare_overlap_rowsets.erase(iter);
            }
        }
        TabletSharedPtr tablet;
        cloud::tablet_mgr()->get_tablet(tablet_id, &tablet);
        auto tablet_meta = tablet->tablet_meta();
        bool is_hot_data {false};
        std::for_each(overlap_rowsets.cbegin(), overlap_rowsets.cend(),
                      [&](const RowsetSharedPtr& rowset) {
                          is_hot_data = is_hot_data || rowset->is_hot();
                      });
        std::shared_ptr<WaitGroup> wait = std::make_shared<WaitGroup>();
        for (int64_t seg_id = 0; seg_id < download_rowset->num_segments(); seg_id++) {
            io::S3FileMeta download_file_meta;
            download_file_meta.is_cold_data = !is_hot_data;
            auto rowset_meta = download_rowset->rowset_meta();
            download_file_meta.file_size = rowset_meta->get_segment_file_size(seg_id);
            download_file_meta.file_system = rowset_meta->fs();
            download_file_meta.path = io::Path(download_rowset->segment_file_path(seg_id));
            download_file_meta.expiration_time =
                    tablet_meta->ttl_seconds() == 0
                            ? 0
                            : rowset_meta->newest_write_timestamp() + tablet_meta->ttl_seconds();
            download_file_meta.download_callback = [wait](Status st) {
                if (!st.ok()) {
                    LOG_WARNING("handle_overlap_rowsets error").error(st);
                }
                wait->done();
            };
            wait->add();
            io::FileCacheSegmentDownloader::instance()->submit_download_task(
                    std::move(download_file_meta));
        }
        // same as be/src/cloud/io/cloud_file_cache_downloader.cpp:FileCacheSegmentDownloader::polling_download_task():hot_interval
        static const int64_t timeout_seconds = 2 * 60 * 60; // 2 hours
        wait->wait(timeout_seconds);
        {
            std::lock_guard wlock(tablet->get_header_lock());
            tablet->cloud_delete_rowsets(overlap_rowsets);
            tablet->cloud_add_rowsets_async(download_rowset, wlock);
        }
    }
}

void CloudTabletMgr::sumbit_overlap_rowsets(int64_t tablet_id, RowsetSharedPtr to_add,
                                            std::vector<RowsetSharedPtr> to_delete) {
    auto& overlap_rowsets_mgr = _ovarlap_rowsets_mgrs[tablet_id % mgr_size];
    {
        std::lock_guard lock(overlap_rowsets_mgr.mtx);
        if (overlap_rowsets_mgr.downloading_rowset_id == to_add->rowset_id()) {
            return;
        }
        auto to_add_v = to_add->version();
        auto& prepare_overlap_rowsets =
                overlap_rowsets_mgr.tablet_id_to_prepare_overlap_rowsets[tablet_id];
        if (prepare_overlap_rowsets.find(to_add_v) != prepare_overlap_rowsets.end()) {
            return;
        }
        auto& new_version_to_overlap_rowset =
                overlap_rowsets_mgr.tablet_id_to_new_version_to_overlap_rowset[tablet_id];
        auto end_iter = prepare_overlap_rowsets.upper_bound(to_add_v);
        for (auto iter = prepare_overlap_rowsets.begin(); iter != end_iter;) {
            if (to_add_v.contains(iter->first)) {
                new_version_to_overlap_rowset.erase(iter->first);
                prepare_overlap_rowsets.erase(iter);
            } else {
                iter++;
            }
        }
        prepare_overlap_rowsets.insert(std::make_pair(to_add_v, to_add));
        new_version_to_overlap_rowset.insert(std::make_pair(to_add_v, std::move(to_delete)));
    }
    overlap_rowsets_mgr.cond.notify_all();
}

} // namespace doris::cloud
