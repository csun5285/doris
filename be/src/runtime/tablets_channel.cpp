// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "runtime/tablets_channel.h"

#include <fmt/format.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>
#include <time.h>

// IWYU pragma: no_include <opentelemetry/common/threadlocal.h>
#include "common/compiler_util.h" // IWYU pragma: keep
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <initializer_list>
#include <set>
#include <thread>
#include <unordered_set>
#include <utility>

#include "cloud/meta_mgr.h"
#include "cloud/utils.h"
#ifdef DEBUG
#include <unordered_set>
#endif

#include "common/logging.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "olap/delta_writer.h"
#include "olap/storage_engine.h"
#include "olap/txn_manager.h"
#include "runtime/load_channel.h"
#include "util/doris_metrics.h"
#include "util/metrics.h"
#include "vec/core/block.h"

namespace doris {
class SlotDescriptor;

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(tablet_writer_count, MetricUnit::NOUNIT);

std::atomic<uint64_t> TabletsChannel::_s_tablet_writer_count;

TabletsChannel::TabletsChannel(const TabletsChannelKey& key, const UniqueId& load_id,
                               bool is_high_priority, RuntimeProfile* profile)
        : _key(key),
          _state(kInitialized),
          _load_id(load_id),
          _closed_senders(64),
          _is_high_priority(is_high_priority) {
    static std::once_flag once_flag;
    _init_profile(profile);
    std::call_once(once_flag, [] {
        REGISTER_HOOK_METRIC(tablet_writer_count, [&]() { return _s_tablet_writer_count.load(); });
    });
}

TabletsChannel::~TabletsChannel() {
    _s_tablet_writer_count -= _tablet_writers.size();
    for (auto& it : _tablet_writers) {
        delete it.second;
    }
    delete _schema;
}

void TabletsChannel::_init_profile(RuntimeProfile* profile) {
    _profile =
            profile->create_child(fmt::format("TabletsChannel {}", _key.to_string()), true, true);
    _add_batch_number_counter = ADD_COUNTER(_profile, "NumberBatchAdded", TUnit::UNIT);

    auto* memory_usage = _profile->create_child("PeakMemoryUsage", true, true);
    _slave_replica_timer = ADD_TIMER(_profile, "SlaveReplicaTime");
    _add_batch_timer = ADD_TIMER(_profile, "AddBatchTime");
    _write_block_timer = ADD_TIMER(_profile, "WriteBlockTime");
    _memory_usage_counter = memory_usage->AddHighWaterMarkCounter("Total", TUnit::BYTES);
    _write_memory_usage_counter = memory_usage->AddHighWaterMarkCounter("Write", TUnit::BYTES);
    _flush_memory_usage_counter = memory_usage->AddHighWaterMarkCounter("Flush", TUnit::BYTES);
    _max_tablet_memory_usage_counter =
            memory_usage->AddHighWaterMarkCounter("MaxTablet", TUnit::BYTES);
    _max_tablet_write_memory_usage_counter =
            memory_usage->AddHighWaterMarkCounter("MaxTabletWrite", TUnit::BYTES);
    _max_tablet_flush_memory_usage_counter =
            memory_usage->AddHighWaterMarkCounter("MaxTabletFlush", TUnit::BYTES);
}

Status TabletsChannel::open(const PTabletWriterOpenRequest& request,
                            PTabletWriterOpenResult* response) {
    std::lock_guard<std::mutex> l(_lock);
    if (_state == kOpened) {
        // Normal case, already open by other sender
        return Status::OK();
    }
    LOG(INFO) << "open tablets channel: " << _key << ", tablets num: " << request.tablets().size()
              << ", timeout(s): " << request.load_channel_timeout_s();
    _txn_id = request.txn_id();
    _index_id = request.index_id();
    _schema = new OlapTableSchemaParam();
    RETURN_IF_ERROR(_schema->init(request.schema()));
    _tuple_desc = _schema->tuple_desc();

    _num_remaining_senders = request.num_senders();
    _next_seqs.resize(_num_remaining_senders, 0);
    _closed_senders.Reset(_num_remaining_senders);

    RETURN_IF_ERROR(_open_all_writers(request, response));

    _state = kOpened;
    return Status::OK();
}

#ifdef CLOUD_MODE
Status TabletsChannel::close(LoadChannel* parent, bool* finished,
                             const PTabletWriterAddBlockRequest& request,
                             PTabletWriterAddBlockResult* response) {
    std::lock_guard l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    auto sender_id = request.sender_id();
    if (_closed_senders.Get(sender_id)) {
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }
    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id
              << ", backend id: " << request.backend_id();
    for (auto pid : request.partition_ids()) {
        _partition_ids.emplace(pid);
    }
    _closed_senders.Set(sender_id, true);
    _num_remaining_senders--;
    *finished = (_num_remaining_senders == 0);
    if (!*finished) return Status::OK();

    auto tablet_errors = response->mutable_tablet_errors();
    auto tablet_vec = response->mutable_tablet_vec();
    _state = kFinished;
    // All senders are closed
    // 1. close all delta writers
    std::vector<DeltaWriter*> need_wait_writers;
    need_wait_writers.reserve(_tablet_writers.size());
    bool success = true;
    for (auto [tablet_id, writer] : _tablet_writers) {
        if (_partition_ids.count(writer->partition_id()) > 0) {
            if (!success) { // Already failed, cancel all remain writers
                writer->cancel();
                continue;
            }
            auto st = writer->close();
            if (!st.ok()) {
                LOG(WARNING) << "close tablet writer failed, tablet_id=" << tablet_id
                             << ", txn_id=" << _txn_id << ", err=" << st;
                PTabletError* tablet_error = tablet_errors->Add();
                tablet_error->set_tablet_id(tablet_id);
                tablet_error->set_msg(st.to_string());
                success = false;
                _close_status = std::move(st);
                continue;
            }
            // to make sure tablet writer in `_broken_tablets` won't call `close_wait` method.
            if (_broken_tablets.find(writer->tablet_id()) != _broken_tablets.end()) [[unlikely]] {
                LOG(WARNING) << "SHOULD NOT HAPPEN, tablet writer is broken but not cancelled"
                             << ", tablet_id=" << tablet_id << ", txn_id=" << _txn_id;
                success = false;
                _close_status = Status::InternalError(
                        "tablet writer is broken but not cancelled, tablet_id={}", tablet_id);
                continue;
            }
            need_wait_writers.push_back(writer);
        } else {
            auto st = writer->cancel();
            if (!st.ok()) {
                LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << tablet_id
                             << ", txn_id=" << _txn_id;
                // just skip this tablet(writer) and continue to close others
                continue;
            }
        }
    }
    if (!success) return _close_status;

    // 2. wait delta writers
    std::vector<RowsetSharedPtr> rowsets_to_commit;
    rowsets_to_commit.reserve(need_wait_writers.size());
    using namespace std::chrono;
    auto build_start = steady_clock::now();
    for (auto writer : need_wait_writers) {
        RowsetSharedPtr rowset;
        auto st = writer->cloud_build_rowset(&rowset);
        if (!st.ok()) {
            LOG(WARNING) << "failed to close wait DeltaWriter. tablet_id=" << writer->tablet_id()
                         << ", err=" << st;
            PTabletError* tablet_error = tablet_errors->Add();
            tablet_error->set_tablet_id(writer->tablet_id());
            tablet_error->set_msg(st.to_string());
            _close_status = std::move(st);
            return _close_status;
        }
        rowsets_to_commit.push_back(std::move(rowset));
    }
    int64_t build_latency = duration_cast<milliseconds>(steady_clock::now() - build_start).count();
    // 3. commit rowsets to meta-service
    auto commit_start = steady_clock::now();
    std::vector<std::function<Status()>> tasks;
    tasks.reserve(rowsets_to_commit.size());
    for (auto& rs : rowsets_to_commit) {
        tasks.push_back([&rs_meta = rs->rowset_meta()] {
            return cloud::meta_mgr()->commit_rowset(rs_meta.get(), true);
        });
    }
    _close_status = cloud::bthread_fork_and_join(tasks, 10);
    if (!_close_status.ok()) {
        return _close_status;
    }
    int64_t commit_latency =
            duration_cast<milliseconds>(steady_clock::now() - commit_start).count();

    // 4. calculate delete bitmap for Unique Key MoW tables
    for (auto it = need_wait_writers.begin(); it != need_wait_writers.end();) {
        auto st = (*it)->submit_calc_delete_bitmap_task();
        if (!st.ok()) {
            _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
            it = need_wait_writers.erase(it);
            continue;
        }
        it++;
    }

    // 5. wait for delete bitmap calculation complete if necessary
    for (auto it = need_wait_writers.begin(); it != need_wait_writers.end();) {
        Status st = (*it)->wait_calc_delete_bitmap();
        if (!st.ok()) {
            _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
            it = need_wait_writers.erase(it);
            continue;
        }
        it++;
    }

    // 6. set txn related delete bitmap if necessary
    for (auto it = need_wait_writers.begin(); it != need_wait_writers.end();) {
        auto st = (*it)->cloud_set_txn_related_delete_bitmap();
        if (!st.ok()) {
            _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
            it = need_wait_writers.erase(it);
            continue;
        }
        it++;
    }

    tablet_vec->Reserve(need_wait_writers.size());
    for (auto writer : need_wait_writers) {
        PTabletInfo* tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(writer->tablet_id());
        tablet_info->set_schema_hash(writer->schema_hash());
        tablet_info->set_received_rows(writer->total_received_rows());
        tablet_info->set_num_rows_filtered(writer->num_rows_filtered());
        // These stats may be larger than the actual value if the txn is aborted
        writer->update_tablet_stats();
    }
    response->set_build_rowset_latency_ms(build_latency);
    response->set_commit_rowset_latency_ms(commit_latency);
    return Status::OK();
}
#else
Status TabletsChannel::close(
        LoadChannel* parent, int sender_id, int64_t backend_id, bool* finished,
        const google::protobuf::RepeatedField<int64_t>& partition_ids,
        google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
        google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
        const google::protobuf::Map<int64_t, PSlaveTabletNodes>& slave_tablet_nodes,
        google::protobuf::Map<int64_t, PSuccessSlaveTabletNodeIds>* success_slave_tablet_node_ids,
        const bool write_single_replica) {
    std::lock_guard l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    if (_closed_senders.Get(sender_id)) {
        // Double close from one sender, just return OK
        *finished = (_num_remaining_senders == 0);
        return _close_status;
    }
    LOG(INFO) << "close tablets channel: " << _key << ", sender id: " << sender_id
              << ", backend id: " << backend_id;
    for (auto pid : partition_ids) {
        _partition_ids.emplace(pid);
    }
    _closed_senders.Set(sender_id, true);
    _num_remaining_senders--;
    *finished = (_num_remaining_senders == 0);
    if (*finished) {
        _state = kFinished;
        // All senders are closed
        // 1. close all delta writers
        std::set<DeltaWriter*> need_wait_writers;
        for (auto& it : _tablet_writers) {
            if (_partition_ids.count(it.second->partition_id()) > 0) {
                auto st = it.second->close();
                if (!st.ok()) {
                    auto err_msg = fmt::format(
                            "close tablet writer failed, tablet_id={}, "
                            "transaction_id={}, err={}",
                            it.first, _txn_id, st.to_string());
                    LOG(WARNING) << err_msg;
                    PTabletError* tablet_error = tablet_errors->Add();
                    tablet_error->set_tablet_id(it.first);
                    tablet_error->set_msg(st.to_string());
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                // tablet writer in `_broken_tablets` should not call `build_rowset` and
                // `commit_txn` method, after that, the publish-version task will success,
                // which can cause the replica inconsistency.
                if (_is_broken_tablet(it.second->tablet_id())) {
                    LOG(WARNING) << "SHOULD NOT HAPPEN, tablet writer is broken but not cancelled"
                                 << ", tablet_id=" << it.first << ", transaction_id=" << _txn_id;
                    continue;
                }
                need_wait_writers.insert(it.second);
            } else {
                auto st = it.second->cancel();
                if (!st.ok()) {
                    LOG(WARNING) << "cancel tablet writer failed, tablet_id=" << it.first
                                 << ", transaction_id=" << _txn_id;
                    // just skip this tablet(writer) and continue to close others
                    continue;
                }
                VLOG_PROGRESS << "cancel tablet writer successfully, tablet_id=" << it.first
                              << ", transaction_id=" << _txn_id;
            }
        }

        _write_single_replica = write_single_replica;

        // 2. wait all writer finished flush.
        for (auto writer : need_wait_writers) {
            writer->wait_flush();
        }

        // 3. build rowset
        for (auto it = need_wait_writers.begin(); it != need_wait_writers.end();) {
            Status st = (*it)->build_rowset();
            if (!st.ok()) {
                _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
                it = need_wait_writers.erase(it);
                continue;
            }
            // 3.1 calculate delete bitmap for Unique Key MoW tables
            st = (*it)->submit_calc_delete_bitmap_task();
            if (!st.ok()) {
                _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
                it = need_wait_writers.erase(it);
                continue;
            }
            it++;
        }

        // 4. wait for delete bitmap calculation complete if necessary
        for (auto it = need_wait_writers.begin(); it != need_wait_writers.end();) {
            Status st = (*it)->wait_calc_delete_bitmap();
            if (!st.ok()) {
                _add_error_tablet(tablet_errors, (*it)->tablet_id(), st);
                it = need_wait_writers.erase(it);
                continue;
            }
            it++;
        }

        // 5. commit all writers
        for (auto writer : need_wait_writers) {
            PSlaveTabletNodes slave_nodes;
            if (write_single_replica) {
                slave_nodes = slave_tablet_nodes.at(writer->tablet_id());
            }
            // close may return failed, but no need to handle it here.
            // tablet_vec will only contains success tablet, and then let FE judge it.
            _commit_txn(writer, tablet_vec, tablet_errors, slave_nodes, write_single_replica);
        }

        if (write_single_replica) {
            // The operation waiting for all slave replicas to complete must end before the timeout,
            // so that there is enough time to collect completed replica. Otherwise, the task may
            // timeout and fail even though most of the replicas are completed. Here we set 0.9
            // times the timeout as the maximum waiting time.
            SCOPED_TIMER(_slave_replica_timer);
            while (need_wait_writers.size() > 0 &&
                   (time(nullptr) - parent->last_updated_time()) < (parent->timeout() * 0.9)) {
                std::set<DeltaWriter*>::iterator it;
                for (it = need_wait_writers.begin(); it != need_wait_writers.end();) {
                    bool is_done = (*it)->check_slave_replicas_done(success_slave_tablet_node_ids);
                    if (is_done) {
                        need_wait_writers.erase(it++);
                    } else {
                        it++;
                    }
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
            for (auto writer : need_wait_writers) {
                writer->add_finished_slave_replicas(success_slave_tablet_node_ids);
            }
            StorageEngine::instance()->txn_manager()->clear_txn_tablet_delta_writer(_txn_id);
        }
    }
    return Status::OK();
}
#endif

#ifndef CLOUD_MODE
void TabletsChannel::_commit_txn(DeltaWriter* writer,
                                 google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec,
                                 google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors,
                                 PSlaveTabletNodes slave_tablet_nodes,
                                 const bool write_single_replica) {
    Status st = writer->commit_txn(slave_tablet_nodes, write_single_replica);
    if (st.ok()) {
        PTabletInfo* tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(writer->tablet_id());
        tablet_info->set_schema_hash(writer->schema_hash());
        tablet_info->set_received_rows(writer->total_received_rows());
        tablet_info->set_num_rows_filtered(writer->num_rows_filtered());
    } else {
        _add_error_tablet(tablet_errors, writer->tablet_id(), st);
    }
}
#endif // !CLOUD_MODE

void TabletsChannel::_add_error_tablet(
        google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors, int64_t tablet_id,
        Status error) {
    PTabletError* tablet_error = tablet_errors->Add();
    tablet_error->set_tablet_id(tablet_id);
    tablet_error->set_msg(error.to_string());
    VLOG_PROGRESS << "close wait failed tablet " << tablet_id << " transaction_id " << _txn_id
                  << "err msg " << error;
}

int64_t TabletsChannel::mem_consumption() {
    int64_t mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_tablet_writers_lock);
        for (auto& it : _tablet_writers) {
            int64_t writer_mem = it.second->mem_consumption(MemType::ALL);
            mem_usage += writer_mem;
        }
    }
    return mem_usage;
}

void TabletsChannel::refresh_profile() {
    int64_t write_mem_usage = 0;
    int64_t flush_mem_usage = 0;
    int64_t max_tablet_mem_usage = 0;
    int64_t max_tablet_write_mem_usage = 0;
    int64_t max_tablet_flush_mem_usage = 0;
    {
        std::lock_guard<SpinLock> l(_tablet_writers_lock);
        for (auto& it : _tablet_writers) {
            int64_t write_mem = it.second->mem_consumption(MemType::WRITE);
            write_mem_usage += write_mem;
            int64_t flush_mem = it.second->mem_consumption(MemType::FLUSH);
            flush_mem_usage += flush_mem;
            if (write_mem > max_tablet_write_mem_usage) max_tablet_write_mem_usage = write_mem;
            if (flush_mem > max_tablet_flush_mem_usage) max_tablet_flush_mem_usage = flush_mem;
            if (write_mem + flush_mem > max_tablet_mem_usage)
                max_tablet_mem_usage = write_mem + flush_mem;
        }
    }
    COUNTER_SET(_memory_usage_counter, write_mem_usage + flush_mem_usage);
    COUNTER_SET(_write_memory_usage_counter, write_mem_usage);
    COUNTER_SET(_flush_memory_usage_counter, flush_mem_usage);
    COUNTER_SET(_max_tablet_memory_usage_counter, max_tablet_mem_usage);
    COUNTER_SET(_max_tablet_write_memory_usage_counter, max_tablet_write_mem_usage);
    COUNTER_SET(_max_tablet_flush_memory_usage_counter, max_tablet_flush_mem_usage);
}

void TabletsChannel::get_active_memtable_mem_consumption(
        std::multimap<int64_t, int64_t, std::greater<int64_t>>* mem_consumptions) {
    mem_consumptions->clear();
    std::lock_guard<SpinLock> l(_tablet_writers_lock);
    for (auto& it : _tablet_writers) {
        int64_t active_memtable_mem = it.second->active_memtable_mem_consumption();
        mem_consumptions->emplace(active_memtable_mem, it.first);
    }
}

Status TabletsChannel::_open_all_writers(const PTabletWriterOpenRequest& request,
                                         PTabletWriterOpenResult* response) {
#ifdef CLOUD_MODE
    DCHECK_GT(request.txn_expiration(), 0);
#endif
    std::vector<SlotDescriptor*>* index_slots = nullptr;
    int32_t schema_hash = 0;
    for (auto& index : _schema->indexes()) {
        if (index->index_id == _index_id) {
            index_slots = &index->slots;
            schema_hash = index->schema_hash;
            break;
        }
    }
    if (index_slots == nullptr) {
        Status::InternalError("unknown index id, key={}", _key.to_string());
    }

#ifdef DEBUG
    // check: tablet ids should be unique
    {
        std::unordered_set<int64_t> tablet_ids;
        for (const auto& tablet : request.tablets()) {
            CHECK(tablet_ids.count(tablet.tablet_id()) == 0)
                    << "found duplicate tablet id: " << tablet.tablet_id();
            tablet_ids.insert(tablet.tablet_id());
        }
    }
#endif

    auto tablet_load_infos = response->mutable_tablet_load_rowset_num_infos();
    for (auto& tablet : request.tablets()) {
        WriteRequest wrequest;
        wrequest.index_id = request.index_id();
        wrequest.tablet_id = tablet.tablet_id();
        wrequest.schema_hash = schema_hash;
        wrequest.write_type = WriteType::LOAD;
        wrequest.txn_id = _txn_id;
        wrequest.partition_id = tablet.partition_id();
        wrequest.load_id = request.id();
        wrequest.tuple_desc = _tuple_desc;
        wrequest.slots = index_slots;
        wrequest.is_high_priority = _is_high_priority;
        wrequest.table_schema_param = _schema;
        wrequest.txn_expiration = request.txn_expiration(); // Required by CLOUD

        DeltaWriter* writer = nullptr;
        auto st = DeltaWriter::open(&wrequest, &writer, _profile, _load_id);
        if (!st.ok()) {
            auto err_msg = fmt::format(
                    "open delta writer failed, tablet_id={}"
                    ", txn_id={}, partition_id={}, err={}",
                    tablet.tablet_id(), _txn_id, tablet.partition_id(), st.to_string());
            LOG(WARNING) << err_msg;
            return Status::InternalError(err_msg);
        }
        {
            std::lock_guard<SpinLock> l(_tablet_writers_lock);
            _tablet_writers.emplace(tablet.tablet_id(), writer);
        }
        TabletSharedPtr tabletsptr = nullptr;
        cloud::tablet_mgr()->get_tablet(tablet.tablet_id(), &tabletsptr);
        if (tabletsptr == nullptr) [[unlikely]] {
            continue;
        }
        if (auto version_cnt = tabletsptr->fetch_add_approximate_num_rowsets(0);
            UNLIKELY(version_cnt > (config::max_tablet_version_num / 2))) {
            auto load_info = tablet_load_infos->Add();
            load_info->set_current_rowset_nums(version_cnt);
            load_info->set_max_config_rowset_nums(config::max_tablet_version_num);
        }
    }

    _s_tablet_writer_count += _tablet_writers.size();
    DCHECK_EQ(_tablet_writers.size(), request.tablets_size());
    return Status::OK();
}

Status TabletsChannel::_init_writes_by_parition_ids(
        const std::unordered_set<int64_t>& partition_ids) {
    std::vector<DeltaWriter*> writers;
    for (auto [tablet_id, writer] : _tablet_writers) {
        if (partition_ids.count(writer->partition_id()) && !writer->initialized()) {
            writers.push_back(writer);
        }
    }
    if (writers.size()) {
        RETURN_IF_ERROR(DeltaWriter::init(writers));
    }
    return Status::OK();
}

Status TabletsChannel::cancel() {
    std::lock_guard l(_lock);
    if (_state == kFinished) {
        return _close_status;
    }
    for (auto& it : _tablet_writers) {
        it.second->cancel();
    }
    _state = kFinished;
    if (_write_single_replica) {
        StorageEngine::instance()->txn_manager()->clear_txn_tablet_delta_writer(_txn_id);
    }
    return Status::OK();
}

std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(load_id=" << key.id << ", index_id=" << key.index_id << ")";
    return os;
}

Status TabletsChannel::add_batch(const PTabletWriterAddBlockRequest& request,
                                 PTabletWriterAddBlockResult* response) {
    SCOPED_TIMER(_add_batch_timer);
    int64_t cur_seq = 0;
    _add_batch_number_counter->update(1);

    auto status = _get_current_seq(cur_seq, request);
    if (UNLIKELY(!status.ok())) {
        return status;
    }

    if (request.packet_seq() < cur_seq) {
        LOG(INFO) << "packet has already recept before, expect_seq=" << cur_seq
                  << ", recept_seq=" << request.packet_seq();
        return Status::OK();
    }

    std::unordered_map<int64_t /* tablet_id */, std::vector<int> /* row index */> tablet_to_rowidxs;
    for (int i = 0; i < request.tablet_ids_size(); ++i) {
        if (request.is_single_tablet_block()) {
            break;
        }
        int64_t tablet_id = request.tablet_ids(i);
        if (_is_broken_tablet(tablet_id)) {
            // skip broken tablets
            VLOG_PROGRESS << "skip broken tablet tablet=" << tablet_id;
            continue;
        }
        auto it = tablet_to_rowidxs.find(tablet_id);
        if (it == tablet_to_rowidxs.end()) {
            tablet_to_rowidxs.emplace(tablet_id, std::initializer_list<int> {i});
        } else {
            it->second.emplace_back(i);
        }
    }

    // In CLOUD_MODE, init DeltaWriter will issue RPC, so it's necessary to init writers by batch.
    std::unordered_set<int64_t> partition_ids;
    for (auto& [tablet_id, _] : tablet_to_rowidxs) {
        auto tablet_writer_it = _tablet_writers.find(tablet_id);
        if (tablet_writer_it == _tablet_writers.end()) {
            return Status::InternalError("unknown tablet to append data, tablet={}", tablet_id);
        }
        partition_ids.insert(tablet_writer_it->second->partition_id());
    }
    if (partition_ids.size()) {
        RETURN_IF_ERROR(_init_writes_by_parition_ids(partition_ids));
    }

    vectorized::Block send_data;
    RETURN_IF_ERROR(send_data.deserialize(request.block()));

    CHECK(send_data.rows() == request.tablet_ids_size())
            << "block rows: " << send_data.rows()
            << ", tablet_ids_size: " << request.tablet_ids_size();

    auto tablet_load_infos = response->mutable_tablet_load_rowset_num_infos();
    auto write_tablet_data = [&](uint32_t tablet_id,
                                 std::function<Status(DeltaWriter * writer)> write_func) {
        google::protobuf::RepeatedPtrField<PTabletError>* tablet_errors =
                response->mutable_tablet_errors();
        auto tablet_writer_it = _tablet_writers.find(tablet_id);
        if (tablet_writer_it == _tablet_writers.end()) {
            return Status::InternalError("unknown tablet to append data, tablet={}", tablet_id);
        }
        Status st = write_func(tablet_writer_it->second);
        if (!st.ok()) {
            auto err_msg =
                    fmt::format("tablet writer write failed, tablet_id={}, txn_id={}, err={}",
                                tablet_id, _txn_id, st.to_string());
            LOG(WARNING) << err_msg;
            PTabletError* error = tablet_errors->Add();
            error->set_tablet_id(tablet_id);
            error->set_msg(err_msg);
            tablet_writer_it->second->cancel_with_status(st);
            _add_broken_tablet(tablet_id);
            // continue write to other tablet.
            // the error will return back to sender.
        }
        tablet_writer_it->second->set_tablet_load_rowset_num_info(tablet_load_infos);
        return Status::OK();
    };

    if (request.is_single_tablet_block()) {
        SCOPED_TIMER(_write_block_timer);
        RETURN_IF_ERROR(write_tablet_data(request.tablet_ids(0), [&](DeltaWriter* writer) {
            return writer->append(&send_data);
        }));
    } else {
        SCOPED_TIMER(_write_block_timer);
        for (const auto& tablet_to_rowidxs_it : tablet_to_rowidxs) {
            RETURN_IF_ERROR(write_tablet_data(tablet_to_rowidxs_it.first, [&](DeltaWriter* writer) {
                return writer->write(&send_data, tablet_to_rowidxs_it.second);
            }));
        }
    }

    {
        std::lock_guard l(_lock);
        _next_seqs[request.sender_id()] = cur_seq + 1;
    }
    return Status::OK();
}

void TabletsChannel::flush_memtable_async(int64_t tablet_id) {
    std::lock_guard l(_lock);
    if (_state == kFinished) {
        // TabletsChannel is closed without LoadChannel's lock,
        // therefore it's possible for reduce_mem_usage() to be called right after close()
        LOG(INFO) << "TabletsChannel is closed when reduce mem usage, txn_id: " << _txn_id
                  << ", index_id: " << _index_id;
        return;
    }

    auto iter = _tablet_writers.find(tablet_id);
    if (iter == _tablet_writers.end()) {
        return;
    }

    if (!(_reducing_tablets.insert(tablet_id).second)) {
        return;
    }

    Status st = iter->second->flush_memtable_and_wait(false);
    if (!st.ok()) {
        auto err_msg = fmt::format(
                "tablet writer failed to reduce mem consumption by flushing memtable, "
                "tablet_id={}, txn_id={}, err={}",
                tablet_id, _txn_id, st.to_string());
        LOG(WARNING) << err_msg;
        iter->second->cancel_with_status(st);
        _add_broken_tablet(tablet_id);
    }
}

void TabletsChannel::wait_flush(int64_t tablet_id) {
    {
        std::lock_guard<std::mutex> l(_lock);
        if (_state == kFinished) {
            // TabletsChannel is closed without LoadChannel's lock,
            // therefore it's possible for reduce_mem_usage() to be called right after close()
            LOG(INFO) << "TabletsChannel is closed when reduce mem usage, txn_id: " << _txn_id
                      << ", index_id: " << _index_id;
            return;
        }
    }

    auto iter = _tablet_writers.find(tablet_id);
    if (iter == _tablet_writers.end()) {
        return;
    }
    Status st = iter->second->wait_flush();
    if (!st.ok()) {
        auto err_msg = fmt::format(
                "tablet writer failed to reduce mem consumption by flushing memtable, "
                "tablet_id={}, txn_id={}, err={}",
                tablet_id, _txn_id, st.to_string());
        LOG(WARNING) << err_msg;
        iter->second->cancel_with_status(st);
        _add_broken_tablet(tablet_id);
    }

    {
        std::lock_guard<std::mutex> l(_lock);
        _reducing_tablets.erase(tablet_id);
    }
}
void TabletsChannel::_add_broken_tablet(int64_t tablet_id) {
    std::unique_lock<std::shared_mutex> wlock(_broken_tablets_lock);
    _broken_tablets.insert(tablet_id);
}

bool TabletsChannel::_is_broken_tablet(int64_t tablet_id) {
    std::shared_lock<std::shared_mutex> rlock(_broken_tablets_lock);
    return _broken_tablets.find(tablet_id) != _broken_tablets.end();
}
} // namespace doris
