
// clang-format off
#include "txn_kv.h"

#include "common/bvars.h"
#include "common/config.h"
#include "common/logging.h"
#include "common/stopwatch.h"
#include "common/util.h"

#include <atomic>
#include <iomanip>
#include <memory>
#include <sstream>
#include <thread>
#include <cstring>
// clang-format on

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

namespace selectdb {

int FdbTxnKv::init() {
    network_ = std::make_shared<fdb::Network>(FDBNetworkOption {});
    int ret = network_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init network";
        return ret;
    }
    database_ = std::make_shared<fdb::Database>(network_, config::fdb_cluster_file_path,
                                                FDBDatabaseOption {});
    ret = database_->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init database";
        return ret;
    }
    return 0;
}

int FdbTxnKv::create_txn(std::unique_ptr<Transaction>* txn) {
    auto t = new fdb::Transaction(database_, FDBTransactionOption {});
    txn->reset(t);
    int ret = t->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txn, ret=" << ret;
        return ret;
    }
    return 0;
}

} // namespace selectdb

namespace selectdb::fdb {

// =============================================================================
// Impl of Network
// =============================================================================
decltype(Network::working) Network::working {false};

int Network::init() {
    // Globaly once
    bool expected = false;
    if (!Network::working.compare_exchange_strong(expected, true)) return 1;

    fdb_error_t err = fdb_select_api_version(fdb_get_max_api_version());

    // Setup network thread
    // Optional setting
    // FDBNetworkOption opt;
    // fdb_network_set_option()
    (void)opt_;
    // ATTN: Network can be configured only once,
    //       even if fdb_stop_network() is called successfully
    err = fdb_setup_network(); // Must be called only once before any
                               // other functions of C-API
    if (err) {
        LOG(WARNING) << "failed to setup fdb network, err: " << fdb_get_error(err);
        return 1;
    }

    // Network complete callback is optional, and useful for some cases
    //   std::function<void()> network_complete_callback =
    //                         []() { std::cout << __PRETTY_FUNCTION__ << std::endl; };
    //   err = fdb_add_network_thread_completion_hook(callback1,
    //                                                &network_complete_callback);
    //   std::cout << "fdb_add_network_thread_completion_hook error: "
    //     << fdb_get_error(err) << std::endl;
    //   if (err) std::exit(-1);

    // Run network in a separate thread
    network_thread_ = std::shared_ptr<std::thread>(
            new std::thread([] {
                // Will not return until fdb_stop_network() called
                auto err = fdb_run_network();
                LOG(WARNING) << "exit fdb_run_network"
                    << (err ? std::string(", error: ") + fdb_get_error(err) : "");
                }
            ),
            [](auto* p) {
                auto err = fdb_stop_network();
                LOG(WARNING) << "fdb_stop_network"
                    << (err ? std::string(", error: ") + fdb_get_error(err) : "");
                p->join();
                delete p;

                // Another network will work only after this thread exits
                bool expected = true;
                Network::working.compare_exchange_strong(expected, false);
            });

    return 0;
}

void Network::stop() {
    network_thread_.reset();
}

// =============================================================================
// Impl of Database
// =============================================================================

int Database::init() {
    // TODO: process opt
    fdb_error_t err = fdb_create_database(cluster_file_path_.c_str(), &db_);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__ << " fdb_create_database error: " << fdb_get_error(err)
                     << " conf: " << cluster_file_path_;
        return 1;
    }

    return 0;
}

// =============================================================================
// Impl of Trasaction
// =============================================================================


int Transaction::init() {
    // TODO: process opt
    fdb_error_t err = fdb_database_create_transaction(db_->db(), &txn_);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " fdb_database_create_transaction error:" << fdb_get_error(err);
        return -1;
    }
    return 0;
}

int Transaction::begin() {
    return 0;
}

void Transaction::put(std::string_view key, std::string_view val) {
    StopWatch sw;
    fdb_transaction_set(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val.data(), val.size());
    g_bvar_txn_kv_put << sw.elapsed_us();
}

int Transaction::get(std::string_view key, std::string* val, bool snapshot) {
    StopWatch sw;
    auto fut = fdb_transaction_get(txn_, (uint8_t*)key.data(), key.size(), snapshot);

    auto release_fut = [fut, &sw](int*) { fdb_future_destroy(fut); g_bvar_txn_kv_get << sw.elapsed_us(); };
    std::unique_ptr<int, decltype(release_fut)> defer((int*)0x01, std::move(release_fut));

    auto err = fdb_future_block_until_ready(fut);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_block_until_ready err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return -1;
    }
    err = fdb_future_get_error(fut);
    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_get_error err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return -2;
    }

    fdb_bool_t found;
    const uint8_t* ret;
    int len;
    err = fdb_future_get_value(fut, &found, &ret, &len);

    if (err) {
        LOG(WARNING) << __PRETTY_FUNCTION__
                     << " failed to fdb_future_get_value err=" << fdb_get_error(err)
                     << " key=" << hex(key);
        return -1;
    }

    if (!found) return 1;
    *val = std::string((char*)ret, len);
    return 0;
}

int Transaction::get(std::string_view begin, std::string_view end,
                     std::unique_ptr<selectdb::RangeGetIterator>* iter, bool snapshot, int limit) {
    StopWatch sw;
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&sw](int*) { g_bvar_txn_kv_range_get << sw.elapsed_us();});

    FDBFuture* fut = fdb_transaction_get_range(
            txn_, FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)begin.data(), begin.size()),
            FDB_KEYSEL_FIRST_GREATER_OR_EQUAL((uint8_t*)end.data(), end.size()), limit,
            0 /*target_bytes, unlimited*/, FDBStreamingMode::FDB_STREAMING_MODE_WANT_ALL,
            //       FDBStreamingMode::FDB_STREAMING_MODE_ITERATOR,
            0 /*iteration*/, snapshot, false /*reverse*/);

    auto err = fdb_future_block_until_ready(fut);
    if (err) {
        LOG(WARNING) << fdb_get_error(err);
        return -1;
    }
    err = fdb_future_get_error(fut);
    if (err) {
        LOG(WARNING) << fdb_get_error(err);
        return -2;
    }

    std::unique_ptr<RangeGetIterator> ret(new RangeGetIterator(fut));
    ret->init();

    *(iter) = std::move(ret);

    return 0;
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    StopWatch sw;
    std::unique_ptr<std::string> key(new std::string(key_prefix));
    int prefix_size = key->size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    key->resize(key->size() + 14, '\0');
    std::memcpy(key->data() + (key->size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key->data(), key->size(), (uint8_t*)val.data(),
                              val.size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_KEY);

    kv_pool_.push_back(std::move(key));
    g_bvar_txn_kv_atomic_set_ver_key << sw.elapsed_us();
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    StopWatch sw;
    std::unique_ptr<std::string> val(new std::string(value));
    int prefix_size = val->size();
    // ATTN:
    // 10 bytes for versiontimestamp must be 0, trailing 4 bytes is for prefix len
    val->resize(val->size() + 14, '\0');
    std::memcpy(val->data() + (val->size() - 4), &prefix_size, 4);

    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val->data(),
                              val->size(),
                              FDBMutationType::FDB_MUTATION_TYPE_SET_VERSIONSTAMPED_VALUE);

    kv_pool_.push_back(std::move(val));
    g_bvar_txn_kv_atomic_set_ver_value << sw.elapsed_us();
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    StopWatch sw;
    auto val = std::make_unique<std::string>(sizeof(to_add), '\0');
    std::memcpy(val->data(), &to_add, val->size());
    fdb_transaction_atomic_op(txn_, (uint8_t*)key.data(), key.size(), (uint8_t*)val->data(),
                              val->size(), FDBMutationType::FDB_MUTATION_TYPE_ADD);

    kv_pool_.push_back(std::move(val));
    g_bvar_txn_kv_atomic_add << sw.elapsed_us();
}

void Transaction::remove(std::string_view key) {
    StopWatch sw;
    fdb_transaction_clear(txn_, (uint8_t*)key.data(), key.size());
    g_bvar_txn_kv_remove << sw.elapsed_us();
}

void Transaction::remove(std::string_view begin, std::string_view end) {
    StopWatch sw;
    fdb_transaction_clear_range(txn_, (uint8_t*)begin.data(), begin.size(), (uint8_t*)end.data(),
                                end.size());
    g_bvar_txn_kv_range_remove << sw.elapsed_us();
}

int Transaction::commit() {
    StopWatch sw;
    auto fut = fdb_transaction_commit(txn_);
    auto release_fut = [fut, &sw](int*) { fdb_future_destroy(fut); g_bvar_txn_kv_commit << sw.elapsed_us(); };
    std::unique_ptr<int, decltype(release_fut)> defer((int*)0x01, std::move(release_fut));
    auto err = fdb_future_block_until_ready(fut);
    if (err) {
        LOG(WARNING) << "fdb commit error, code=" << err <<  " msg=" << fdb_get_error(err);
        err == 1020 ? g_bvar_txn_kv_commit_conflict_counter << 1 : g_bvar_txn_kv_commit_error_counter << 1;
        return err == 1020 ? -1 : -2;
    }
    err = fdb_future_get_error(fut);
    if (err) {
        LOG(WARNING) << "fdb commit error, code=" << err <<  " msg=" << fdb_get_error(err);
        err == 1020 ? g_bvar_txn_kv_commit_conflict_counter << 1 : g_bvar_txn_kv_commit_error_counter << 1;
        return err == 1020 ? -1 : -2;
    }
    return 0;
}

int64_t Transaction::get_read_version() {
    StopWatch sw;
    auto fut = fdb_transaction_get_read_version(txn_);
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                          [fut, &sw](...) { fdb_future_destroy(fut);
                                                                            g_bvar_txn_kv_get_read_version << sw.elapsed_us(); });
    auto err = fdb_future_block_until_ready(fut);
    if (err) {
        LOG(WARNING) << " " << fdb_get_error(err);
        return -1;
    }
    err = fdb_future_get_error(fut);
    if (err) {
        LOG(WARNING) << " " << fdb_get_error(err);
        return -2;
    }
    int64_t ver;
    err = fdb_future_get_int64(fut, &ver);
    if (err) {
        LOG(WARNING) << " " << fdb_get_error(err);
        return -3;
    }
    return ver;
}

int64_t Transaction::get_committed_version() {
    StopWatch sw;
    int64_t ver;
    auto err = fdb_transaction_get_committed_version(txn_, &ver);
    if (err) {
        LOG(WARNING) << " " << fdb_get_error(err);
        g_bvar_txn_kv_get_committed_version << sw.elapsed_us();
        return -1;
    }
    g_bvar_txn_kv_get_committed_version << sw.elapsed_us();
    return ver;
}

int Transaction::abort() {
    return 0;
}

} // namespace selectdb::fdb
// vim: et tw=100 ts=4 sw=4 cc=80:
