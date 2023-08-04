
#pragma once

// clang-format off
#define FDB_API_VERSION 710
#include "foundationdb/fdb_c.h"
#include "foundationdb/fdb_c_options.g.h"

#include <iostream>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <vector>
// clang-format on

// =============================================================================
//
// =============================================================================

namespace selectdb {

class Transaction;
class RangeGetIterator;

class TxnKv {
public:
    TxnKv() = default;
    virtual ~TxnKv() = default;

    /**
     * Creates a transaction
     * TODO: add options to create the txn
     *
     * @param txn output param
     * @return 0 for success
     */
    virtual int create_txn(std::unique_ptr<Transaction>* txn) = 0;

    virtual int init() = 0;
};

class Transaction {
public:
    Transaction() = default;
    virtual ~Transaction() = default;

    virtual int begin() = 0;

    virtual void put(std::string_view key, std::string_view val) = 0;

    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return 0 for success get a key, 1 for key not found, negative for error
     */
    virtual int get(std::string_view key, std::string* val, bool snapshot = false) = 0;

    /**
     * Closed-open range
     * @param snapshot if true, key range will not be included in txn conflict detection this time
     * @param limit if non-zero, indicates the maximum number of key-value pairs to return
     * @return 0 for success, negative for error
     */
    virtual int get(std::string_view begin, std::string_view end,
                    std::unique_ptr<RangeGetIterator>* iter, bool snapshot = false,
                    int limit = 10000) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) = 0;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    virtual void atomic_set_ver_value(std::string_view key, std::string_view val) = 0;

    /**
     * Adds a value to database
     * @param to_add positive for addition, negative for substraction
     * @return 0 for success otherwise error
     */
    virtual void atomic_add(std::string_view key, int64_t to_add) = 0;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    virtual void remove(std::string_view key) = 0;

    /**
     * Remove a closed-open range
     */
    virtual void remove(std::string_view begin, std::string_view end) = 0;

    /**
     *
     *@return 0 for success otehrwise error
     */
    virtual int commit() = 0;

    /**
     * Gets the read version used by the txn.
     * Note that it does not make any sense we call this function before
     * any `Transaction::get()` is called.
     *
     *@return positive number for success otehrwise error
     */
    virtual int64_t get_read_version() = 0;

    /**
     * Gets the commited version used by the txn.
     * Note that it does not make any sense we call this function before
     * a successful call to `Transaction::commit()`.
     *
     *@return positive number for success otehrwise error
     */
    virtual int64_t get_committed_version() = 0;

    /**
     * Aborts this transaction
     *
     * @returns 0 for success, otherwise non-zero
     */
    virtual int abort() = 0;
};

class RangeGetIterator {
public:
    RangeGetIterator() = default;
    virtual ~RangeGetIterator() = default;

    /**
     * Checks if we can call `next()` on this iterator.
     */
    virtual bool has_next() = 0;

    /**
     * Gets next element, this is usually called after a check of `has_next()` succeeds,
     * If `has_next()` is not checked, the return value may be undefined.
     *
     * @return a kv pair
     */
    virtual std::pair<std::string_view, std::string_view> next() = 0;

    /**
     * Checks if there are more KVs to be get from the range, caller usually wants
     * to issue a nother `get` with the last key of this iteration.
     *
     * @return if there are more kvs that this iterator cannot cover
     */
    virtual bool more() = 0;

    /**
     * 
     * Gets size of the range, some kinds of iterators may not support this function.
     *
     * @return size
     */
    virtual int size() = 0;

    /**
     * Resets to initial state, some kinds of iterators may not support this function.
     *
     * @returns 0 for success otherwise failure
     */
    virtual int reset() = 0;

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;
};

// =============================================================================
//  FoundationDB implementation of TxnKv
// =============================================================================

namespace fdb {
class Database;
class Transaction;
class Network;
} // namespace fdb

class FdbTxnKv : public TxnKv {
public:
    FdbTxnKv() = default;
    ~FdbTxnKv() override = default;

    int create_txn(std::unique_ptr<Transaction>* txn) override;

    int init() override;

private:
    std::shared_ptr<fdb::Network> network_;
    std::shared_ptr<fdb::Database> database_;
};

namespace fdb {

class Network {
public:
    Network(FDBNetworkOption opt) : opt_(opt) {}

    /**
     * @return 0 for success otherwise non-zero
     */
    int init();

    /**
     * Notify the newwork thread to stop, this is an async. call, check 
     * Network::working to ensure the network exited finaly.
     *
     * FIXME: may be we can implement it as a sync. function.
     */
    void stop();

    ~Network() = default;

private:
    std::shared_ptr<std::thread> network_thread_;
    FDBNetworkOption opt_;

    // Global state, only one instance of Network is allowed
    static std::atomic<bool> working;
};

class Database {
public:
    Database(std::shared_ptr<Network> net, const std::string& cluster_file, FDBDatabaseOption opt)
            : network_(std::move(net)), cluster_file_path_(cluster_file), opt_(std::move(opt)) {}

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    ~Database() {
        if (db_ != nullptr) fdb_database_destroy(db_);
    }

    FDBDatabase* db() { return db_; };

    std::shared_ptr<Transaction> create_txn(FDBTransactionOption opt);

private:
    std::shared_ptr<Network> network_;
    std::string cluster_file_path_;
    FDBDatabase* db_ = nullptr;
    FDBDatabaseOption opt_;
};

class RangeGetIterator : public selectdb::RangeGetIterator {
public:
    /**
     * Iterator takes the ownership of input future
     */
    RangeGetIterator(FDBFuture* fut, bool owns = true)
            : fut_(fut), owns_fut_(owns), kvs_(nullptr), kvs_size_(-1), more_(false), idx_(-1) {}

    int init() {
        if (fut_ == nullptr) return -1;
        idx_ = 0;
        kvs_size_ = 0;
        more_ = false;
        kvs_ = nullptr;
        auto err = fdb_future_get_keyvalue_array(fut_, &kvs_, &kvs_size_, &more_);
        if (err) {
            std::cerr << fdb_get_error(err) << std::endl;
            return -1;
        }
        return 0;
    }

    RangeGetIterator(RangeGetIterator&& o) {
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
        fut_ = o.fut_;
        o.fut_ = nullptr;
    }

    ~RangeGetIterator() override {
        // Release all memory
        if (fut_ && owns_fut_) fdb_future_destroy(fut_);
    }

    std::pair<std::string_view, std::string_view> next() override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};

        auto inc = [this](int*) { ++idx_; };
        std::unique_ptr<int, decltype(inc)> defer((int*)0x01, std::move(inc));

        return {{(char*)kvs_[idx_].key, (size_t)kvs_[idx_].key_length},
                {(char*)kvs_[idx_].value, (size_t)kvs_[idx_].value_length}};
    }

    bool has_next() override { return (idx_ < kvs_size_); }

    /**
     * Check if there are more KVs to be get from the range, caller usually wants
     * to issue a nother `get` with the last key of this iteration.
     */
    bool more() override { return more_; }

    int size() override { return kvs_size_; }

    int reset() override {
        idx_ = 0;
        return 0;
    }

    RangeGetIterator(const RangeGetIterator&) = delete;
    RangeGetIterator& operator=(const RangeGetIterator&) = delete;

private:
    FDBFuture* fut_;
    bool owns_fut_;
    const FDBKeyValue* kvs_;
    int kvs_size_;
    fdb_bool_t more_;
    int idx_;
};

class Transaction : public selectdb::Transaction {
public:
    friend class Database;
    Transaction(std::shared_ptr<Database> db) : db_(std::move(db)) {}

    ~Transaction() override {
        if (txn_) fdb_transaction_destroy(txn_);
    }

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    int begin() override;

    void put(std::string_view key, std::string_view val) override;

    using selectdb::Transaction::get;
    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return 0 for success get a key, 1 for key not found, negative for error
     */
    int get(std::string_view key, std::string* val, bool snapshot = false) override;
    /**
     * Closed-open range
     * @param snapshot if true, key range will not be included in txn conflict detection this time
     * @param limit if non-zero, indicates the maximum number of key-value pairs to return
     * @return 0 for success, negative for error
     */
    int get(std::string_view begin, std::string_view end,
            std::unique_ptr<selectdb::RangeGetIterator>* iter, bool snapshot = false,
            int limit = 10000) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `key_prefix + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key_prefix prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_key(std::string_view key_prefix, std::string_view val) override;

    /**
     * Put a key-value pair in which key will in the form of
     * `value + versiontimestamp`. `versiontimestamp` is autogenerated by the
     * system and it's 10-byte long and encoded in big-endian
     *
     * @param key prefix for key convertion, can be zero-length
     * @param val value
     */
    void atomic_set_ver_value(std::string_view key, std::string_view val) override;

    /**
     * Adds a value to database
     * @param to_add positive for addition, negative for substraction
     * @return 0 for success otherwise error
     */
    void atomic_add(std::string_view key, int64_t to_add) override;
    // TODO: min max or and xor cmp_and_clear set_ver_value

    void remove(std::string_view key) override;

    /**
     * Remove a closed-open range
     */
    void remove(std::string_view begin, std::string_view end) override;

    /**
     *
     *@return 0 for success, -1 for conflict, -2 for otehrwise error
     */
    int commit() override;

    int64_t get_read_version() override;
    int64_t get_committed_version() override;

    int abort() override;

private:
    std::shared_ptr<Database> db_ {nullptr};
    bool commited_ = false;
    bool aborted_ = false;
    FDBTransaction* txn_ = nullptr;
    std::vector<std::unique_ptr<std::string>> kv_pool_;
};

} // namespace fdb

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
