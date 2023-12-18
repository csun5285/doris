#pragma once

#include <cstdint>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

#include "meta-service/txn_kv_error.h"
#include "txn_kv.h"
namespace selectdb {

namespace memkv {
class Transaction;
enum class ModifyOpType;
} // namespace memkv

class MemTxnKv : public TxnKv, public std::enable_shared_from_this<MemTxnKv> {
    friend class memkv::Transaction;

public:
    MemTxnKv() = default;
    ~MemTxnKv() override = default;

    TxnErrorCode create_txn(std::unique_ptr<Transaction>* txn) override;

    int init() override;

    TxnErrorCode get_kv(const std::string& key, std::string* val, int64_t version);
    TxnErrorCode get_kv(const std::string& begin, const std::string& end, int64_t version,
                        int limit, bool* more, std::map<std::string, std::string>* kv_list);

private:
    using OpTuple = std::tuple<memkv::ModifyOpType, std::string, std::string>;
    TxnErrorCode update(const std::set<std::string>& read_set, const std::vector<OpTuple>& op_list,
                        int64_t read_version, int64_t* committed_version);

    int get_kv(std::map<std::string, std::string>* kv, int64_t* version);

    int64_t get_last_commited_version();
    int64_t get_last_read_version();

    static int gen_version_timestamp(int64_t ver, int16_t seq, std::string* str);

    struct LogItem {
        memkv::ModifyOpType op_;
        int64_t commit_version_;

        // for get's op: key=key, value=""
        // for range get's op: key=begin, value=end
        // for atomic_set_ver_key/atomic_set_ver_value's op: key=key, value=value
        // for atomic_add's op: key=key, value=to_add
        // for remove's op: key=key, value=""
        // for range remove's op: key=begin, value=end
        std::string key;
        std::string value;
    };

    struct Version {
        int64_t commit_version;
        std::optional<std::string> value;
    };

    std::map<std::string, std::list<Version>> mem_kv_;
    std::unordered_map<std::string, std::list<LogItem>> log_kv_;
    std::mutex lock_;
    int64_t committed_version_ = 0;
    int64_t read_version_ = 0;
};

namespace memkv {

enum class ModifyOpType {
    PUT,
    ATOMIC_SET_VER_KEY,
    ATOMIC_SET_VER_VAL,
    ATOMIC_ADD,
    REMOVE,
    REMOVE_RANGE
};

class Transaction : public selectdb::Transaction {
public:
    Transaction(std::shared_ptr<MemTxnKv> kv);

    ~Transaction() override = default;

    /**
     *
     * @return 0 for success otherwise false
     */
    int init();

    void put(std::string_view key, std::string_view val) override;

    using selectdb::Transaction::get;
    /**
     * @param snapshot if true, `key` will not be included in txn conflict detection this time
     * @return TXN_OK for success get a key, TXN_KEY_NOT_FOUND for key not found, otherwise for error
     */
    TxnErrorCode get(std::string_view key, std::string* val, bool snapshot = false) override;
    /**
     * Closed-open range
     * @param snapshot if true, key range will not be included in txn conflict detection this time
     * @param limit if non-zero, indicates the maximum number of key-value pairs to return
     * @return TXN_OK for success, otherwise for error
     */
    TxnErrorCode get(std::string_view begin, std::string_view end,
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
     *@return TXN_OK for success otherwise error
     */
    TxnErrorCode commit() override;

    TxnErrorCode get_read_version(int64_t* version) override;
    TxnErrorCode get_committed_version(int64_t* version) override;

    TxnErrorCode abort() override;

    TxnErrorCode batch_get(std::vector<std::optional<std::string>>* res, const std::vector<std::string>& keys,
                  const BatchGetOptions& opts = BatchGetOptions()) override;

private:
    TxnErrorCode inner_get(const std::string& key, std::string* val, bool snapshot);

    TxnErrorCode inner_get(const std::string& begin, const std::string& end,
                           std::unique_ptr<selectdb::RangeGetIterator>* iter, bool snapshot,
                           int limit);

    std::shared_ptr<MemTxnKv> kv_ {nullptr};
    bool commited_ = false;
    bool aborted_ = false;
    std::mutex lock_;
    std::set<std::string> unreadable_keys_;
    std::set<std::string> read_set_;
    std::map<std::string, std::string> writes_;
    std::list<std::pair<std::string, std::string>> remove_ranges_;
    std::vector<std::tuple<ModifyOpType, std::string, std::string>> op_list_;

    int64_t committed_version_ = -1;
    int64_t read_version_ = -1;
};

class RangeGetIterator : public selectdb::RangeGetIterator {
public:
    RangeGetIterator(std::vector<std::pair<std::string, std::string>> kvs, bool more)
            : kvs_(std::move(kvs)), kvs_size_(kvs_.size()), idx_(0), more_(more) {}

    ~RangeGetIterator() override = default;

    bool has_next() override { return idx_ < kvs_size_; }

    std::pair<std::string_view, std::string_view> next() override {
        if (idx_ < 0 || idx_ >= kvs_size_) return {};
        auto& kv = kvs_[idx_++];
        return {kv.first, kv.second};
    }

    void seek(size_t pos) override { idx_ = pos; }

    bool more() override { return more_; }

    int size() override { return kvs_size_; }
    void reset() override { idx_ = 0; }

    std::string next_begin_key() override {
        std::string k;
        if (!more()) return k;
        auto& key = kvs_[kvs_size_ - 1].first;
        k.reserve(key.size() + 1);
        k.append(key);
        k.push_back('\x00');
        return k;
    }

private:
    std::vector<std::pair<std::string, std::string>> kvs_;
    int kvs_size_;
    int idx_;
    bool more_;
};

} // namespace memkv
} // namespace selectdb