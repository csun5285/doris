#include "common/logging.h"
#include "mem_txn_kv.h"
#include "txn_kv.h"

#include <cstdint>
#include <memory>
#include <mutex>
#include <cstring>
#include <ostream>
#include <string>


namespace selectdb {

int MemTxnKv::init() {
    return 0;
}

int MemTxnKv::create_txn(std::unique_ptr<Transaction>* txn) {
    auto t = new memkv::Transaction(this->shared_from_this());
    txn->reset(t);
    return 0;
}

int MemTxnKv::update(const std::set<std::string>& read_set,
            const std::vector<OpTuple> &op_list, int64_t read_version, int64_t* committed_version) {
    std::lock_guard<std::mutex> l(lock_);

    // check_confict
    for (const auto& k: read_set) {
        auto iter = log_kv_.find(k);
        if (iter != log_kv_.end()) {
            auto log_item = iter->second;
            if (log_item.front().commit_version_ > read_version) {
                LOG(WARNING) << "commit confict";
                //keep the same behaviour with fdb.
                return 1002;
            }
        }
    }

    ++committed_version_;;
    int16_t seq = 0;
    for (const auto& vec: op_list) {
        auto [op_type, k, v] = vec;
        LogItem log_item{op_type, committed_version_, k, v};
        log_kv_[k].push_front(log_item);
        switch (op_type) {
            case memkv::ModifyOpType::PUT: {
                mem_kv_[k] = v;
                break;
            }
            case memkv::ModifyOpType::ATOMIC_SET_VER_KEY: {
                std::string ver_key(k);
                gen_version_timestamp(committed_version_, seq, &ver_key);
                mem_kv_[ver_key] = v;
                break;
            }
            case memkv::ModifyOpType::ATOMTC_SET_VER_VAL: {
                std::string ver_val(v);
                gen_version_timestamp(committed_version_, seq, &ver_val);
                mem_kv_[k] = ver_val;
                break;
            }
            case memkv::ModifyOpType::ATOMIC_ADD: {
                if (mem_kv_.count(k) == 0) {
                    mem_kv_[k] = v;
                } else {
                    std::string org_val = mem_kv_[k];
                    if (org_val.size() != 8) {
                        org_val.resize(8, '\0');
                    }
                    int64_t res = *reinterpret_cast<const int64_t*>(org_val.data()) + std::stoll(v);
                    std::string res_str = std::string(sizeof(res), '\0');
                    std::memcpy(res_str.data(), &res, res_str.size());
                    mem_kv_[k] = res_str;
                }
                break;
            }
            case memkv::ModifyOpType::REMOVE: {
                mem_kv_.erase(k);
                break;
            }
            case memkv::ModifyOpType::REMOVE_RANGE: {
                auto begin_iter = mem_kv_.lower_bound(k);
                auto end_iter = mem_kv_.lower_bound(v);
                mem_kv_.erase(begin_iter, end_iter);
                break;
            }
            default:
                break;
        }

    }

    *committed_version = committed_version_;
    return 0;
}

int MemTxnKv::get_kv(std::map<std::string, std::string> *kv, int64_t* version) {
    std::lock_guard<std::mutex> l(lock_);
    *kv = mem_kv_;
    *version = committed_version_;
    return 0;
}

int MemTxnKv::gen_version_timestamp(int64_t ver, int16_t seq, std::string* str) {
    // Convert litter endian to big endian
    static auto to_big_int64 = [](int64_t v) {
        v = ((v & 0xffffffff00000000) >> 32) | ((v & 0x00000000ffffffff) << 32);
        v = ((v & 0xffff0000ffff0000) >> 16) | ((v & 0x0000ffff0000ffff) << 16);
        v = ((v & 0xff00ff00ff00ff00) >> 8) | ((v & 0x00ff00ff00ff00ff) << 8);
        return v;
    };

    static auto to_big_int16 = [](int16_t v) {
        v = ((v & 0xff00) >> 8) | ((v & 0x00ff) << 8);
        return v;
    };

    ver = to_big_int64(ver);
    seq = to_big_int16(seq);

    int size = str->size();
    str->resize(size + 10, '\0');
    std::memcpy(str->data() + size, &ver, sizeof(ver));
    std::memcpy(str->data() + size + 8, &seq, sizeof(seq));
    return 0;
}

int64_t MemTxnKv::get_last_commited_version() {
    std::lock_guard<std::mutex> l(lock_);
    return committed_version_;
}

int64_t MemTxnKv::get_last_read_version() {
    std::lock_guard<std::mutex> l(lock_);
    read_version_ = committed_version_;
    return read_version_;
}

} // namespace selectdb

namespace selectdb::memkv {

// =============================================================================
// Impl of Trasaction
// =============================================================================

int Transaction::init() {
    return 0;
}

int Transaction::begin() {
    return 0;
}

void Transaction::put(std::string_view key, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(val.data(), val.size());
    inner_kv_[k] = v;
    op_list_.emplace_back(ModifyOpType::PUT, k, v);
}

int Transaction::get(std::string_view key, std::string* val, bool snapshot) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());

    // the key set by atomic_xxx can't not be read before the txn is committed.
    // if it is read, the txn will not be able to commit. 
    if (unreadable_keys_.count(k) != 0) {
        aborted_ = true;
        LOG(WARNING) << "read unreadable key, abort";
        return -2;
    }
    return inner_get(k, val, snapshot);
}

int Transaction::get(std::string_view begin, std::string_view end,
                     std::unique_ptr<selectdb::RangeGetIterator>* iter, bool snapshot, int limit) { 
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    // TODO: figure out what happen if range_get has part of unreadable_keys
    if (unreadable_keys_.count(begin_k) != 0) {
        aborted_ = true;
        LOG(WARNING) << "read unreadable key, abort";
        return -2;
    }
    return inner_get(begin_k, end_k, iter, snapshot, limit);
}


int Transaction::inner_get(const std::string& key, std::string* val, bool snapshot) {
    auto iter = inner_kv_.find(key);
    if (!snapshot) read_set_.emplace(key);
    if (iter == inner_kv_.end()) { return 1;}
    *val = iter->second;
    return 0;
}

int Transaction::inner_get(const std::string& begin, const std::string& end, 
                        std::unique_ptr<selectdb::RangeGetIterator>* iter, bool snapshot, int limit) {
    std::vector<std::pair<std::string, std::string>> kv_list;
    if (begin >= end) {
        std::unique_ptr<RangeGetIterator> ret(new memkv::RangeGetIterator(kv_list, false));
        *(iter) = std::move(ret);
        return 0;
    }

    bool use_limit = true;

    if (limit < 0) { return -1;}
    if (limit == 0) { use_limit = false;}

    bool more = false;
    auto begin_iter = inner_kv_.lower_bound(begin);
    auto end_iter = inner_kv_.lower_bound(end);
    for (; begin_iter != inner_kv_.end() && begin_iter != end_iter; begin_iter++) {
        kv_list.push_back({begin_iter->first, begin_iter->second});
        if (!snapshot) read_set_.emplace(begin_iter->first);
        if (use_limit) {
            limit--;
            if (limit == 0) { break;}
        }
    }
    if (use_limit && limit == 0 && ++begin_iter != end_iter) {
        more = true;
    }
    std::unique_ptr<RangeGetIterator> ret(new memkv::RangeGetIterator(kv_list, more));
    *(iter) = std::move(ret);
    return 0;
}

void Transaction::atomic_set_ver_key(std::string_view key_prefix, std::string_view val) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key_prefix.data(), key_prefix.size());
    std::string v(val.data(), val.size());
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMIC_SET_VER_KEY, k, v);
}

void Transaction::atomic_set_ver_value(std::string_view key, std::string_view value) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    std::string v(value.data(), value.size());
    unreadable_keys_.insert(k);
    op_list_.emplace_back(ModifyOpType::ATOMTC_SET_VER_VAL, k, v);
}

void Transaction::atomic_add(std::string_view key, int64_t to_add) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    op_list_.emplace_back(ModifyOpType::ATOMIC_ADD, k, std::to_string(to_add));
}

void Transaction::remove(std::string_view key) {
    std::lock_guard<std::mutex> l(lock_);
    std::string k(key.data(), key.size());
    inner_kv_.erase(k);
    op_list_.emplace_back(ModifyOpType::REMOVE, k, "");
}

void Transaction::remove(std::string_view begin, std::string_view end) {
    std::lock_guard<std::mutex> l(lock_);
    std::string begin_k(begin.data(), begin.size());
    std::string end_k(end.data(), end.size());
    if (begin_k >= end_k) {
        aborted_ = true;
    } else {
        auto begin_iter = inner_kv_.lower_bound(begin_k);
        auto end_iter = inner_kv_.lower_bound(end_k);
        inner_kv_.erase(begin_iter, end_iter);
        op_list_.emplace_back(ModifyOpType::REMOVE_RANGE, begin_k, end_k);
    }
}

int Transaction::commit() {
    std::lock_guard<std::mutex> l(lock_);
    if (aborted_) {
        return -2;
    }
    int ret = kv_->update(read_set_, op_list_, read_version_, &committed_version_);
    if (ret != 0) {
        return ret == 1002? -1 : -2;
    }
    commited_ = true;
    op_list_.clear();
    inner_kv_.clear();
    read_set_.clear();
    return 0;
}

int64_t Transaction::get_read_version() {
    std::lock_guard<std::mutex> l(lock_);
    return read_version_;
}

int64_t Transaction::get_committed_version() {
    std::lock_guard<std::mutex> l(lock_);
    return committed_version_;
}

int Transaction::abort() {
    return 0;
}

} // namespace selectdb::memkv
