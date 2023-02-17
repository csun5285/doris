#pragma once

#include <mutex>
#include <set>

#include "recycler/s3_accessor.h"

namespace selectdb {

class MockAccessor : public ObjStoreAccessor {
public:
    explicit MockAccessor(const S3Conf& conf) {
        path_ = conf.endpoint + '/' + conf.bucket + '/' + conf.prefix;
    }
    ~MockAccessor() override = default;

    const std::string& path() const override { return path_; }

    // returns 0 for success otherwise error
    int init() override { return 0; }

    // returns 0 for success otherwise error
    int delete_objects_by_prefix(const std::string& relative_path) override {
        std::lock_guard lock(mtx_);
        if (relative_path.empty()) {
            objects_.clear();
            return 0;
        }
        auto begin = objects_.lower_bound(relative_path);
        if (begin == objects_.end()) {
            return 0;
        }
        auto path1 = relative_path;
        path1.back() += 1;
        auto end = objects_.lower_bound(path1);
        objects_.erase(begin, end);
        return 0;
    }

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths) override {
        std::lock_guard lock(mtx_);
        for (auto& path : relative_paths) {
            objects_.erase(path);
        }
        return 0;
    }

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path) override {
        std::lock_guard lock(mtx_);
        objects_.erase(relative_path);
        return 0;
    }

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override {
        std::lock_guard lock(mtx_);
        objects_.insert(relative_path);
        return 0;
    }

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<std::string>* keys) override {
        std::lock_guard lock(mtx_);
        if (relative_path == "") {
            for (const auto& obj : objects_) {
                keys->push_back(obj);
            }
            return 0;
        }
        auto begin = objects_.lower_bound(relative_path);
        if (begin == objects_.end()) {
            return 0;
        }
        auto path1 = relative_path;
        path1.back() += 1;
        auto end = objects_.lower_bound(path1);
        for (auto it = begin; it != end; ++it) {
            keys->push_back(*it);
        }
        return 0;
    }

    // returns 0 for success otherwise error
    int exists(const std::string& relative_path, const std::string& etag, bool* exist) override {
        std::lock_guard lock(mtx_);
        *exist = objects_.find(relative_path) != objects_.end();
        return 0;
    }

    // returns 0 for success otherwise error
    int get_etag(const std::string& relative_path, std::string* etag) override {
        *etag = relative_path;
        return 0;
    }

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    int delete_expired_objects(const std::string& relative_path, int64_t expired_time) override {
        return 0;
    }
private:
    std::string path_;

    std::mutex mtx_;
    std::set<std::string> objects_; // store objects' relative path
};

} // namespace selectdb
