#include "recycler/s3_accessor.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ServiceClientModel.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/GetBucketLifecycleConfigurationRequest.h>
#include <aws/s3/model/GetBucketVersioningRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/LifecycleRule.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <algorithm>
#include <chrono>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <thread>

#include "common/configbase.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "gtest/gtest.h"
#include "mock_accessor.h"

std::unique_ptr<selectdb::MockAccessor> _mock_fs;

class MockS3Client {
    auto ListObjectsV2(const Aws::S3::Model::ListObjectsV2Request& req) -> decltype(auto) {
        auto prefix = req.GetPrefix();
        auto continuation_token =
                req.ContinuationTokenHasBeenSet() ? req.GetContinuationToken() : "";
        bool truncated = true;
        std::vector<selectdb::ObjectMeta> files;
        size_t num = 0;
        do {
            _mock_fs->list(prefix, &files);
            if (num == files.size()) {
                truncated = false;
                break;
            }
            num = files.size();
            auto path1 = files.back().path;
            prefix = path1.back() += 1;
        } while (files.size() <= 1000);
        Aws::S3::Model::ListObjectsV2Result result;
        result.SetIsTruncated(truncated);
        std::vector<Aws::S3::Model::Object> objects;
        std::for_each(files.begin(), files.end(), [&](const selectdb::ObjectMeta& file) {
            Aws::S3::Model::Object obj;
            obj.SetKey(file.path);
            Aws::Utils::DateTime date;
            obj.SetLastModified(date);
            objects.emplace_back(std::move(obj));
        });
        result.SetContents(std::move(objects));
        return Aws::S3::Model::ListObjectsV2Outcome(std::move(result));
    }

    auto DeleteObjects(const Aws::S3::Model::DeleteObjectsRequest& req) -> decltype(auto) {
        Aws::S3::Model::DeleteObjectsResult result;
        const auto& deletes = req.GetDelete();
        for (const auto& obj : deletes.GetObjects()) {
            _mock_fs->delete_object(obj.GetKey());
        }
        return Aws::S3::Model::DeleteObjectsOutcome(std::move(result));
    }

    auto PutObject(const Aws::S3::Model::PutObjectRequest& req) -> decltype(auto) {
        Aws::S3::Model::PutObjectResult result;
        const auto& key = req.GetKey();
        _mock_fs->put_object(key, "");
        return Aws::S3::Model::PutObjectOutcome(std::move(result));
    }

    auto HeadObject(const Aws::S3::Model::HeadObjectRequest& req) -> decltype(auto) {
        Aws::S3::Model::HeadObjectResult result;
        const auto& key = req.GetKey();
        auto v = _mock_fs->exist(key);
        if (v == 1) {
            auto err = Aws::Client::AWSError<Aws::S3::S3Errors>(
                    Aws::S3::S3Errors::RESOURCE_NOT_FOUND, false);
            err.SetResponseCode(Aws::Http::HttpResponseCode::NOT_FOUND);

            return Aws::S3::Model::HeadObjectOutcome(std::move(err));
        }
        return Aws::S3::Model::HeadObjectOutcome(std::move(result));
    }
};

std::unique_ptr<MockS3Client> _mock_client;

struct MockCallable {
    std::string point_name;
    std::function<void(void*)> func;
};

static auto callbacks = std::array {
        MockCallable {"s3_client::list_objects_v2",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::ListObjectsV2Outcome*,
                                                  Aws::S3::Model::ListObjectsV2Request*>*)p;
                          *pair.first = (*_mock_client).ListObjectsV2(*pair.second);
                      }},
        MockCallable {"s3_client::delete_objects",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::DeleteObjectsOutcome*,
                                                  Aws::S3::Model::DeleteObjectsRequest*>*)p;
                          *pair.first = (*_mock_client).DeleteObjects(*pair.second);
                      }},
        MockCallable {"s3_client::put_object",
                      [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::PutObjectOutcome*,
                                                  Aws::S3::Model::PutObjectRequest*>*)p;
                          *pair.first = (*_mock_client).PutObject(*pair.second);
                      }},
        MockCallable {"s3_client::head_object", [](void* p) {
                          auto pair = *(std::pair<Aws::S3::Model::HeadObjectOutcome*,
                                                  Aws::S3::Model::HeadObjectRequest*>*)p;
                          *pair.first = (*_mock_client).HeadObject(*pair.second);
                      }}};

int main(int argc, char** argv) {
    const std::string conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (!selectdb::init_glog("s3_accessor_test")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

namespace selectdb {

std::string get_key(const std::string& relative_path) {
    return fmt::format("/{}", relative_path);
}

void create_file_under_prefix(std::string_view prefix, size_t file_nums) {
    for (size_t i = 0; i < file_nums; i++) {
        _mock_fs->put_object(get_key(fmt::format("{}{}", prefix, i)), "");
    }
}

TEST(S3AccessorTest, list) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    create_file_under_prefix("test_list", 300);
    std::vector<ObjectMeta> files;
    ASSERT_EQ(0, accessor->list("test_list", &files));
    ASSERT_EQ(300, files.size());
}

TEST(S3AccessorTest, put) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_put";
    for (size_t i = 0; i < 300; i++) {
        ASSERT_EQ(0, accessor->put_object(fmt::format("{}{}", prefix, i), ""));
    }
    std::vector<ObjectMeta> files;
    ASSERT_EQ(0, accessor->list("test_put", &files));
    ASSERT_EQ(300, files.size());
}

TEST(S3AccessorTest, exist) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_exist";
    ASSERT_EQ(1, accessor->exist(prefix));
    ASSERT_EQ(0, accessor->put_object(prefix, ""));
    ASSERT_EQ(0, accessor->exist(prefix));
}

// function is not implemented
TEST(S3AccessorTest, DISABLED_delete_object) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_object";
    create_file_under_prefix(prefix, 200);
    for (size_t i = 0; i < 200; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(0, accessor->delete_object(path));
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, delete_objects) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_objects";
    std::vector<std::string> paths;
    size_t num = 300;
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        _mock_fs->put_object(path, "");
        paths.emplace_back(std::move(path));
    }
    ASSERT_EQ(0, accessor->delete_objects(paths));
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(1, accessor->exist(path));
    }
}

TEST(S3AccessorTest, delete_object_by_prefix) {
    _mock_fs = std::make_unique<selectdb::MockAccessor>(selectdb::S3Conf {});
    _mock_client = std::make_unique<MockS3Client>();
    auto accessor = std::make_unique<S3Accessor>(S3Conf {});
    auto sp = SyncPoint::get_instance();
    std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
        sp->set_call_back(fmt::format("{}::pred", mock_callback.point_name),
                          [](void* p) { *((bool*)p) = true; });
        sp->set_call_back(mock_callback.point_name, mock_callback.func);
    });
    sp->enable_processing();
    std::unique_ptr<int, std::function<void(int*)>> defer_log_statistics((int*)0x01, [&](int*) {
        sp->disable_processing();
        std::for_each(callbacks.begin(), callbacks.end(), [&](const MockCallable& mock_callback) {
            sp->clear_call_back(mock_callback.point_name);
        });
    });
    std::string prefix = "test_delete_objects_by_prefix";
    size_t num = 2000;
    create_file_under_prefix(prefix, num);
    ASSERT_EQ(0, accessor->delete_objects_by_prefix(prefix));
    for (size_t i = 0; i < num; i++) {
        auto path = fmt::format("{}{}", prefix, i);
        ASSERT_EQ(1, accessor->exist(path));
    }
}

} // namespace selectdb
