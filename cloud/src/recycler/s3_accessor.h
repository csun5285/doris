#pragma once

#include <memory>
#include <string>
#include <vector>

namespace Aws::S3 {
class S3Client;
} // namespace Aws::S3

namespace selectdb {

enum class S3RateLimitType;
extern int reset_s3_rate_limiter(S3RateLimitType type, size_t max_speed, size_t max_burst,
                                 size_t limit);

struct ObjectMeta {
    std::string path; // Relative path
    int64_t size {0};
};

class ObjStoreAccessor {
public:
    ObjStoreAccessor() = default;
    virtual ~ObjStoreAccessor() = default;

    virtual const std::string& path() const = 0;

    // returns 0 for success otherwise error
    virtual int init() = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects_by_prefix(const std::string& relative_path,
                                         const std::string& instance_id) = 0;

    // returns 0 for success otherwise error
    virtual int delete_objects(const std::vector<std::string>& relative_paths,
                               const std::string& instance_id) = 0;

    // returns 0 for success otherwise error
    virtual int delete_object(const std::string& relative_path, const std::string& instance_id) = 0;

    // for test
    // returns 0 for success otherwise error
    virtual int put_object(const std::string& relative_path, const std::string& content) = 0;

    // returns 0 for success otherwise error
    virtual int list(const std::string& relative_path, std::vector<ObjectMeta>* files) = 0;

    // return 0 if object exists, 1 if object is not found, negative for error
    virtual int exist(const std::string& relative_path) = 0;

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    virtual int delete_expired_objects(const std::string& relative_path, int64_t expired_time,
                                       const std::string& instance_id) = 0;

    // return 0 for success otherwise error
    virtual int get_bucket_lifecycle(int64_t* expiration_days) = 0;

    // returns 0 for enabling bucket versioning, otherwise error
    virtual int check_bucket_versioning() = 0;
};

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
};

class S3Accessor : public ObjStoreAccessor {
public:
    explicit S3Accessor(S3Conf conf);
    ~S3Accessor() override;

    const std::string& path() const override { return path_; }

    const std::shared_ptr<Aws::S3::S3Client>& s3_client() const { return s3_client_; }

    const S3Conf& conf() const { return conf_; }

    // returns 0 for success otherwise error
    int init() override;

    // returns 0 for success, returns 1 for http FORBIDDEN error, negative for other errors
    int delete_objects_by_prefix(const std::string& relative_path,
                                 const std::string& instance_id) override;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths,
                       const std::string& instance_id) override;

    // returns 0 for success otherwise error
    int delete_object(const std::string& relative_path, const std::string& instance_id) override;

    // for test
    // returns 0 for success otherwise error
    int put_object(const std::string& relative_path, const std::string& content) override;

    // returns 0 for success otherwise error
    int list(const std::string& relative_path, std::vector<ObjectMeta>* ObjectMeta) override;

    // return 0 if object exists, 1 if object is not found, otherwise error
    int exist(const std::string& relative_path) override;

    // delete objects which last modified time is less than the input expired time and under the input relative path
    // returns 0 for success otherwise error
    int delete_expired_objects(const std::string& relative_path, int64_t expired_time,
                               const std::string& instance_id) override;

    // returns 0 for success otherwise error
    int get_bucket_lifecycle(int64_t* expiration_days) override;

    // returns 0 for enabling bucket versioning, otherwise error
    int check_bucket_versioning() override;

private:
    std::string get_key(const std::string& relative_path) const;
    // return empty string if the input key does not start with the prefix of S3 conf
    std::string get_relative_path(const std::string& key) const;

private:
    std::shared_ptr<Aws::S3::S3Client> s3_client_;
    S3Conf conf_;
    std::string path_;
};

class GcsAccessor final : public S3Accessor {
public:
    explicit GcsAccessor(S3Conf conf) : S3Accessor(std::move(conf)) {}
    ~GcsAccessor() override = default;

    // returns 0 for success otherwise error
    int delete_objects(const std::vector<std::string>& relative_paths,
                       const std::string& instance_id) override;
};

} // namespace selectdb
