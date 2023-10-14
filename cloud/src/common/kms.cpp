
#include "common/kms.h"

#include <alibabacloud/core/AlibabaCloud.h>
#include <alibabacloud/core/CommonClient.h>
#include <json/json.h>

#include <memory>

#include "common/logging.h"
#include "common/sync_point.h"
namespace selectdb {

int create_kms_client(KmsConf&& conf, std::unique_ptr<KmsClient>* kms_client) {
    if (conf.ak.empty() || conf.sk.empty() || conf.endpoint.empty() || conf.region.empty() ||
        conf.provider.empty() || conf.cmk.empty()) {
        LOG(WARNING) << "incorrect kms conf";
        return -1;
    }
    // Todo: only support ali cloud now
    if (conf.provider != "ali") {
        LOG(WARNING) << "only support kms for ali cloud now";
        return -1;
    }
    *kms_client = std::make_unique<AliKmsClient>(std::move(conf));
    return 0;
}

AliKmsClient::AliKmsClient(KmsConf&& conf) : KmsClient(std::move(conf)) {
    AlibabaCloud::InitializeSdk();
}

AliKmsClient::~AliKmsClient() {
    AlibabaCloud::ShutdownSdk();
}

int AliKmsClient::init() {
    AlibabaCloud::ClientConfiguration configuration(conf_.region);
    AlibabaCloud::Credentials credential(conf_.ak, conf_.sk);
    kms_client_ = std::make_unique<AlibabaCloud::CommonClient>(std::move(credential),
                                                               std::move(configuration));
    return 0;
}

int AliKmsClient::encrypt(const std::string& plaintext, std::string* output) {
    {
        [[maybe_unused]] int ret = -1;
        [[maybe_unused]] auto p = &plaintext;
        TEST_SYNC_POINT_CALLBACK("alikms::encrypt::plaintext",
                                 reinterpret_cast<void*>(const_cast<std::string*>(p)));
        TEST_SYNC_POINT_CALLBACK("alikms::encrypt::output", output);
        TEST_SYNC_POINT_RETURN_WITH_VALUE("alikms::encrypt::ret", &ret);
    }
    AlibabaCloud::CommonRequest request(AlibabaCloud::CommonRequest::RequestPattern::RpcPattern);
    request.setHttpMethod(AlibabaCloud::HttpRequest::Method::Post);
    request.setDomain(conf_.endpoint);
    request.setVersion("2016-01-20");
    request.setQueryParameter("Action", "Encrypt");
    request.setQueryParameter("KeyId", conf_.cmk);
    request.setQueryParameter("Plaintext", plaintext);

    auto response = kms_client_->commonResponse(request);
    if (response.isSuccess()) {
        Json::Value json;
        Json::Reader reader;
        if (reader.parse(response.result().payload(), json) && json.isMember("CiphertextBlob")) {
            *output = json["CiphertextBlob"].asString();
            return 0;
        } else {
            LOG(WARNING) << "failed to parse response, response=" << response.result().payload();
            return -1;
        }
    } else {
        LOG(WARNING) << "failed to encrypt data, error=" << response.error().errorMessage()
                     << " request id=" << response.error().requestId();
        return -1;
    }
    return 0;
}

int AliKmsClient::decrypt(const std::string& ciphertext, std::string* output) {
    {
        [[maybe_unused]] int ret = -1;
        TEST_SYNC_POINT_CALLBACK("alikms::decrypt::output", output);
        TEST_SYNC_POINT_RETURN_WITH_VALUE("alikms::decrypt::ret", &ret);
    }
    AlibabaCloud::CommonRequest request(AlibabaCloud::CommonRequest::RequestPattern::RpcPattern);
    request.setHttpMethod(AlibabaCloud::HttpRequest::Method::Post);
    request.setDomain(conf_.endpoint);
    request.setVersion("2016-01-20");
    request.setQueryParameter("Action", "Decrypt");
    request.setQueryParameter("CiphertextBlob", ciphertext);

    auto response = kms_client_->commonResponse(request);
    if (response.isSuccess()) {
        Json::Value json;
        Json::Reader reader;
        if (reader.parse(response.result().payload(), json) && json.isMember("Plaintext")) {
            *output = json["Plaintext"].asString();
            return 0;
        } else {
            LOG(WARNING) << "failed to parse response, response=" << response.result().payload();
            return -1;
        }
    } else {
        LOG(WARNING) << "failed to decrypt data, error=" << response.error().errorMessage()
                     << " request id=" << response.error().requestId();
        return -1;
    }
    return 0;
}

int AliKmsClient::generate_data_key(std::string* ciphertext, std::string* plaintext) {
    {
        [[maybe_unused]] int ret = -1;
        TEST_SYNC_POINT_CALLBACK("alikms::generate_data_key::ciphertext", ciphertext);
        TEST_SYNC_POINT_CALLBACK("alikms::generate_data_key::plaintext", plaintext);
        TEST_SYNC_POINT_RETURN_WITH_VALUE("alikms::generate_data_key::ret", &ret);
    }
    AlibabaCloud::CommonRequest request(AlibabaCloud::CommonRequest::RequestPattern::RpcPattern);
    request.setHttpMethod(AlibabaCloud::HttpRequest::Method::Post);
    request.setDomain(conf_.endpoint);
    request.setVersion("2016-01-20");
    request.setQueryParameter("Action", "GenerateDataKey");
    request.setQueryParameter("KeySpec", "AES_256");
    request.setQueryParameter("KeyId", "839ff82c-dcb5-4438-a6a4-6ec832443ba8");

    auto response = kms_client_->commonResponse(request);
    if (response.isSuccess()) {
        Json::Value json;
        Json::Reader reader;
        if (reader.parse(response.result().payload(), json) && json.isMember("Plaintext") &&
            json.isMember("CiphertextBlob")) {
            *plaintext = json["Plaintext"].asString();
            *ciphertext = json["CiphertextBlob"].asString();
            return 0;
        } else {
            LOG(WARNING) << "failed to parse response, response=" << response.result().payload();
            return -1;
        }
    } else {
        LOG(WARNING) << "failed to generate data key, error=" << response.error().errorMessage()
                     << " request id=" << response.error().requestId();
        return -1;
    }
    return 0;
}

} // namespace selectdb