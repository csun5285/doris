#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/ListObjectVersionsRequest.h>
#include <cos/cos_config.h>
#include <curl/curl.h>
#include <fmt/core.h>
#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <map>
#include <sstream>
#include <string>

#include "cos/cos_api.h"
#include "cos/cos_defines.h"
#include "cos/cos_sys_config.h"
#include "cos/request/bucket_req.h"
#include "cos/response/bucket_resp.h"
#include "cos/util/string_util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "rapidjson/document.h"
#include "rapidjson/error/en.h"

DEFINE_string(mode, "warehouse",
              "recovery mode: 'warehouse' or 's3', \
                            warehouse for using warehouse id, s3 for using s3's account");
// mode: warehouse
DEFINE_string(warehouse_id, "", "warehouse id");
DEFINE_string(ms_address, "", "ms address, ip + port");
DEFINE_string(ms_token, "", "ms token");
DEFINE_string(recovery_file, "", "recovery file, e.g., data/123/456.dat");

// mode: s3
DEFINE_string(ak, "", "ak");
DEFINE_string(sk, "", "sk");
DEFINE_string(bucket, "", "bucket");
DEFINE_string(region, "", "region");
DEFINE_string(endpoint, "", "endpoint");
DEFINE_string(provider, "", "provider, specifically for COS");
DEFINE_string(object_key, "", "e.g., ABC/data/123/456.dat");

DEFINE_int32(list_versions_nums, 2,
             "list last versions num, include recoverable version and delete version, -1 means "
             "unlimited");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is a data recovery tool.\n";
    ss << "Usage:\n";
    ss << "for warehouse mode:\n";
    ss << "./recovery_tool --warehouse_id=warehouse_id --ms_address=ip:port "
          "--ms_token=ms token --recovery_file=recovery_file\n";
    ss << "for s3 mode:\n";
    ss << "./recovery_tool --mode=s3 --ak=ak --sk=sk --bucket=bucket --region=region "
          "--endpoint=endpoint "
          "--provider=provider --object_key=object_key\n";
    return ss.str();
}

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;
    std::string provider;

    std::string to_string() const {
        std::stringstream ss;
        ss << "ak: " << ak << "\nsk: " << sk << "\nendpoint: " << endpoint << "\nregion: " << region
           << "\nbucket: " << bucket << "\nprefix: " << prefix << "\nprovider: " << provider
           << "\n";
        return ss.str();
    };
};

size_t write_callback(char* ptr, size_t size, size_t nmemb, void* userdata) {
    ((std::string*)userdata)->append(ptr, size * nmemb);
    return size * nmemb;
}
int get_warehouse_info(const std::string& ms_address, const std::string& ms_token,
                       const std::string& warehouse_id, std::string* resp) {
    CURL* curl;
    curl = curl_easy_init();
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01, [&curl](int*) {
        if (curl != nullptr) {
            curl_easy_cleanup(curl);
            curl = nullptr;
        }
    });
    if (curl == nullptr) {
        std::cerr << "curl init failed" << std::endl;
        return -1;
    }
    CURLcode res;
    std::string response;
    std::unique_ptr<int, std::function<void(int*)>> defer_status(
            (int*)0x01, [&](int*) { curl_easy_cleanup(curl); });
    std::string url = fmt::format("{}/MetaService/http/get_instance?token={}&instance_id={}",
                                  ms_address, ms_token, warehouse_id);
    curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_callback);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);
    res = curl_easy_perform(curl);
    if (res != CURLE_OK) {
        std::cerr << "curl failed: " << curl_easy_strerror(res) << std::endl;
        return -1;
    }
    *resp = std::move(response);
    return 0;
}
int get_obj_info(const std::string& resp, S3Conf* s3_conf) {
    rapidjson::Document doc;
    doc.Parse(resp.c_str());
    if (doc.HasParseError()) {
        std::cerr << "parse warehouse info failed, please check params, err: "
                  << GetParseError_En(doc.GetParseError()) << std::endl;
        return -1;
    }

    if (!doc.HasMember("code")) {
        std::cerr << "cannot find the code field in resp" << std::endl;
        return -1;
    }
    auto node = doc.FindMember("code");
    if (node->value.IsString() && std::strcmp(node->value.GetString(), "OK") != 0) {
        if (!doc.HasMember("msg")) {
            std::cerr << "cannot find the msg field in resp" << std::endl;
            return -1;
        }
        node = doc.FindMember("msg");
        std::cerr << "resp code != OK,  msg: " << node->value.GetString() << std::endl;
        return -1;
    }

    if (!doc.HasMember("result") || !doc.FindMember("result")->value.HasMember("obj_info")) {
        std::cerr << "cannot find the result/obj_info field in resp" << std::endl;
        return -1;
    }

    node = doc.FindMember("result")->value.FindMember("obj_info");
    std::map<int, S3Conf> s3_confs;
    for (auto i = 0; i < node->value.GetArray().Size(); ++i) {
        S3Conf conf;
        conf.ak = (node->value.GetArray())[i].FindMember("ak")->value.GetString();
        conf.sk = (node->value.GetArray())[i].FindMember("sk")->value.GetString();
        conf.bucket = (node->value.GetArray())[i].FindMember("bucket")->value.GetString();
        conf.prefix = (node->value.GetArray())[i].FindMember("prefix")->value.GetString();
        conf.endpoint = (node->value.GetArray())[i].FindMember("endpoint")->value.GetString();
        conf.region = (node->value.GetArray())[i].FindMember("region")->value.GetString();
        conf.provider = (node->value.GetArray())[i].FindMember("provider")->value.GetString();
        auto [_, succ] = s3_confs.insert({i, std::move(conf)});
        if (!succ) {
            std::cerr << "insert s3conf failed" << std::endl;
            return -1;
        }
    }
    std::cout << "------ Find s3 conf: ------" << std::endl;
    for (const auto& [i, c] : s3_confs) {
        std::cout << "ID: " << i << std::endl;
        std::cout << c.to_string() << std::endl;
    }
    std::cout << "----------------------------" << std::endl;
    if (s3_confs.size() == 1) {
        std::cout << "only one s3 conf, automatically select that one" << std::endl;
        auto c = s3_confs.begin();
        *s3_conf = std::move(c->second);
        return 0;
    }
    int num;
    std::cout << "Multiple s3 info, select the ID of s3 conf: ";
    std::cin >> num;
    if (!s3_confs.count(num)) {
        std::cerr << "wrong ID of s3 conf" << std::endl;
        return -1;
    }
    *s3_conf = std::move(s3_confs[num]);
    return 0;
}

int recovery_data(const S3Conf& s3_conf, const std::string& recovery_file, int list_versions_nums) {
    if (s3_conf.ak.empty() || s3_conf.sk.empty() || s3_conf.endpoint.empty()) {
        std::cerr << "wrong format of s3 conf" << std::endl;
        return -1;
    }
    Aws::SDKOptions aws_options;
    Aws::InitAPI(aws_options);
    const std::string bucket = s3_conf.bucket;
    std::string recovery_file_key = recovery_file;
    Aws::Auth::AWSCredentials aws_cred(s3_conf.ak, s3_conf.sk);
    Aws::Client::ClientConfiguration aws_config;
    aws_config.endpointOverride = s3_conf.endpoint;
    aws_config.region = s3_conf.region;
    aws_config.verifySSL = false;
    Aws::S3::S3Client s3_client(aws_cred, aws_config,
                                Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never, true);
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&aws_options](int*) { Aws::ShutdownAPI(aws_options); });

    struct obj_version_info {
        std::string version_id;
        std::string last_modify_time;
        std::string key;
    };
    std::map<int, obj_version_info> recoverable_version_infos;
    int idx = 0;
    if (s3_conf.provider == "COS") {
        qcloud_cos::CosConfig cos_config(0, s3_conf.ak, s3_conf.sk, s3_conf.region);
        qcloud_cos::CosAPI cos(cos_config);

        qcloud_cos::GetBucketObjectVersionsReq req(s3_conf.bucket);
        req.SetPrefix(recovery_file_key);
        if (list_versions_nums != -1) {
            req.SetMaxKeys(list_versions_nums);
        }
        bool is_truncated = false;
        do {
            qcloud_cos::GetBucketObjectVersionsResp resp;
            qcloud_cos::CosResult result = cos.GetBucketObjectVersions(req, &resp);
            if (result.IsSucc()) {
                std::vector<qcloud_cos::COSVersionSummary> cotents = resp.GetVersionSummary();
                for (std::vector<qcloud_cos::COSVersionSummary>::const_iterator itr =
                             cotents.begin();
                     itr != cotents.end(); ++itr) {
                    const qcloud_cos::COSVersionSummary& content = *itr;
                    if (!content.m_is_delete_marker) {
                        auto [_, succ] = recoverable_version_infos.insert(
                                {idx,
                                 {content.m_version_id, content.m_last_modified, content.m_key}});
                        if (!succ) {
                            std::cerr << "insert obj version failed" << std::endl;
                            return -1;
                        }
                        ++idx;
                    }
                }
            } else {
                std::cerr << "list object versions failed, HttpStatus=" << result.GetHttpStatus()
                          << "ErrorCode=" << result.GetErrorCode()
                          << "ErrorMsg=" << result.GetErrorMsg() << std::endl;
                return -1;
            }
            is_truncated = resp.IsTruncated();
            req.SetKeyMarker(resp.GetNextKeyMarker());
        } while (is_truncated);
    } else {
        Aws::S3::Model::ListObjectVersionsRequest list_request;
        list_request.WithBucket(bucket).WithPrefix(recovery_file_key);
        if (list_versions_nums != -1) {
            list_request.SetMaxKeys(list_versions_nums);
        }

        bool is_truncated = false;
        do {
            auto list_response = s3_client.ListObjectVersions(list_request);
            if (!list_response.IsSuccess()) {
                std::cerr << "list object versions failed, err: "
                          << list_response.GetError().GetMessage() << std::endl;
                return -1;
            } else {
                const auto& object_version_list = list_response.GetResult().GetVersions();
                for (const auto& object_version : object_version_list) {
                    auto [_, succ] = recoverable_version_infos.insert(
                            {idx,
                             {object_version.GetVersionId(),
                              object_version.GetLastModified().ToGmtString(
                                      Aws::Utils::DateFormat::ISO_8601),
                              object_version.GetKey()}});
                    if (!succ) {
                        std::cerr << "insert obj version failed" << std::endl;
                        return -1;
                    }
                    ++idx;
                }
            }
            is_truncated = list_response.GetResult().GetIsTruncated();
            list_request.SetKeyMarker(list_response.GetResult().GetNextKeyMarker());
            list_request.SetVersionIdMarker(list_response.GetResult().GetNextVersionIdMarker());
        } while (is_truncated);
    }
    std::cout << "------ Find recoverable version: ------" << std::endl;
    for (const auto& [i, v] : recoverable_version_infos) {
        std::cout << "ID: " << i << std::endl;
        std::cout << "Key: " << v.key << std::endl;
        std::cout << "version_id: " << v.version_id << "\nlast_modify_time: " << v.last_modify_time
                  << "\n"
                  << std::endl;
    }
    std::cout << "---------------------------------------" << std::endl;
    if (recoverable_version_infos.empty()) {
        std::cout << "empty version list, please check the params of list_versions_nums"
                  << std::endl;
        return -1;
    }

    std::string version_id;
    if (recoverable_version_infos.size() == 1) {
        std::cout << "only one recoverable version, automatically select that one" << std::endl;
        auto v = recoverable_version_infos.begin();
        version_id = v->second.version_id;
    } else {
        int num;
        std::cout << "Multiple recoverable version, select the ID of recoverable version: ";
        std::cin >> num;
        if (!recoverable_version_infos.count(num)) {
            std::cerr << "wrong ID of recoverable version" << std::endl;
            return -1;
        }
        version_id = recoverable_version_infos[num].version_id;
    }

    // You create a copy of object up to 5 GB in size in a single atomic action using this API
    Aws::S3::Model::CopyObjectRequest copy_request;
    copy_request.SetBucket(bucket);
    copy_request.SetKey(recovery_file_key);
    copy_request.SetCopySource(bucket + "/" + recovery_file_key + "?versionId=" + version_id);

    auto copy_object_outcome = s3_client.CopyObject(copy_request);
    if (copy_object_outcome.IsSuccess()) {
        std::cout << "Successfully recovery object" << std::endl;
    } else {
        std::cerr << "recovery faild, err: " << copy_object_outcome.GetError().GetMessage()
                  << std::endl;
        return -1;
    }
    return 0;
}

int main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_mode != "warehouse" && FLAGS_mode != "s3") {
        std::cerr << "invaild mode" << std::endl;
        return -1;
    }
    S3Conf s3_conf;
    std::string recovery_file_key;
    if (FLAGS_mode == "warehouse") {
        if (FLAGS_ms_address.empty() || FLAGS_ms_token.empty() || FLAGS_warehouse_id.empty() ||
            FLAGS_recovery_file.empty()) {
            std::cerr << "invaild params" << std::endl;
            return -1;
        }
        std::string resp;
        if (get_warehouse_info(FLAGS_ms_address, FLAGS_ms_token, FLAGS_warehouse_id, &resp) != 0) {
            std::cerr << "get warehouse info failed" << std::endl;
            return -1;
        }
        if (get_obj_info(resp, &s3_conf) != 0) {
            std::cerr << "get obj info failed" << std::endl;
            return -1;
        }
        recovery_file_key = s3_conf.prefix + "/" + FLAGS_recovery_file;
    } else {
        s3_conf.ak = FLAGS_ak;
        s3_conf.sk = FLAGS_sk;
        s3_conf.bucket = FLAGS_bucket;
        s3_conf.endpoint = FLAGS_endpoint;
        s3_conf.region = FLAGS_region;
        s3_conf.provider = FLAGS_provider;
        recovery_file_key = FLAGS_object_key;
        std::cout << "use s3 info :\n" << s3_conf.to_string() << std::endl;
    }
    recovery_data(s3_conf, recovery_file_key, FLAGS_list_versions_nums);
    return 0;
}