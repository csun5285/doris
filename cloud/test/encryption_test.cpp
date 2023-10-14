#include <gen_cpp/selectdb_cloud.pb.h>
#include <gtest/gtest.h>

#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/mem_txn_kv.h"

int main(int argc, char** argv) {
    auto conf_file = "selectdb_cloud.conf";
    if (!selectdb::config::init(conf_file, true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }
    if (!selectdb::init_glog("encrypt")) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(EncryptionTest, EncryptTest) {
    std::string mock_ak = "AKIDOsfsagadsgdfaadgdsgdf";
    std::string mock_sk = "Hx60p12123af234541nsVsffdfsdfghsdfhsdf34t";
    selectdb::config::encryption_method = "AES_256_ECB";
    // "selectdbselectdbselectdbselectdb" -> "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI="
    selectdb::config::encryption_key = "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI=";
    {
        std::string decoded_text(selectdb::config::encryption_key.length(), '0');
        int decoded_text_len = selectdb::base64_decode(selectdb::config::encryption_key.c_str(),
                                                       selectdb::config::encryption_key.length(),
                                                       decoded_text.data());
        ASSERT_TRUE(decoded_text_len > 0);
        decoded_text.assign(decoded_text.data(), decoded_text_len);
        std::cout << "decoded_string: " << decoded_text << std::endl;
        ASSERT_EQ(decoded_text, "selectdbselectdbselectdbselectdb");
        int ret;
        selectdb::AkSkPair cipher_ak_sk_pair;
        ret = selectdb::encrypt_ak_sk({mock_ak, mock_sk}, selectdb::config::encryption_method,
                                      decoded_text, &cipher_ak_sk_pair);
        ASSERT_EQ(ret, 0);
        std::cout << "cipher ak: " << cipher_ak_sk_pair.first << std::endl;
        std::cout << "cipher sk: " << cipher_ak_sk_pair.second << std::endl;
        selectdb::AkSkPair plain_ak_sk_pair;
        ret = selectdb::decrypt_ak_sk(cipher_ak_sk_pair, selectdb::config::encryption_method,
                                      decoded_text, &plain_ak_sk_pair);
        ASSERT_EQ(ret, 0);
        std::cout << "plain ak: " << plain_ak_sk_pair.first << std::endl;
        std::cout << "plain sk: " << plain_ak_sk_pair.second << std::endl;
        ASSERT_EQ(mock_ak, plain_ak_sk_pair.first);
        ASSERT_EQ(mock_sk, plain_ak_sk_pair.second);
    }
}

TEST(EncryptionTest, RootKeyTestWithoutKms) {
    using namespace selectdb;

    config::enable_kms = false;
    // generate new root key
    global_encryption_key_info_map.clear();
    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);
    auto ret = init_global_encryption_key_info_map(mem_kv.get());
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(global_encryption_key_info_map.size(), 1);
    std::string key = system_meta_service_encryption_key_info_key();
    std::string val;
    std::unique_ptr<Transaction> txn;
    ret = mem_kv->create_txn(&txn);
    ret = txn->get(key, &val);
    ASSERT_EQ(ret, 0);
    EncryptionKeyInfoPB key_info;
    key_info.ParseFromString(val);
    ASSERT_EQ(key_info.items_size(), 1);
    std::cout << proto_to_json(key_info) << std::endl;
    const auto& item = key_info.items().at(0);
    ASSERT_EQ(item.key_id(), 1);
    std::string decoded_string(item.key().length(), '0');
    int decoded_text_len =
            base64_decode(item.key().c_str(), item.key().length(), decoded_string.data());
    decoded_string.assign(decoded_string.data(), decoded_text_len);
    ASSERT_EQ(global_encryption_key_info_map.at(item.key_id()), decoded_string);

    // get existed root key
    global_encryption_key_info_map.clear();
    ret = init_global_encryption_key_info_map(mem_kv.get());
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(global_encryption_key_info_map.size(), 1);
    ASSERT_EQ(global_encryption_key_info_map.at(item.key_id()), decoded_string);
}

TEST(EncryptionTest, DecryptKmsInfo) {
    std::string plaintext = "AKIDsZHqgyhdDSRBpfaNtHPHN0Wnpci";
    std::string encryption_key = "selectdb";
    // use `echo -n "$plaintext" | openssl enc -aes-256-ecb -e -a -pbkdf2 -pass pass:"$encryption_key" -p -nosalt`
    // to create hex_encryption_key and ciphertext
    // then use `echo -n "$hex_encryption_key"  | xxd -r -p | base64` to create encoded_encryption_key
    std::string ciphertext = "DRdlYbJmyEPJ9q1KggTCjBErv/9GzyjTFKXBgGR7X4I=";
    std::string encoded_encryption_key = "uwPXjGTuFJXyDZJBuYG52kdMxrWB24952HkXSa2v3Vw=";
    std::string decoded_key(encoded_encryption_key.length(), '0');
    int decoded_key_len = selectdb::base64_decode(
            encoded_encryption_key.c_str(), encoded_encryption_key.length(), decoded_key.data());
    ASSERT_TRUE(decoded_key_len > 0);
    decoded_key.assign(decoded_key.data(), decoded_key_len);
    std::cout << "decoded_string: " << selectdb::hex(decoded_key) << std::endl;

    selectdb::AkSkPair out;
    auto ret = selectdb::decrypt_ak_sk({"", ciphertext}, "AES_256_ECB", decoded_key, &out);
    ASSERT_EQ(ret, 0);
    ASSERT_EQ(out.second, plaintext);
}

TEST(EncryptionTest, RootKeyTestWithKms1) {
    using namespace selectdb;
    config::enable_kms = true;

    // incorrect kms conf

    // case1: empty param
    config::kms_ak = "";
    config::kms_sk = "DRdlYbJmyEPJ9q1KggTCjBErv/9GzyjTFKXBgGR7X4I=";
    config::kms_cmk = "2";
    config::kms_endpoint = "3";
    config::kms_region = "4";
    config::kms_provider = "ali";
    config::kms_info_encryption_key = "uwPXjGTuFJXyDZJBuYG52kdMxrWB24952HkXSa2v3Vw=";

    global_encryption_key_info_map.clear();
    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);
    auto ret = init_global_encryption_key_info_map(mem_kv.get());
    ASSERT_EQ(ret, -1);
    ASSERT_TRUE(global_encryption_key_info_map.empty());

    // case2: only support ali cloud
    config::kms_ak = "DRdlYbJmyEPJ9q1KggTCjBErv/9GzyjTFKXBgGR7X4I=";
    config::kms_provider = "tx"; // only support ali cloud
    ret = init_global_encryption_key_info_map(mem_kv.get());
    ASSERT_EQ(ret, -1);
    ASSERT_TRUE(global_encryption_key_info_map.empty());
}

TEST(EncryptionTest, RootKeyTestWithKms2) {
    using namespace selectdb;
    config::enable_kms = true;
    config::kms_ak = "DRdlYbJmyEPJ9q1KggTCjBErv/9GzyjTFKXBgGR7X4I=";
    config::kms_sk = "DRdlYbJmyEPJ9q1KggTCjBErv/9GzyjTFKXBgGR7X4I=";
    config::kms_cmk = "2";
    config::kms_endpoint = "3";
    config::kms_region = "4";
    config::kms_provider = "ali";
    config::kms_info_encryption_key = "uwPXjGTuFJXyDZJBuYG52kdMxrWB24952HkXSa2v3Vw=";

    auto mem_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_kv->init(), 0);
    // clear for generating new root key
    global_encryption_key_info_map.clear();

    // Generate data key failed
    {
        // mock falied to generate key
        auto sp = SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
        sp->set_call_back("alikms::generate_data_key::ret::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });
        sp->set_call_back("alikms::generate_data_key::ret", [](void* p) { *((int*)p) = -1; });
        sp->enable_processing();

        auto ret = init_global_encryption_key_info_map(mem_kv.get());
        ASSERT_EQ(ret, -1);
        ASSERT_TRUE(global_encryption_key_info_map.empty());
    }

    // "selectdbselectdbselectdbselectdb" -> "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI="
    std::string plaintext = "selectdbselectdbselectdbselectdb";
    std::string mock_encoded_plaintext = "c2VsZWN0ZGJzZWxlY3RkYnNlbGVjdGRic2VsZWN0ZGI=";
    std::string mock_encoded_ciphertext = mock_encoded_plaintext;
    // Generate data key succeeded
    {
        // mock succ to generate key
        auto sp = SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
        sp->set_call_back("alikms::generate_data_key::plaintext",
                          [&](void* p) { *((std::string*)p) = mock_encoded_plaintext; });
        sp->set_call_back("alikms::generate_data_key::ciphertext",
                          [&](void* p) { *((std::string*)p) = mock_encoded_ciphertext; });
        sp->set_call_back("alikms::generate_data_key::ret::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });
        sp->set_call_back("alikms::generate_data_key::ret", [](void* p) { *((int*)p) = 0; });
        sp->enable_processing();
        auto ret = init_global_encryption_key_info_map(mem_kv.get());
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(global_encryption_key_info_map.size(), 1);

        std::string key = system_meta_service_encryption_key_info_key();
        std::string val;
        std::unique_ptr<Transaction> txn;
        ret = mem_kv->create_txn(&txn);
        ret = txn->get(key, &val);
        ASSERT_EQ(ret, 0);
        EncryptionKeyInfoPB key_info;
        key_info.ParseFromString(val);
        ASSERT_EQ(key_info.items_size(), 1);
        std::cout << proto_to_json(key_info) << std::endl;
        const auto& item = key_info.items().at(0);
        ASSERT_EQ(item.key_id(), 1);
        ASSERT_EQ(global_encryption_key_info_map.at(item.key_id()), plaintext);
    }

    // Decryption failed
    {
        // clear for getting existed root key from memkv
        // do not need to mock kms encryption
        global_encryption_key_info_map.clear();

        // mock abnormal decryption
        auto sp = SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
        sp->set_call_back("alikms::decrypt::ret::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });
        sp->set_call_back("alikms::decrypt::ret", [](void* p) { *((int*)p) = -1; });
        sp->enable_processing();

        // memkv already has key info
        auto ret = init_global_encryption_key_info_map(mem_kv.get());
        ASSERT_EQ(ret, -1);
        ASSERT_TRUE(global_encryption_key_info_map.empty());
    }

    // Decryption succeeded
    {
        auto sp = SyncPoint::get_instance();
        std::unique_ptr<int, std::function<void(int*)>> defer(
                (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
        sp->set_call_back("alikms::decrypt::output",
                          [&](void* p) { *((std::string*)p) = mock_encoded_plaintext; });
        sp->set_call_back("alikms::decrypt::ret::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });
        sp->set_call_back("alikms::decrypt::ret", [](void* p) { *((int*)p) = 0; });
        sp->enable_processing();
        auto ret = init_global_encryption_key_info_map(mem_kv.get());
        ASSERT_EQ(ret, 0);
        ASSERT_EQ(global_encryption_key_info_map.size(), 1);
        ASSERT_EQ(global_encryption_key_info_map.at(1), plaintext);
    }
}