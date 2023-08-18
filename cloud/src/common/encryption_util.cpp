#include "common/encryption_util.h"

#include <math.h>
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/ossl_typ.h>
#include <sys/types.h>

#include <cstring>
#include <memory>
#include <string>
#include <string_view>
#include <thread>
#include <unordered_map>

#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "gen_cpp/selectdb_cloud.pb.h"
#include "meta-service/keys.h"
#include "meta-service/txn_kv.h"

namespace selectdb {
namespace config {
extern std::string encryption_key;
}; // namespace config

enum class EncryptionMode {
    AES_128_ECB,
    AES_256_ECB,
    AES_128_CBC,
    AES_256_CBC,
    AES_128_CFB,
    AES_256_CFB,
    AES_128_CFB1,
    AES_256_CFB1,
    AES_128_CFB8,
    AES_256_CFB8,
    AES_128_CFB128,
    AES_256_CFB128,
    AES_128_CTR,
    AES_256_CTR,
    AES_128_OFB,
    AES_256_OFB,
};

enum EncryptionState { AES_SUCCESS = 0, AES_BAD_DATA = -1 };

class EncryptionUtil {
public:
    static int encrypt(EncryptionMode mode, const unsigned char* source, uint32_t source_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* encrypt);

    static int decrypt(EncryptionMode mode, const unsigned char* encrypt, uint32_t encrypt_length,
                       const unsigned char* key, uint32_t key_length, const char* iv_str,
                       int iv_input_length, bool padding, unsigned char* decrypt_content);
};

// aes encrypty/dencrypty
static const int ENCRYPTION_MAX_KEY_LENGTH = 256;

const EVP_CIPHER* get_evp_type(const EncryptionMode mode) {
    switch (mode) {
    case EncryptionMode::AES_128_ECB:
        return EVP_aes_128_ecb();
    case EncryptionMode::AES_128_CBC:
        return EVP_aes_128_cbc();
    case EncryptionMode::AES_128_CFB:
        return EVP_aes_128_cfb();
    case EncryptionMode::AES_128_CFB1:
        return EVP_aes_128_cfb1();
    case EncryptionMode::AES_128_CFB8:
        return EVP_aes_128_cfb8();
    case EncryptionMode::AES_128_CFB128:
        return EVP_aes_128_cfb128();
    case EncryptionMode::AES_128_CTR:
        return EVP_aes_128_ctr();
    case EncryptionMode::AES_128_OFB:
        return EVP_aes_128_ofb();
    case EncryptionMode::AES_256_ECB:
        return EVP_aes_256_ecb();
    case EncryptionMode::AES_256_CBC:
        return EVP_aes_256_cbc();
    case EncryptionMode::AES_256_CFB:
        return EVP_aes_256_cfb();
    case EncryptionMode::AES_256_CFB1:
        return EVP_aes_256_cfb1();
    case EncryptionMode::AES_256_CFB8:
        return EVP_aes_256_cfb8();
    case EncryptionMode::AES_256_CFB128:
        return EVP_aes_256_cfb128();
    case EncryptionMode::AES_256_CTR:
        return EVP_aes_256_ctr();
    case EncryptionMode::AES_256_OFB:
        return EVP_aes_256_ofb();
    default:
        return nullptr;
    }
}

static uint mode_key_sizes[] = {
        128 /* AES_128_ECB */,  256 /* AES_256_ECB */,    128 /* AES_128_CBC */,
        256 /* AES_256_CBC */,  128 /* AES_128_CFB */,    256 /* AES_256_CFB */,
        128 /* AES_128_CFB1 */, 256 /* AES_256_CFB1 */,   128 /* AES_128_CFB8 */,
        256 /* AES_256_CFB8 */, 128 /* AES_128_CFB128 */, 256 /* AES_256_CFB128 */,
        128 /* AES_128_CTR */,  256 /* AES_256_CTR */,    128 /* AES_128_OFB */,
        256 /* AES_256_OFB */,
};

static void create_key(const unsigned char* origin_key, uint32_t key_length, uint8_t* encrypt_key,
                       EncryptionMode mode) {
    const uint key_size = mode_key_sizes[int(mode)] / 8;
    uint8_t* origin_key_end = ((uint8_t*)origin_key) + key_length; /* origin key boundary*/

    uint8_t* encrypt_key_end; /* encrypt key boundary */
    encrypt_key_end = encrypt_key + key_size;

    std::memset(encrypt_key, 0, key_size); /* initialize key  */

    uint8_t* ptr;        /* Start of the encrypt key*/
    uint8_t* origin_ptr; /* Start of the origin key */
    for (ptr = encrypt_key, origin_ptr = (uint8_t*)origin_key; origin_ptr < origin_key_end;
         ptr++, origin_ptr++) {
        if (ptr == encrypt_key_end) {
            /* loop over origin key until we used all key */
            ptr = encrypt_key;
        }
        *ptr ^= *origin_ptr;
    }
}

static int do_encrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* source, uint32_t source_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* encrypt, int* length_ptr) {
    int ret = EVP_EncryptInit(cipher_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(cipher_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;

    ret = EVP_EncryptUpdate(cipher_ctx, encrypt, &u_len, source, source_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_EncryptFinal(cipher_ctx, encrypt + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int EncryptionUtil::encrypt(EncryptionMode mode, const unsigned char* source,
                            uint32_t source_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* encrypt) {
    const EVP_CIPHER* cipher = get_evp_type(mode);
    /* The encrypt key to be used for encryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("SELECTDBCLOUD___");

    if (iv_str) {
        init_vec = &iv_default[0];
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = do_encrypt(cipher_ctx, cipher, source, source_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, encrypt, &length);
    EVP_CIPHER_CTX_free(cipher_ctx);
    if (ret == 0) {
        ERR_clear_error();
        return AES_BAD_DATA;
    } else {
        return length;
    }
}

static int do_decrypt(EVP_CIPHER_CTX* cipher_ctx, const EVP_CIPHER* cipher,
                      const unsigned char* encrypt, uint32_t encrypt_length,
                      const unsigned char* encrypt_key, const unsigned char* iv, bool padding,
                      unsigned char* decrypt_content, int* length_ptr) {
    int ret = EVP_DecryptInit(cipher_ctx, cipher, encrypt_key, iv);
    if (ret == 0) {
        return ret;
    }
    ret = EVP_CIPHER_CTX_set_padding(cipher_ctx, padding);
    if (ret == 0) {
        return ret;
    }
    int u_len = 0;
    ret = EVP_DecryptUpdate(cipher_ctx, decrypt_content, &u_len, encrypt, encrypt_length);
    if (ret == 0) {
        return ret;
    }
    int f_len = 0;
    ret = EVP_DecryptFinal_ex(cipher_ctx, decrypt_content + u_len, &f_len);
    *length_ptr = u_len + f_len;
    return ret;
}

int EncryptionUtil::decrypt(EncryptionMode mode, const unsigned char* encrypt,
                            uint32_t encrypt_length, const unsigned char* key, uint32_t key_length,
                            const char* iv_str, int iv_input_length, bool padding,
                            unsigned char* decrypt_content) {
    const EVP_CIPHER* cipher = get_evp_type(mode);

    /* The encrypt key to be used for decryption */
    unsigned char encrypt_key[ENCRYPTION_MAX_KEY_LENGTH / 8];
    create_key(key, key_length, encrypt_key, mode);

    int iv_length = EVP_CIPHER_iv_length(cipher);
    if (cipher == nullptr || (iv_length > 0 && !iv_str)) {
        return AES_BAD_DATA;
    }
    char* init_vec = nullptr;
    std::string iv_default("SELECTDBCLOUD___");

    if (iv_str) {
        init_vec = &iv_default[0];
        memcpy(init_vec, iv_str, std::min(iv_input_length, EVP_MAX_IV_LENGTH));
        init_vec[iv_length] = '\0';
    }
    EVP_CIPHER_CTX* cipher_ctx = EVP_CIPHER_CTX_new();
    EVP_CIPHER_CTX_reset(cipher_ctx);
    int length = 0;
    int ret = do_decrypt(cipher_ctx, cipher, encrypt, encrypt_length, encrypt_key,
                         reinterpret_cast<unsigned char*>(init_vec), padding, decrypt_content,
                         &length);
    EVP_CIPHER_CTX_free(cipher_ctx);
    if (ret > 0) {
        return length;
    } else {
        ERR_clear_error();
        return AES_BAD_DATA;
    }
}

// base64 endcode/decode

static char encoding_table[] = {'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
                                'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
                                'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
                                'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
                                '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};

static const char base64_pad = '=';

static short decoding_table[256] = {
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -1, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -1, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, 62,
        -2, -2, -2, 63, 52, 53, 54, 55, 56, 57, 58, 59, 60, 61, -2, -2, -2, -2, -2, -2, -2, 0,
        1,  2,  3,  4,  5,  6,  7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22,
        23, 24, 25, -2, -2, -2, -2, -2, -2, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 38,
        39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2,
        -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2};

static int mod_table[] = {0, 2, 1};

size_t base64_encode(const unsigned char* data, size_t length, unsigned char* encoded_data) {
    size_t output_length = (size_t)(4.0 * ceil((double)length / 3.0));

    if (encoded_data == nullptr) {
        return 0;
    }

    for (uint32_t i = 0, j = 0; i < length;) {
        uint32_t octet_a = i < length ? data[i++] : 0;
        uint32_t octet_b = i < length ? data[i++] : 0;
        uint32_t octet_c = i < length ? data[i++] : 0;
        uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

        encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
        encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[length % 3]; i++) {
        encoded_data[output_length - 1 - i] = '=';
    }

    return output_length;
}

size_t base64_decode(const char* data, size_t length, char* decoded_data) {
    const char* current = data;
    size_t ch = 0;
    size_t i = 0;
    size_t j = 0;
    size_t k = 0;

    // run through the whole string, converting as we go
    while ((ch = *current++) != '\0' && length-- > 0) {
        if (ch >= 256 || ch < 0) {
            return -1;
        }

        if (ch == base64_pad) {
            if (*current != '=' && (i % 4) == 1) {
                return -1;
            }
            continue;
        }

        ch = decoding_table[ch];
        // a space or some other separator character, we simply skip over
        if (ch == -1) {
            continue;
        } else if (ch == -2) {
            return -1;
        }

        switch (i % 4) {
        case 0:
            decoded_data[j] = ch << 2;
            break;
        case 1:
            decoded_data[j++] |= ch >> 4;
            decoded_data[j] = (ch & 0x0f) << 4;
            break;
        case 2:
            decoded_data[j++] |= ch >> 2;
            decoded_data[j] = (ch & 0x03) << 6;
            break;
        case 3:
            decoded_data[j++] |= ch;
            break;
        default:
            break;
        }

        i++;
    }

    k = j;
    /* mop things up if we ended on a boundary */
    if (ch == base64_pad) {
        switch (i % 4) {
        case 1:
            return 0;
        case 2:
            k++;
        case 3:
            decoded_data[k] = 0;
        default:
            break;
        }
    }

    decoded_data[j] = '\0';

    return j;
}

// encrypty/dencrypty with base64

static std::unordered_map<std::string, EncryptionMode> to_encryption_mode {
        {"AES_128_ECB", EncryptionMode::AES_128_ECB},
        {"AES_256_ECB", EncryptionMode::AES_256_ECB},
        {"AES_128_CBC", EncryptionMode::AES_128_CBC},
        {"AES_256_CBC", EncryptionMode::AES_256_CBC},
        {"AES_128_CFB", EncryptionMode::AES_128_CFB},
        {"AES_256_CFB", EncryptionMode::AES_256_CFB},
        {"AES_128_CFB1", EncryptionMode::AES_128_CFB1},
        {"AES_256_CFB1", EncryptionMode::AES_256_CFB1},
        {"AES_128_CFB8", EncryptionMode::AES_128_CFB8},
        {"AES_256_CFB8", EncryptionMode::AES_256_CFB8},
        {"AES_128_CFB128", EncryptionMode::AES_128_CFB128},
        {"AES_256_CFB128", EncryptionMode::AES_256_CFB128},
        {"AES_128_CTR", EncryptionMode::AES_128_CTR},
        {"AES_256_CTR", EncryptionMode::AES_256_CTR},
        {"AES_128_OFB", EncryptionMode::AES_128_OFB},
        {"AES_256_OFB", EncryptionMode::AES_256_OFB},
};

static inline int encrypt_to_base64_impl(std::string_view source, EncryptionMode mode,
                                         const std::string& key, std::string* encrypt) {
    /*
     * Buffer for ciphertext. Ensure the buffer is long enough for the
     * ciphertext which may be longer than the plaintext, depending on the
     * algorithm and mode.
     */
    int cipher_len = source.length() + 16;
    std::unique_ptr<unsigned char[]> cipher_text(new unsigned char[cipher_len]);
    int cipher_text_len = EncryptionUtil::encrypt(
            mode, (unsigned char*)source.data(), source.length(), (unsigned char*)key.c_str(),
            key.length(), nullptr, 0, true, cipher_text.get());
    if (cipher_text_len < 0) {
        return -1;
    }

    int encoded_len = (size_t)(4.0 * ceil(cipher_text_len / 3.0));
    std::unique_ptr<unsigned char[]> encoded_text(new unsigned char[encoded_len]);
    int encoded_text_len = base64_encode(cipher_text.get(), cipher_text_len, encoded_text.get());
    if (encoded_text_len < 0) {
        return -1;
    }
    encrypt->assign((char*)encoded_text.get(), encoded_text_len);
    return 0;
}

static int encrypt_to_base64(std::string_view source, const std::string& encrypt_method,
                             const std::string& key, std::string* encrypt) {
    if (source.empty()) {
        *encrypt = "";
        return 0;
    }
    auto iter = to_encryption_mode.find(encrypt_method);
    if (iter == to_encryption_mode.end()) {
        return -1;
    }

    return encrypt_to_base64_impl(source, iter->second, key, encrypt);
}

static inline int decrypt_with_base64_impl(std::string_view encrypt, EncryptionMode mode,
                                           const std::string& key, std::string* source) {
    // base64
    std::unique_ptr<char[]> decoded_text(new char[encrypt.length()]);
    int decoded_text_len = base64_decode(encrypt.data(), encrypt.length(), decoded_text.get());
    if (decoded_text_len < 0) {
        return -1;
    }

    std::unique_ptr<char[]> plain_text(new char[decoded_text_len]);
    int plain_text_len = EncryptionUtil::decrypt(
            mode, (unsigned char*)decoded_text.get(), decoded_text_len, (unsigned char*)key.c_str(),
            key.length(), nullptr, 0, true, (unsigned char*)plain_text.get());
    if (plain_text_len < 0) {
        return -1;
    }
    source->assign(plain_text.get(), plain_text_len);
    return 0;
}

static int decrypt_with_base64(std::string_view encrypt, const std::string& encrypt_method,
                               const std::string& key, std::string* source) {
    if (encrypt.empty()) {
        *source = "";
        return 0;
    }
    auto iter = to_encryption_mode.find(encrypt_method);
    if (iter == to_encryption_mode.end()) {
        return -1;
    }
    return decrypt_with_base64_impl(encrypt, iter->second, key, source);
}

int encrypt_ak_sk(AkSkRef plain_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* cipher_ak_sk) {
    std::string encrypt_ak;
    std::string encrypt_sk;
    if (encrypt_to_base64(plain_ak_sk.second, encryption_method, encryption_key, &encrypt_sk) !=
        0) {
        *cipher_ak_sk = {"", ""};
        return -1;
    }
    *cipher_ak_sk = {std::string(plain_ak_sk.first), std::move(encrypt_sk)};
    return 0;
}

int decrypt_ak_sk(AkSkRef cipher_ak_sk, const std::string& encryption_method,
                  const std::string& encryption_key, AkSkPair* plain_ak_sk) {
    std::string ak;
    std::string sk;
    if (decrypt_with_base64(cipher_ak_sk.second, encryption_method, encryption_key, &sk) != 0) {
        *plain_ak_sk = {"", ""};
        return -1;
    }
    *plain_ak_sk = {std::string(cipher_ak_sk.first), std::move(sk)};
    return 0;
}

// Does not need to be locked, only generated when the process is initialized
std::map<int64_t, std::string> global_encryption_key_info_map; // key_id->encryption_key

int decrypt_ak_sk_helper(std::string_view cipher_ak, std::string_view cipher_sk,
                         const EncryptionInfoPB& encryption_info, AkSkPair* plain_ak_sk_pair) {
    std::string key;
    int ret = get_encryption_key_for_ak_sk(encryption_info.key_id(), &key);
    {
        TEST_SYNC_POINT_CALLBACK("decrypt_ak_sk:get_encryption_key_ret", &ret);
        TEST_SYNC_POINT_CALLBACK("decrypt_ak_sk:get_encryption_key", &key);
    }
    if (ret != 0) {
        LOG(WARNING) << "failed to get encryption key version_id: " << encryption_info.key_id();
        return -1;
    }
    ret = decrypt_ak_sk({cipher_ak, cipher_sk}, encryption_info.encryption_method(), key,
                        plain_ak_sk_pair);
    if (ret != 0) {
        LOG(WARNING) << "failed to decrypt";
        return -1;
    }
    return 0;
}

int init_global_encryption_key_info_map(std::shared_ptr<TxnKv> txn_kv) {
    while (true) {
        std::string key = system_meta_service_encryption_key_info_key();
        std::string val;
        std::unique_ptr<Transaction> txn;
        int ret = txn_kv->create_txn(&txn);
        if (ret != 0) return -1;
        ret = txn->get(key, &val);
        if (ret != 0 && ret != 1) return -1;
        if (ret == 1 && selectdb::config::encryption_key.empty()) {
            LOG_WARNING("the server encryption_key is not found and the config is not set");
            return -1;
        }

        EncryptionKeyInfoPB key_info;
        if (ret != 1) {
            if (!key_info.ParseFromString(val)) {
                LOG_WARNING("fail to parse encryption_key");
                return -1;
            }

            LOG_INFO("get server encryption_key")
                .tag("key_info", proto_to_json(key_info));
        }

        bool add_new_encryption_key = !selectdb::config::encryption_key.empty();
        for (auto& item : key_info.items()) {
            if (!selectdb::config::encryption_key.empty() &&
                selectdb::config::encryption_key == item.key()) {
                add_new_encryption_key = false;
            }
            std::string decoded_string(item.key().length(), '0');
            int decoded_text_len =
                    base64_decode(item.key().c_str(), item.key().length(), decoded_string.data());
            if (decoded_text_len < 0) {
                LOG_WARNING("fail to decode encryption_key");
                return -1;
            }
            decoded_string.assign(decoded_string.data(), decoded_text_len);
            global_encryption_key_info_map.insert({item.key_id(), decoded_string});
        }
        if (add_new_encryption_key) {
            std::string decoded_string(selectdb::config::encryption_key.length(), '0');
            int decoded_text_len =
                    base64_decode(selectdb::config::encryption_key.c_str(),
                                  selectdb::config::encryption_key.length(), decoded_string.data());
            if (decoded_text_len < 0) {
                LOG_WARNING("fail to decode config encryption_key");
                return -1;
            }
            decoded_string.assign(decoded_string.data(), decoded_text_len);

            int32_t new_key_id = key_info.items().size() + 1;
            auto item = key_info.add_items();
            item->set_key_id(new_key_id);
            item->set_key(selectdb::config::encryption_key);
            val = key_info.SerializeAsString();
            if (val.empty()) return -1;
            txn->put(key, val);
            LOG_INFO("put server encryption_key")
                .tag("encryption_key", selectdb::config::encryption_key)
                .tag("key_id", new_key_id);
            ret = txn->commit();
            if (ret == -1) {
                LOG_WARNING("commit encryption_key is conflicted, retry it later");
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                continue;
            } else if (ret != 0) {
                LOG_WARNING("failed to commit encryption_key");
                return -1;
            }
            global_encryption_key_info_map.insert({new_key_id, decoded_string});
        }
        return 0;
    }
}

} // namespace selectdb
