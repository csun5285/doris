#pragma once

#include <alibabacloud/core/CommonClient.h>

#include <memory>
#include <string>
#include <string_view>

namespace selectdb {

struct KmsConf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string cmk;
    std::string provider;
};

class KmsClient {
public:
    KmsClient(KmsConf&& conf) : conf_(std::move(conf)) {}
    virtual ~KmsClient() = default;

    const KmsConf& conf() const { return conf_; }

    // returns 0 for success otherwise error
    virtual int init() = 0;

    /**
    * @brief This function encrypts the plaintext.
    *
    * @param plaintext The plaintext (base64-encoded)  to be encrypted.
    * @param output Output the ciphertext (base64-encoded).
    * @return int Returns 0 on success and -1 on failure.
    */
    virtual int encrypt(const std::string& plaintext, std::string* output) = 0;

    /**
    * @brief This function decrypts the ciphertext.
    *
    * @param ciphertext The ciphertext (base64-encoded) to be decrypted.
    * @param output Output the decrypted (base64-encoded) plaintext.
    * @return int Returns 0 on success and -1 on failure.
    */
    virtual int decrypt(const std::string& ciphertext, std::string* output) = 0;

    /**
     * @brief This function generate data key
     * 
     * @param ciphertext return ciphertext (base64-encoded)
     * @param plaintext  return plaintext (base64-encoded)
     * @return int Returns 0 on success and -1 on failure. 
     */
    virtual int generate_data_key(std::string* ciphertext, std::string* plaintext) = 0;

protected:
    KmsConf conf_;
};

int create_kms_client(KmsConf&& conf, std::unique_ptr<KmsClient>* kms_client);

class AliKmsClient : public KmsClient {
public:
    explicit AliKmsClient(KmsConf&& conf);
    ~AliKmsClient() override;

    // returns 0 for success otherwise error
    int init() override;

    /**
    * @brief This function encrypts the plaintext.
    *
    * @param plaintext The plaintext (base64-encoded)  to be encrypted.
    * @param output Output the ciphertext (base64-encoded).
    * @return int Returns 0 on success and -1 on failure.
    */
    int encrypt(const std::string& plaintext, std::string* output) override;

    /**
    * @brief This function decrypts the ciphertext.
    *
    * @param ciphertext The ciphertext (base64-encoded) to be decrypted.
    * @param output Output the decrypted (base64-encoded) plaintext.
    * @return int Returns 0 on success and -1 on failure.
    */
    int decrypt(const std::string& ciphertext, std::string* output) override;

    /**
     * @brief This function generate data key
     * 
     * @param ciphertext return ciphertext (base64-encoded)
     * @param plaintext  return plaintext (base64-encoded)
     * @return int Returns 0 on success and -1 on failure. 
     */
    int generate_data_key(std::string* ciphertext, std::string* plaintext) override;

private:
    std::unique_ptr<AlibabaCloud::CommonClient> kms_client_;
};

} // namespace selectdb