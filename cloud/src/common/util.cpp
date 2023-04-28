
// clang-format off
#include "util.h"
// FIXME: we should not rely other modules that may rely on this common module
#include "meta-service/keys.h"
#include "meta-service/codec.h"

#include "google/protobuf/util/json_util.h"

#include <iomanip>
#include <sstream>
#include <unordered_map>
#include <variant>
// clang-format on

namespace selectdb {

/**
 * This is a naïve implementation of hex, DONOT use it on retical path.
 */
std::string hex(std::string_view str) {
    std::stringstream ss;
    for (auto& i : str) {
        ss << std::hex << std::setw(2) << std::setfill('0') << ((int16_t)i & 0xff);
    }
    return ss.str();
}

/**
 * This is a naïve implementation of unhex.
 */
std::string unhex(std::string_view hex_str) {
    // clang-format off
    const static std::unordered_map<char, char> table = {
            {'0', 0},  {'1', 1},  {'2', 2},  {'3', 3},  {'4', 4}, 
            {'5', 5},  {'6', 6},  {'7', 7},  {'8', 8},  {'9', 9}, 
            {'a', 10}, {'b', 11}, {'c', 12}, {'d', 13}, {'e', 14}, {'f', 15},
            {'A', 10}, {'B', 11}, {'C', 12}, {'D', 13}, {'E', 14}, {'F', 15}};
    [[maybe_unused]] static int8_t lut[std::max({'9', 'f', 'F'}) + 1];
    lut[(int)'0'] = 0; lut[(int)'1'] = 1; lut[(int)'2'] = 2; lut[(int)'3'] = 3; lut[(int)'4'] = 4; lut[(int)'5'] = 5; lut[(int)'6'] = 6; lut[(int)'7'] = 7; lut[(int)'8'] = 8; lut[(int)'9'] = 9;
    lut[(int)'a'] = 10; lut[(int)'b'] = 11; lut[(int)'c'] = 12; lut[(int)'d'] = 13; lut[(int)'e'] = 14; lut[(int)'f'] = 15;
    lut[(int)'A'] = 10; lut[(int)'B'] = 11; lut[(int)'C'] = 12; lut[(int)'D'] = 13; lut[(int)'E'] = 14; lut[(int)'F'] = 15;
    // clang-format on
    size_t len = hex_str.length();
    len &= ~0x01UL;
    std::string buf(len >> 1, '\0');
    for (size_t i = 0; i < len; ++i) {
        const auto it = table.find(hex_str[i]);
        if (it == table.end()) break;
        buf[i >> 1] |= i & 0x1 ? (it->second & 0x0f) : (it->second & 0x0f) << 4;
    }
    return buf;
}

static std::string explain_fields(std::string_view text, const std::vector<std::string>& fields,
                                  const std::vector<int>& pos, bool unicode = false) {
    if (fields.size() != pos.size() || fields.size() == 0 || pos.size() == 0) {
        return std::string(text.data(), text.size());
    }
    size_t last_hyphen_pos = pos.back() + 1;
    std::stringstream ss;
    std::string blank_line(last_hyphen_pos + 1, ' ');

    // clang-format off
    static const std::string hyphen("\xe2\x94\x80"); // ─ e2 94 80
    static const std::string bar   ("\xe2\x94\x82"); // │ e2 94 82
    static const std::string angle ("\xe2\x94\x8c"); // ┌ e2 94 8c
    static const std::string arrow ("\xe2\x96\xbc"); // ▼ e2 96 bc
    // clang-format on

    // Each line with hyphens
    for (size_t i = 0; i < fields.size(); ++i) {
        std::string line = blank_line;
        line[pos[i]] = '/';
        int nbar = i;
        for (size_t j = 0; j < i; ++j) {
            line[pos[j]] = '|';
        }
        int nhyphen = 0;
        for (size_t j = pos[i] + 1; j <= last_hyphen_pos; ++j) {
            line[j] = '-';
            ++nhyphen;
        }

        if (unicode) {
            int i = line.size();
            line.resize(line.size() + 2 * (1 /*angle*/ + nbar + nhyphen), ' ');
            int j = line.size();
            while (--i >= 0) {
                if (line[i] == '-') {
                    line[--j] = hyphen[2];
                    line[--j] = hyphen[1];
                    line[--j] = hyphen[0];
                } else if (line[i] == '|') {
                    line[--j] = bar[2];
                    line[--j] = bar[1];
                    line[--j] = bar[0];
                } else if (line[i] == '/') {
                    line[--j] = angle[2];
                    line[--j] = angle[1];
                    line[--j] = angle[0];
                } else {
                    --j;
                    continue;
                }
                line[i] = i != j ? ' ' : line[i]; // Replace if needed
            }
        }

        ss << line << " " << i << ". " << fields[i] << "\n";
    }

    // Mark position indicator
    std::string line = blank_line;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = '|';
    }

    if (unicode) {
        int i = line.size();
        line.resize(line.size() + 2 * fields.size(), ' ');
        int j = line.size();
        while (--i >= 0) {
            if (line[i] != '|') {
                --j;
                continue;
            }
            line[--j] = bar[2];
            line[--j] = bar[1];
            line[--j] = bar[0];
            line[i] = i != j ? ' ' : line[i]; // Replace if needed
        }
    }

    ss << line << "\n";

    line = blank_line;
    for (size_t i = 0; i < fields.size(); ++i) {
        line[pos[i]] = 'v';
    }

    if (unicode) {
        int i = line.size();
        line.resize(line.size() + 2 * fields.size(), ' ');
        int j = line.size();
        while (--i >= 0) {
            if (line[i] != 'v') {
                --j;
                continue;
            }
            line[--j] = arrow[2];
            line[--j] = arrow[1];
            line[--j] = arrow[0];
            line[i] = i != j ? ' ' : line[i]; // Replace if needed
        }
    }

    ss << line << "\n";

    // Original text to explain
    ss << text << "\n";

    return ss.str();
}

std::string prettify_key(std::string_view key_hex, bool unicode) {
    // Decoded result container
    //                                    val                  tag  pos
    //                     .---------------^----------------.  .^.  .^.
    std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> fields;
    std::string unhex_key = unhex(key_hex);
    int key_space = unhex_key[0];
    std::string_view key_copy = unhex_key;
    key_copy.remove_prefix(1); // Remove the first key space byte
    int ret = decode_key(&key_copy, &fields);
    if (ret != 0) return "";

    std::vector<std::string> fields_str;
    std::vector<int> fields_pos;
    fields_str.reserve(fields.size() + 1);
    fields_pos.reserve(fields.size() + 1);
    // Key space byte
    fields_str.push_back("key space: " + std::to_string(key_space));
    fields_pos.push_back(0);

    for (auto& i : fields) {
        fields_str.emplace_back(std::get<1>(i) == EncodingTag::BYTES_TAG
                                        ? std::get<std::string>(std::get<0>(i))
                                        : std::to_string(std::get<int64_t>(std::get<0>(i))));
        fields_pos.push_back((std::get<2>(i) + 1) * 2);
    }

    return explain_fields(key_hex, fields_str, fields_pos, unicode);
}

std::string proto_to_json(const ::google::protobuf::Message& msg, bool add_whitespace) {
    std::string json;
    google::protobuf::util::JsonPrintOptions opts;
    opts.add_whitespace = add_whitespace;
    opts.preserve_proto_field_names = true;
    google::protobuf::util::MessageToJsonString(msg, &json, opts);
    return json;
}

std::vector<std::string_view> split_string(const std::string_view& str, int n) {
    std::vector<std::string_view> substrings;

    for (size_t i = 0; i < str.size(); i += n) {
        substrings.push_back(str.substr(i, n));
    }

    return substrings;
}

int remove(const std::string& key, Transaction* txn) {
    std::string key0 = key;
    selectdb::encode_int64(0, &key0);
    std::string key1 = key;
    selectdb::encode_int64(std::numeric_limits<int64_t>::max(), &key1);
    VLOG_DEBUG << "remove key_start=" << hex(key0) << " key_end=" << hex(key1);
    txn->remove(key0, key1);
    auto ret = txn->commit();
    if (ret != 0) {
        std::string msg = ret == -1 ? "KV_TXN_CONFLICT" : "KV_TXN_COMMIT_ERR";
        LOG(WARNING) << fmt::format("failed to commit kv txn, ret={}, reason={}", ret, msg);
        return -1;
    }
    return 0;
}

int get(const std::string& key, Transaction* txn, google::protobuf::Message* pb) {
    int ret = 0;
    if (txn == nullptr) {
        LOG(WARNING) << "not give txn";
        return -1;
    }

    // give a key prefix, range get key
    std::string key0 = key;
    selectdb::encode_int64(0, &key0);
    std::string key1 = key;
    selectdb::encode_int64(std::numeric_limits<int64_t>::max(), &key1);
    std::unique_ptr<RangeGetIterator> it;
    butil::IOBuf merge;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) {
                key0 = k;
            }
            // merge value to string
            merge.append(v.data(), v.size());
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    butil::IOBufAsZeroCopyInputStream merge_stream(merge);
    if (merge.size() == 0) {
        LOG(INFO) << "not found key=" << hex(key);
        return 1;
    }

    // string to PB
    if (!pb->ParseFromZeroCopyStream(&merge_stream)) {
        LOG(WARNING) << "value deserialize to pb err" << typeid(pb).name();
        return -1;
    }

    return 0;
}

int put(const std::string& key, Transaction* txn,
        const google::protobuf::Message& pb, size_t value_limit) {
    assert(value_limit > 0 && value_limit <= 90 * 1000);
    int ret = 0;
    if (txn == nullptr) {
        LOG(WARNING) << "not give txn";
        return -1;
    }

    std::string value;
    if (!pb.SerializeToString(&value)) {
        LOG(WARNING) << "failed to serialize pb to string";
        return -1;
    }

    // after test, if > 99946, fdb commit error, code=2103 msg=Value length exceeds limit
    // const int value_limit = 90 * 1000;
    auto split_vec = split_string(value, value_limit);
    // LOG(INFO) << "value.size=" << value.size() << " vec=" << split_vec.size();
    // generate key suffix
    std::vector<std::string> keys;
    keys.reserve(split_vec.size());
    for (int i = 0; i < split_vec.size(); i++) {
        keys.emplace_back(key);
        selectdb::encode_int64(i, &keys.back());
        txn->put(keys.back(), split_vec.at(i));
        VLOG_DEBUG << "put key=" << hex(keys.back()) << " i=" << i;
    }
    // remove not need key, [split_vec.size, max_int_64)
    std::string remove_key0 = key;
    selectdb::encode_int64(split_vec.size(), &remove_key0);
    std::string remove_key1 = key;
    selectdb::encode_int64(std::numeric_limits<int64_t>::max(), &remove_key1);
    VLOG_DEBUG << "remove key_start=" << hex(remove_key0) << " key_end=" << hex(remove_key1);
    txn->remove(remove_key0, remove_key1);

    ret = txn->commit();
    if (ret != 0) {
        std::string msg = ret == -1 ? "KV_TXN_CONFLICT" : "KV_TXN_COMMIT_ERR";
        LOG(WARNING) << fmt::format("failed to commit kv txn, ret={}, reason={}", ret, msg);
        return -1;
    }
    return 0;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
