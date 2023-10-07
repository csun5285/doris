#include "common/sync_point.h"
#include "meta-service/meta_service_http.h"

#include <brpc/uri.h>
#include <gtest/gtest.h>

using namespace selectdb;

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

TEST(HttpEncodeKeyTest, process_http_encode_key_test) {
    brpc::URI uri;
    HttpResponse http_res;

    // test unsupported key type
    uri.SetQuery("key_type", "foobarkey");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("key_type not supported"), std::string::npos);

    // test missing argument
    uri.SetQuery("key_type", "MetaRowsetKey");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("instance_id is not given or empty"), std::string::npos) << http_res.body;

    // clang-format-off
    std::string unicode_res = R"(┌─────────────────────────────────────────────────────────────────────────────────────── 0. key space: 1
│ ┌───────────────────────────────────────────────────────────────────────────────────── 1. meta
│ │             ┌─────────────────────────────────────────────────────────────────────── 2. gavin-instance
│ │             │                                 ┌───────────────────────────────────── 3. rowset
│ │             │                                 │                 ┌─────────────────── 4. 10086
│ │             │                                 │                 │                 ┌─ 5. 10010
│ │             │                                 │                 │                 │ 
▼ ▼             ▼                                 ▼                 ▼                 ▼ 
01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a
\x01\x10\x6d\x65\x74\x61\x00\x01\x10\x67\x61\x76\x69\x6e\x2d\x69\x6e\x73\x74\x61\x6e\x63\x65\x00\x01\x10\x72\x6f\x77\x73\x65\x74\x00\x01\x12\x00\x00\x00\x00\x00\x00\x27\x66\x12\x00\x00\x00\x00\x00\x00\x27\x1a
)";

    std::string nonunicode_res = R"(/--------------------------------------------------------------------------------------- 0. key space: 1
| /------------------------------------------------------------------------------------- 1. meta
| |             /----------------------------------------------------------------------- 2. gavin-instance
| |             |                                 /------------------------------------- 3. rowset
| |             |                                 |                 /------------------- 4. 10086
| |             |                                 |                 |                 /- 5. 10010
| |             |                                 |                 |                 | 
v v             v                                 v                 v                 v 
01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a
\x01\x10\x6d\x65\x74\x61\x00\x01\x10\x67\x61\x76\x69\x6e\x2d\x69\x6e\x73\x74\x61\x6e\x63\x65\x00\x01\x10\x72\x6f\x77\x73\x65\x74\x00\x01\x12\x00\x00\x00\x00\x00\x00\x27\x66\x12\x00\x00\x00\x00\x00\x00\x27\x1a
)";
    // clang-format-on

    // test normal path, with unicode
    uri.SetQuery("instance_id", "gavin-instance");
    uri.SetQuery("tablet_id", "10086");
    uri.SetQuery("version", "10010");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 200);
    EXPECT_EQ(http_res.body, unicode_res);

    // test normal path, with non-unicode
    uri.SetQuery("unicode", "false");
    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 200);
    EXPECT_EQ(http_res.body, nonunicode_res);

    // test empty body branch
    auto sp = SyncPoint::get_instance();
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [](int*) { SyncPoint::get_instance()->clear_all_call_backs(); });
    sp->set_call_back("process_http_encode_key::empty_body", [](void* p) { ((std::string*)p)->clear(); });
    sp->enable_processing();

    http_res = process_http_encode_key(uri);
    EXPECT_EQ(http_res.status_code, 400);
    EXPECT_NE(http_res.body.find("failed to decode encoded key"), std::string::npos);
}


TEST(HttpEncodeKeyTest, process_http_encode_key_test_cover_all_template) {
    struct ParamAndResult {
        std::string param;
        std::string result;
    };
    // clang-format-off
    // key_type -> {params, expected_output}
    std::unordered_map<std::string, ParamAndResult> test_input {
        {"InstanceKey",                {"instance_id=gavin-instance",                                                                "0110696e7374616e6365000110676176696e2d696e7374616e63650001"                                                                                              }},
        {"TxnLabelKey",                {"instance_id=gavin-instance&db_id=10086&label=test-label",                                   "011074786e000110676176696e2d696e7374616e636500011074786e5f6c6162656c000112000000000000276610746573742d6c6162656c0001"                                    }},
        {"TxnInfoKey",                 {"instance_id=gavin-instance&db_id=10086&txn_id=10010",                                       "011074786e000110676176696e2d696e7374616e636500011074786e5f696e666f000112000000000000276612000000000000271a"                                              }},
        {"TxnIndexKey",                {"instance_id=gavin-instance&txn_id=10086",                                                   "011074786e000110676176696e2d696e7374616e636500011074786e5f696e6465780001120000000000002766"                                                              }},
        {"TxnRunningKey",              {"instance_id=gavin-instance&db_id=10086&txn_id=10010",                                       "011074786e000110676176696e2d696e7374616e636500011074786e5f72756e6e696e67000112000000000000276612000000000000271a"                                        }},
        {"VersionKey",                 {"instance_id=gavin-instance&db_id=10086&tbl_id=10010&partition_id=10000",                    "011076657273696f6e000110676176696e2d696e7374616e6365000110706172746974696f6e000112000000000000276612000000000000271a120000000000002710"                  }},
        {"MetaRowsetKey",              {"instance_id=gavin-instance&tablet_id=10086&version=10010",                                  "01106d657461000110676176696e2d696e7374616e6365000110726f77736574000112000000000000276612000000000000271a"                                                }},
        {"MetaRowsetTmpKey",           {"instance_id=gavin-instance&txn_id=10086&tablet_id=10010",                                   "01106d657461000110676176696e2d696e7374616e6365000110726f777365745f746d70000112000000000000276612000000000000271a"                                        }},
        {"MetaTabletKey",              {"instance_id=gavin-instance&table_id=10086&index_id=100010&part_id=10000&tablet_id=1008601", "01106d657461000110676176696e2d696e7374616e63650001107461626c657400011200000000000027661200000000000186aa1200000000000027101200000000000f63d9"            }},
        {"MetaTabletIdxKey",           {"instance_id=gavin-instance&tablet_id=10086",                                                "01106d657461000110676176696e2d696e7374616e63650001107461626c65745f696e6465780001120000000000002766"                                                      }},
        {"RecycleIndexKey",            {"instance_id=gavin-instance&index_id=10086",                                                 "011072656379636c65000110676176696e2d696e7374616e6365000110696e6465780001120000000000002766"                                                              }},
        {"RecyclePartKey",             {"instance_id=gavin-instance&part_id=10086",                                                  "011072656379636c65000110676176696e2d696e7374616e6365000110706172746974696f6e0001120000000000002766"                                                      }},
        {"RecycleRowsetKey",           {"instance_id=gavin-instance&tablet_id=10086&rowset_id=10010",                                "011072656379636c65000110676176696e2d696e7374616e6365000110726f7773657400011200000000000027661031303031300001"                                            }},
        {"RecycleTxnKey",              {"instance_id=gavin-instance&db_id=10086&txn_id=10010",                                       "011072656379636c65000110676176696e2d696e7374616e636500011074786e000112000000000000276612000000000000271a"                                                }},
        {"StatsTabletKey",             {"instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008601",  "01107374617473000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9"          }},
        {"JobTabletKey",               {"instance_id=gavin-instance&table_id=10086&index_id=10010&part_id=10000&tablet_id=1008601",  "01106a6f62000110676176696e2d696e7374616e63650001107461626c6574000112000000000000276612000000000000271a1200000000000027101200000000000f63d9"              }},
        {"CopyJobKey",                 {"instance_id=gavin-instance&stage_id=10086&table_id=10010&copy_id=10000&group_id=1008601",   "0110636f7079000110676176696e2d696e7374616e63650001106a6f620001103130303836000112000000000000271a10313030303000011200000000000f63d9"                      }},
        {"CopyFileKey",                {"instance_id=gavin-instance&stage_id=10086&table_id=10010&obj_key=10000&obj_etag=1008601",   "0110636f7079000110676176696e2d696e7374616e63650001106c6f6164696e675f66696c650001103130303836000112000000000000271a103130303030000110313030383630310001"  }},
        {"RecycleStageKey",            {"instance_id=gavin-instance&stage_id=10086",                                                 "011072656379636c65000110676176696e2d696e7374616e6365000110737461676500011031303038360001"                                                                }},
        {"JobRecycleKey",              {"instance_id=gavin-instance",                                                                "01106a6f62000110676176696e2d696e7374616e6365000110636865636b0001"                                                                                        }},
        {"MetaSchemaKey",              {"instance_id=gavin-instance&index_id=10086&schema_version=10010",                            "01106d657461000110676176696e2d696e7374616e6365000110736368656d61000112000000000000276612000000000000271a"                                                }},
        {"MetaDeleteBitmap",           {"instance_id=gavin-instance&tablet_id=10086&rowest_id=10010&version=10000&seg_id=1008601",   "01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d6170000112000000000000276610313030313000011200000000000027101200000000000f63d9"}},
        {"MetaDeleteBitmapUpdateLock", {"instance_id=gavin-instance&table_id=10086&partition_id=10010",                              "01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d61705f6c6f636b000112000000000000276612000000000000271a"                        }},
        {"MetaPendingDeleteBitmap",    {"instance_id=gavin-instance&tablet_id=10086",                                                "01106d657461000110676176696e2d696e7374616e636500011064656c6574655f6269746d61705f70656e64696e670001120000000000002766"                                    }},
        {"RLJobProgressKey",           {"instance_id=gavin-instance&db_id=10086&job_id=10010",                                       "01106a6f62000110676176696e2d696e7374616e6365000110726f7574696e655f6c6f61645f70726f6772657373000112000000000000276612000000000000271a"                    }},
        {"MetaServiceRegistryKey",     {"",                                                                                          "021073797374656d0001106d6574612d7365727669636500011072656769737472790001"                                                                                }},
        {"MetaServiceArnInfoKey",      {"",                                                                                          "021073797374656d0001106d6574612d7365727669636500011061726e5f696e666f0001"                                                                                }},
        {"MetaServiceEncryptionKey",   {"",                                                                                          "021073797374656d0001106d6574612d73657276696365000110656e6372797074696f6e5f6b65795f696e666f0001"                                                          }},
    };
    // clang-format-on

    for (auto& [k, v] : test_input) {
        std::stringstream url;
        url << "localhost:5000/MetaService/http?key_type=" << k;
        if (!v.param.empty()) { url << "&" << v.param; }
        // std::cout << url.str() << std::endl;
        brpc::URI uri;
        EXPECT_EQ(uri.SetHttpURL(url.str()), 0); // clear and set query string
        (void) uri.get_query_map(); // initialize query map
        auto http_res = process_http_encode_key(uri);
        EXPECT_EQ(http_res.status_code, 200);
        EXPECT_NE(http_res.body.find(v.result), std::string::npos)
            << "real full text: " << http_res.body << "\nexpect contains: " << v.result;
    }
}

// vim: et tw=80 ts=4 sw=4 cc=80:
