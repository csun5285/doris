#pragma once

#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/selectdb_cloud.pb.h>

namespace selectdb {
class Transaction;
struct ValueBuf;

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaPB& schema);

// Return true if parse success
[[nodiscard]] bool parse_schema_value(const ValueBuf& buf, doris::TabletSchemaPB* schema);

} // namespace selectdb
