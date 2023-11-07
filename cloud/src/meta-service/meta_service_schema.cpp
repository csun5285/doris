#include "meta-service/meta_service_schema.h"

#include <fmt/format.h>

#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"
#include "meta-service/keys.h"
#include "meta-service/meta_service_helper.h"
#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

namespace selectdb {
namespace config {
extern int16_t meta_schema_value_version;
}

void put_schema_kv(MetaServiceCode& code, std::string& msg, Transaction* txn,
                   std::string_view schema_key, const doris::TabletSchemaPB& schema) {
    TxnErrorCode err = selectdb::key_exists(txn, schema_key);
    if (err == TxnErrorCode::TXN_OK) { // schema has already been saved
        TEST_SYNC_POINT_RETURN_WITH_VOID("put_schema_kv:schema_key_exists_return");
        DCHECK([&] {
            auto transform = [](std::string_view type) -> std::string_view {
                if (type == "DECIMALV2") return "DECIMAL";
                if (type == "BITMAP") return "OBJECT";
                return type;
            };
            ValueBuf buf;
            auto err = selectdb::get(txn, schema_key, &buf);
            if (err != TxnErrorCode::TXN_OK) {
                LOG(WARNING) << "failed to get schema, err=" << err;
                return false;
            }
            doris::TabletSchemaPB saved_schema;
            if (!buf.to_pb(&saved_schema)) {
                LOG(WARNING) << "failed to parse schema value";
                return false;
            }
            if (saved_schema.column_size() != schema.column_size()) {
                LOG(WARNING) << "saved_schema.column_size()=" << saved_schema.column_size()
                             << " schema.column_size()=" << schema.column_size();
                return false;
            }
            // Sort by column id
            std::sort(saved_schema.mutable_column()->begin(), saved_schema.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            auto& schema_ref = const_cast<doris::TabletSchemaPB&>(schema);
            std::sort(schema_ref.mutable_column()->begin(), schema_ref.mutable_column()->end(),
                      [](auto& c1, auto& c2) { return c1.unique_id() < c2.unique_id(); });
            for (int i = 0; i < saved_schema.column_size(); ++i) {
                auto& saved_column = saved_schema.column(i);
                auto& column = schema.column(i);
                if (saved_column.unique_id() != column.unique_id() ||
                    transform(saved_column.type()) != transform(column.type())) {
                    LOG(WARNING) << "existed column: " << saved_column.DebugString()
                                 << "\nto save column: " << column.DebugString();
                    return false;
                }
            }
            if (saved_schema.index_size() != schema.index_size()) {
                LOG(WARNING) << "saved_schema.index_size()=" << saved_schema.column_size()
                             << " schema.index_size()=" << schema.column_size();
                return false;
            }
            // Sort by index id
            std::sort(saved_schema.mutable_index()->begin(), saved_schema.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            std::sort(schema_ref.mutable_index()->begin(), schema_ref.mutable_index()->end(),
                      [](auto& i1, auto& i2) { return i1.index_id() < i2.index_id(); });
            for (int i = 0; i < saved_schema.index_size(); ++i) {
                auto& saved_index = saved_schema.index(i);
                auto& index = schema.index(i);
                if (saved_index.index_id() != index.index_id() ||
                    saved_index.index_type() != index.index_type()) {
                    LOG(WARNING) << "existed index: " << saved_index.DebugString()
                                 << "\nto save index: " << index.DebugString();
                    return false;
                }
            }
            return true;
        }()) << hex(schema_key)
             << "\n to_save: " << schema.ShortDebugString();
        return;
    } else if (err != TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = "failed to check that key exists";
        code = cast_as<ErrCategory::READ>(err);
        return;
    }
    uint8_t ver = config::meta_schema_value_version;
    if (ver > 0) {
        selectdb::put(txn, schema_key, schema, ver);
    } else {
        auto schema_value = schema.SerializeAsString();
        txn->put(schema_key, schema_value);
    }
}

bool parse_schema_value(const ValueBuf& buf, doris::TabletSchemaPB* schema) {
    // TODO(plat1ko): Apply decompression based on value version
    return buf.to_pb(schema);
}

} // namespace selectdb
