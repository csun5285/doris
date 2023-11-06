#pragma once

#include <ostream>

namespace selectdb {

enum class [[nodiscard]] TxnErrorCode : int {
    TXN_OK = 0,
    TXN_KEY_NOT_FOUND = 1,
    TXN_CONFLICT = -1,
    TXN_TOO_OLD = -2,
    TXN_MAYBE_COMMITTED = -3,
    TXN_RETRYABLE_NOT_COMMITTED = -4,
    TXN_TIMEOUT = -5,
    TXN_INVALID_ARGUMENT = -6,
    // other unidentified errors.
    TXN_UNIDENTIFIED_ERROR = -7,
};

inline std::ostream& operator<<(std::ostream& out, selectdb::TxnErrorCode code) {
    // clang-format off
    using selectdb::TxnErrorCode;
    switch (code) {
    case TxnErrorCode::TXN_OK: out << "Ok"; break;
    case TxnErrorCode::TXN_KEY_NOT_FOUND: out << "KeyNotFound"; break;
    case TxnErrorCode::TXN_CONFLICT: out << "Conflict"; break;
    case TxnErrorCode::TXN_TOO_OLD: out << "TxnTooOld"; break;
    case TxnErrorCode::TXN_MAYBE_COMMITTED: out << "MaybeCommitted"; break;
    case TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED: out << "RetryableNotCommitted"; break;
    case TxnErrorCode::TXN_TIMEOUT: out << "Timeout"; break;
    case TxnErrorCode::TXN_INVALID_ARGUMENT: out << "InvalidArgument"; break;
    case TxnErrorCode::TXN_UNIDENTIFIED_ERROR: out << "Unknown"; break;
    }
    return out;
    // clang-format on
}

} // namespace selectdb
