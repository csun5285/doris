#pragma once

namespace selectdb {

enum [[nodiscard]] TxnErrorCode : int {
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

} // namespace selectdb

