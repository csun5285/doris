#pragma once

#include <fmt/core.h>

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

inline const char* format_as(TxnErrorCode code) {
    // clang-format off
    switch (code) {
    case TxnErrorCode::TXN_OK: return "Ok";
    case TxnErrorCode::TXN_KEY_NOT_FOUND: return "KeyNotFound";
    case TxnErrorCode::TXN_CONFLICT: return "Conflict";
    case TxnErrorCode::TXN_TOO_OLD: return "TxnTooOld";
    case TxnErrorCode::TXN_MAYBE_COMMITTED: return "MaybeCommitted";
    case TxnErrorCode::TXN_RETRYABLE_NOT_COMMITTED: return "RetryableNotCommitted";
    case TxnErrorCode::TXN_TIMEOUT: return "Timeout";
    case TxnErrorCode::TXN_INVALID_ARGUMENT: return "InvalidArgument";
    case TxnErrorCode::TXN_UNIDENTIFIED_ERROR: return "Unknown";
    }
    return "NotImplemented";
    // clang-format on
}

inline std::ostream& operator<<(std::ostream& out, TxnErrorCode code) {
    out << format_as(code);
    return out;
}

} // namespace selectdb

template <>
struct fmt::formatter<selectdb::TxnErrorCode> {
    constexpr auto parse(format_parse_context& ctx) -> format_parse_context::iterator {
        return ctx.begin();
    }

    auto format(const selectdb::TxnErrorCode& code, format_context& ctx) const
            -> format_context::iterator {
        return fmt::format_to(ctx.out(), "{}", selectdb::format_as(code));
    }
};
