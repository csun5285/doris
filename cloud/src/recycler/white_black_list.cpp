#include "recycler/white_black_list.h"

namespace selectdb {

void WhiteBlackList::reset(const std::vector<std::string>& whitelist,
                           const std::vector<std::string>& blacklist) {
    blacklist_.clear();
    whitelist_.clear();
    if (!whitelist.empty()) {
        for (auto& str : whitelist) {
            whitelist_.insert(str);
        }
    } else {
        for (auto& str : blacklist) {
            blacklist_.insert(str);
        }
    }
}

bool WhiteBlackList::filter_out(const std::string& instance_id) const {
    if (whitelist_.empty()) {
        return blacklist_.count(instance_id);
    }
    return !whitelist_.count(instance_id);
}

} // namespace selectdb
