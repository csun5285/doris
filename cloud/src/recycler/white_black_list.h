#pragma once

#include <set>
#include <string>
#include <vector>

namespace selectdb {

class WhiteBlackList {
public:
    void reset(const std::vector<std::string>& whitelist,
               const std::vector<std::string>& blacklist);
    bool filter_out(const std::string& name) const;

private:
    std::set<std::string> whitelist_;
    std::set<std::string> blacklist_;
};

} // namespace selectdb
