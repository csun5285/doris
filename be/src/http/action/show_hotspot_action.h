
#pragma once

#include "http/http_handler.h"

namespace doris {

class ShowHotspotAction final : public HttpHandler {
public:
    ShowHotspotAction() = default;

    ~ShowHotspotAction() override = default;

    void handle(HttpRequest* req) override;
};

} // namespace doris
