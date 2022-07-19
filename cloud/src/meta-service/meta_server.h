// Container of meta service
#pragma once

#include <memory>

#include "brpc/server.h"

// namespace brpc {
// class Server;
// };

namespace selectdb {

class MetaServer {
public:
    MetaServer();
    ~MetaServer() = default;

    /**
     * Starts to listen and server
     *
     * return 0 for success otherwise failure
     */
    int start(int port);

    void join();

private:
    std::unique_ptr<brpc::Server> server_;
};

} // namespace selectdb
// vim: et ts=4 sw=4 cc=80:
