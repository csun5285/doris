// clang-format off

#include "common/arg_parser.h"
#include "common/config.h"
#include "common/encryption_util.h"
#include "common/logging.h"
#include "gen_cpp/selectdb_version.h"
#include "meta-service/mem_txn_kv.h"
#include "meta-service/meta_server.h"
#include "meta-service/txn_kv.h"
#include "recycler/recycler.h"

#include <brpc/server.h>
#include <unistd.h> // ::lockf
#include <fcntl.h> // ::open
#include <functional>
#include <iostream>
#include <filesystem>
#include <fstream>
#include <mutex>
#include <sstream>
// clang-format on

/**
 * Generates a pidfile with given process name
 *
 * @return an fd holder with auto-storage-lifecycle
 */
std::shared_ptr<int> gen_pidfile(const std::string& process_name) {
    std::cerr << "process working directory: " << std::filesystem::current_path() << std::endl;
    std::string pid_path = "./bin/" + process_name + ".pid";
    int fd = ::open(pid_path.c_str(), O_RDWR | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);
    // clang-format off
    std::shared_ptr<int> holder(&fd, // Just pretend to need an address of int
            [fd, pid_path](...) {    // The real fd is captured
                if (fd <= 0) { return; }
                [[maybe_unused]] auto x = ::lockf(fd, F_UNLCK, 0);
                ::close(fd);
                // FIXME: removing the pidfile may result in missing pidfile
                //        after launching the process...
                // std::error_code ec; std::filesystem::remove(pid_path, ec);
            });
    // clang-format on
    if (::lockf(fd, F_TLOCK, 0) != 0) {
        std::cerr << "failed to lock pidfile=" << pid_path << " fd=" << fd << std::endl;
        return nullptr;
    }
    std::fstream pidfile(pid_path, std::ios::out);
    if (!pidfile.is_open()) {
        std::cerr << "failed to open pid file " << pid_path << std::endl;
        return nullptr;
    }
    pidfile << getpid() << std::endl;
    pidfile.close();
    std::cout << "pid=" << getpid() << " written to file=" << pid_path << std::endl;
    return holder;
}

/**
 * Prepares extra conf files
 */
std::string prepare_extra_conf_file() {
    std::fstream fdb_cluter_file(selectdb::config::fdb_cluster_file_path, std::ios::out);
    fdb_cluter_file << "# DO NOT EDIT UNLESS YOU KNOW WHAT YOU ARE DOING!\n"
                    << "# This file is auto-generated with selectdb_cloud.conf:fdb_cluster.\n"
                    << "# It is not to be edited by hand.\n"
                    << selectdb::config::fdb_cluster;
    fdb_cluter_file.close();
    return "";
}

// TODO(gavin): support daemon mode
// must be called before pidfile generation and any network resource
// initializaiton, <https://man7.org/linux/man-pages/man3/daemon.3.html>
// void daemonize(1, 1); // Maybe nohup will do?

// Arguments
// clang-format off
constexpr static const char* ARG_META_SERVICE = "meta-service";
constexpr static const char* ARG_RECYCLER     = "recycler";
constexpr static const char* ARG_HELP         = "help";
constexpr static const char* ARG_VERSION      = "version";
constexpr static const char* ARG_CONF         = "conf";
selectdb::ArgParser args(
  {
    selectdb::ArgParser::new_arg<bool>(ARG_META_SERVICE, false, "run as meta-service"),
    selectdb::ArgParser::new_arg<bool>(ARG_RECYCLER, false, "run as recycler")    ,
    selectdb::ArgParser::new_arg<bool>(ARG_HELP, false, "print help msg")     ,
    selectdb::ArgParser::new_arg<bool>(ARG_VERSION, false, "print version info") ,
    selectdb::ArgParser::new_arg<std::string>(ARG_CONF, "./conf/selectdb_cloud.conf", "path to conf file")  ,
  }
);
// clang-format on

static void help() {
    args.print();
}

static std::string build_info() {
    std::stringstream ss;
#if defined(NDEBUG)
    ss << "version:{" SELECTDB_BUILD_VERSION "-release}"
#else
    ss << "version:{" SELECTDB_BUILD_VERSION "-debug}"
#endif
       << " code_version:{commit=" SELECTDB_BUILD_HASH " time=" SELECTDB_BUILD_VERSION_TIME "}"
       << " build_info:{initiator=" SELECTDB_BUILD_INITIATOR " build_at=" SELECTDB_BUILD_TIME
          " build_on=" SELECTDB_BUILD_OS_VERSION "}\n";
    return ss.str();
}

namespace brpc {
DECLARE_uint64(max_body_size);
DECLARE_int64(socket_max_unwritten_bytes);
} // namespace brpc

int main(int argc, char** argv) {
    if (argc > 1) {
        if (auto ret = args.parse(argc - 1, argv + 1); !ret.empty()) {
            std::cerr << ret << std::endl;
            help();
            return -1;
        }
    }

    if (argc < 2 || args.get<bool>(ARG_HELP)) {
        help();
        return 0;
    }

    if (args.get<bool>(ARG_VERSION)) {
        std::cout << build_info();
        return 0;
    }

    // FIXME(gavin): do we need to enable running both MS and recycler within
    //               single process
    if (!(args.get<bool>(ARG_META_SERVICE) ^ args.get<bool>(ARG_RECYCLER))) {
        std::cerr << "only one of --meta-service and --recycler must be specified" << std::endl;
        return 1;
    }

    // There may be more roles to play
    std::string process_name = args.get<bool>(ARG_META_SERVICE) ? "meta_service"
                               : args.get<bool>(ARG_RECYCLER)   ? "recycler"
                                                                : "";
    if (process_name.empty()) {
        std::cerr << "failed to determine prcess name with given args" << std::endl;
        return 1;
    }

    auto pid_file_fd_holder = gen_pidfile("selectdb_cloud");
    if (pid_file_fd_holder == nullptr) {
        return -1;
    }

    auto conf_file = args.get<std::string>(ARG_CONF);
    if (!selectdb::config::init(conf_file.c_str(), true)) {
        std::cerr << "failed to init config file, conf=" << conf_file << std::endl;
        return -1;
    }

    if (auto ret = prepare_extra_conf_file(); !ret.empty()) {
        std::cerr << "failed to prepare extra conf file, err=" << ret << std::endl;
        return -1;
    }

    if (!selectdb::init_glog(process_name.data())) {
        std::cerr << "failed to init glog" << std::endl;
        return -1;
    }

    // We can invoke glog from now on

    std::string msg;
    LOG(INFO) << build_info();
    std::cout << build_info() << std::endl;

    using namespace selectdb;
    brpc::Server server;
    brpc::FLAGS_max_body_size = config::brpc_max_body_size;
    brpc::FLAGS_socket_max_unwritten_bytes = config::brpc_socket_max_unwritten_bytes;

    std::shared_ptr<TxnKv> txn_kv;
    if (config::use_mem_kv) {
        // MUST NOT be used in production environment
        std::cerr << "use volatile mem kv, please make sure it is not a production environment"
                  << std::endl;
        txn_kv = std::make_shared<MemTxnKv>();
    } else {
        txn_kv = std::make_shared<FdbTxnKv>();
    }
    if (txn_kv == nullptr) {
        LOG(WARNING) << "failed to create txn kv, invalid txnkv type";
        return 1;
    }
    LOG(INFO) << "begin to init txn kv";
    int ret = txn_kv->init();
    if (ret != 0) {
        LOG(WARNING) << "failed to init txnkv, ret=" << ret;
        return 1;
    }
    LOG(INFO) << "successfully init txn kv";

    if (init_global_encryption_key_info_map(txn_kv) != 0) {
        LOG(WARNING) << "failed to init global encryption key map";
        return -1;
    }

    std::unique_ptr<MetaServer> meta_server;
    std::unique_ptr<Recycler> recycler;

    if (args.get<bool>(ARG_META_SERVICE)) {
        meta_server = std::make_unique<MetaServer>(txn_kv);
        int ret = meta_server->start(&server);
        if (ret != 0) {
            msg = "failed to start meta server";
            LOG(ERROR) << msg;
            std::cerr << msg << std::endl;
            return ret;
        }
        msg = "meta-service started";
        LOG(INFO) << msg;
        std::cout << msg << std::endl;
    } else if (args.get<bool>(ARG_RECYCLER)) {
        recycler = std::make_unique<Recycler>(txn_kv);
        int ret = recycler->start(&server);
        if (ret != 0) {
            msg = "failed to start recycler";
            LOG(ERROR) << msg;
            std::cerr << msg << std::endl;
            return ret;
        }
        msg = "recycler started";
        LOG(INFO) << msg;
        std::cout << msg << std::endl;

    } else {
        std::cerr << "selectdb starts without doing anything and exits" << std::endl;
        return -1;
    }
    // start service
    brpc::ServerOptions options;
    if (config::brpc_num_threads != -1) {
        options.num_threads = config::brpc_num_threads;
    }
    int port = config::brpc_listen_port;
    if (server.Start(port, &options) != 0) {
        char buf[64];
        LOG(WARNING) << "failed to start brpc, errno=" << errno
                     << ", errmsg=" << strerror_r(errno, buf, 64) << ", port=" << port;
        return -1;
    }
    LOG(INFO) << "succesfully started brpc listening on port=" << port;

    server.RunUntilAskedToQuit(); // Wait for signals
    server.ClearServices();
    if (meta_server) {
        meta_server->stop();
    }
    if (recycler) {
        recycler->stop();
    }

    return 0;
}
// vim: et ts=4 sw=4 cc=80:
