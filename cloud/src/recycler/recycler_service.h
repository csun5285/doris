#pragma once

#include <gen_cpp/selectdb_cloud.pb.h>

#include "meta-service/txn_kv.h"

namespace selectdb {

class Recycler;
class Checker;

class RecyclerServiceImpl : public selectdb::RecyclerService {
public:
    RecyclerServiceImpl(std::shared_ptr<TxnKv> txn_kv, Recycler* recycler, Checker* checker);
    ~RecyclerServiceImpl() override;

    void recycle_instance(::google::protobuf::RpcController* controller,
                          const ::selectdb::RecycleInstanceRequest* request,
                          ::selectdb::RecycleInstanceResponse* response,
                          ::google::protobuf::Closure* done) override;

    void http(::google::protobuf::RpcController* controller,
              const ::selectdb::MetaServiceHttpRequest* request,
              ::selectdb::MetaServiceHttpResponse* response,
              ::google::protobuf::Closure* done) override;

private:
    void check_instance(const std::string& instance_id, MetaServiceCode& code, std::string& msg);

private:
    std::shared_ptr<TxnKv> txn_kv_;
    Recycler* recycler_; // Ref
    Checker* checker_;   // Ref
};

} // namespace selectdb
