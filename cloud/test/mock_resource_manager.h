#pragma once

#include "meta-service/txn_kv.h"
#include "resource-manager/resource_manager.h"

using namespace selectdb;

std::string mock_instance = "test_instance";
std::string mock_cluster_name = "test_cluster";
std::string mock_cluster_id = "test_cluster_id";

class MockResourceManager : public ResourceManager {
public:
    MockResourceManager(std::shared_ptr<TxnKv> txn_kv) : ResourceManager(txn_kv) {};
    ~MockResourceManager() override = default;

    int init() override { return 0; }

    std::string get_node(const std::string& cloud_unique_id,
                         std::vector<NodeInfo>* nodes) override {
        NodeInfo i {Role::COMPUTE_NODE, mock_instance, mock_cluster_name, mock_cluster_id};
        nodes->push_back(i);
        return "";
    }

    std::pair<MetaServiceCode, std::string> add_cluster(const std::string& instance_id,
                                                        const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> drop_cluster(const std::string& instance_id,
                                                         const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> refresh_instance(const std::string& instance_id) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::string update_cluster(
            const std::string& instance_id, const ClusterInfo& cluster,
            std::function<bool(const ::selectdb::ClusterPB&)> filter,
            std::function<std::string(::selectdb::ClusterPB&, std::set<std::string>& cluster_names)>
                    action) override {
        return "";
    }

    std::pair<int, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                             const std::string& instance_id,
                                             InstanceInfoPB* inst_pb) override {
        return {1, ""};
    }

    std::string modify_nodes(const std::string& instance_id, const std::vector<NodeInfo>& to_add,
                             const std::vector<NodeInfo>& to_del) override {
        return "";
    }
};