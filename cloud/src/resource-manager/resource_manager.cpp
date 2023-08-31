
// clang-format off
#include "resource_manager.h"
#include "meta-service/keys.h"
#include "common/logging.h"
#include "common/sync_point.h"
#include "common/util.h"

#include <sstream>
// clang-format on

namespace selectdb {

static std::atomic_int64_t seq = 0;

int ResourceManager::init() {
    // Scan all instances
    std::unique_ptr<Transaction> txn;
    int ret = txn_kv_->create_txn(&txn);
    if (ret != 0) {
        LOG(INFO) << "failed to init txn, ret=" << ret;
        return -1;
    }

    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<RangeGetIterator> it;

    int num_instances = 0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log_range(
            (int*)0x01, [key0, key1, &num_instances](int*) {
                LOG(INFO) << "get instances, num_instances=" << num_instances << " range=["
                          << hex(key0) << "," << hex(key1) << "]";
            });

    //                     instance_id  instance
    std::vector<std::tuple<std::string, InstanceInfoPB>> instances;
    do {
        ret = txn->get(key0, key1, &it);
        if (ret != 0) {
            LOG(WARNING) << "internal error, failed to get instance, ret=" << ret;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            LOG(INFO) << "range get instance_key=" << hex(k);
            instances.emplace_back("", InstanceInfoPB {});
            auto& [instance_id, inst] = instances.back();

            if (!inst.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance, unable to deserialize, key=" << hex(k);
                return -1;
            }
            // 0x01 "instance" ${instance_id} -> InstanceInfoPB
            k.remove_prefix(1); // Remove key space
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            ret = decode_key(&k, &out);
            if (ret != 0) {
                LOG(WARNING) << "failed to decode key, ret=" << ret;
                return -2;
            }
            if (out.size() != 2) {
                LOG(WARNING) << "decoded size no match, expect 2, given=" << out.size();
            }
            instance_id = std::get<std::string>(std::get<0>(out[1]));

            LOG(INFO) << "get an instance, instance_id=" << instance_id
                      << " instance json=" << proto_to_json(inst);

            ++num_instances;
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    for (auto& [inst_id, inst] : instances) {
        for (auto& c : inst.clusters()) {
            add_cluster_to_index(inst_id, c);
        }
    }

    return 0;
}

std::string ResourceManager::get_node(const std::string& cloud_unique_id,
                                      std::vector<NodeInfo>* nodes) {
    // FIXME(gavin): refresh the all instance if there is a miss?
    //               Or we can refresh all instances regularly to reduce
    //               read amplification.
    std::shared_lock l(mtx_);
    auto [s, e] = node_info_.equal_range(cloud_unique_id);
    if (s == node_info_.end() || s->first != cloud_unique_id) {
        LOG(INFO) << "cloud unique id not found cloud_unique_id=" << cloud_unique_id;
        return "cloud_unique_id not found";
    }
    nodes->reserve(nodes->size() + node_info_.count(cloud_unique_id));
    for (auto i = s; i != e; ++i) {
        nodes->emplace_back(i->second); // Just copy, it's cheap
    }
    return "";
}

bool ResourceManager::check_cluster_params_valid(const ClusterPB& cluster, std::string* err,
                                                 bool check_master_num) {
    // check
    if (!cluster.has_type()) {
        *err = "cluster must have type arg";
        return false;
    }

    std::stringstream ss;
    bool no_err = true;
    int master_num = 0;
    for (auto& n : cluster.nodes()) {
        if (ClusterPB::SQL == cluster.type() && n.has_edit_log_port() && n.edit_log_port() &&
            n.has_node_type() &&
            (n.node_type() == NodeInfoPB_NodeType_FE_MASTER ||
             n.node_type() == NodeInfoPB_NodeType_FE_OBSERVER)) {
            master_num += n.node_type() == NodeInfoPB_NodeType_FE_MASTER ? 1 : 0;
            continue;
        }
        if (ClusterPB::COMPUTE == cluster.type() && n.has_heartbeat_port() && n.heartbeat_port()) {
            continue;
        }
        ss << "check cluster params failed, node : " << proto_to_json(n);
        *err = ss.str();
        no_err = false;
        break;
    }
    // ATTN: add_cluster check must have only a master node
    // add_node doesn't check it
    if (check_master_num && ClusterPB::SQL == cluster.type() && master_num != 1) {
        no_err = false;
        ss << "cluster is SQL type, must have only one master node, now master count: "
           << master_num;
        *err = ss.str();
    }
    return no_err;
}

std::pair<MetaServiceCode, std::string> ResourceManager::add_cluster(const std::string& instance_id,
                                                                     const ClusterInfo& cluster) {
    std::string err;
    std::stringstream ss;

    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&err](int*) { LOG(INFO) << "add_cluster err=" << err; });

    if (!check_cluster_params_valid(cluster.cluster, &err, true)) {
        LOG(WARNING) << err;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, err);
    }

    // FIXME(gavin): ensure atomicity of the entire process of adding cluster.
    //               Inserting a placeholer to node_info_ before persistence
    //               and updating it after persistence for the cloud_unique_id
    //               to add is a fairly fine solution.
    {
        std::shared_lock l(mtx_);
        // Check uniqueness of cloud_unique_id to add, cloud unique ids are not
        // shared between instances
        for (auto& i : cluster.cluster.nodes()) {
            // check cloud_unique_id in the same instance
            auto [start, end] = node_info_.equal_range(i.cloud_unique_id());
            if (start == node_info_.end() || start->first != i.cloud_unique_id()) continue;
            for (auto it = start; it != end; ++it) {
                if (it->second.instance_id != instance_id) {
                    // different instance, but has same cloud_unique_id
                    ss << "cloud_unique_id is already occupied by an instance,"
                       << " instance_id=" << it->second.instance_id
                       << " cluster_name=" << it->second.cluster_name
                       << " cluster_id=" << it->second.cluster_id
                       << " cloud_unique_id=" << it->first;
                    err = ss.str();
                    LOG(INFO) << err;
                    return std::make_pair(MetaServiceCode::ALREADY_EXISTED, err);
                }
            }
        }
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_CREATE_ERR, err);
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = "failed to get instance, info " + m0;
        LOG(WARNING) << err << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_GET_ERR, err);
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        err = "instance status has been set delete, plz check it";
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, err);
    }

    LOG(INFO) << "cluster to add json=" << proto_to_json(cluster.cluster);
    LOG(INFO) << "json=" << proto_to_json(instance);

    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        if (i.cluster_id() == cluster.cluster.cluster_id()) {
            ss << "try to add a existing cluster id,"
               << " existing_cluster_id=" << i.cluster_id();
            err = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, err);
        }

        if (i.cluster_name() == cluster.cluster.cluster_name()) {
            ss << "try to add a existing cluster name,"
               << " existing_cluster_name=" << i.cluster_name();
            err = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, err);
        }
    }

    // TODO(gavin): Check duplicated nodes, one node cannot deploy on multiple clusters
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    for (auto& n : cluster.cluster.nodes()) {
        auto& node = const_cast<std::decay_t<decltype(n)>&>(n);
        node.set_ctime(time);
        node.set_mtime(time);
    }

    auto to_add_cluster = instance.add_clusters();
    to_add_cluster->CopyFrom(cluster.cluster);
    // create compute cluster, set it status normal as default value
    if (cluster.cluster.type() == ClusterPB::COMPUTE) {
        to_add_cluster->set_cluster_status(ClusterStatus::NORMAL);
    }
    LOG(INFO) << "instance " << instance_id << " has " << instance.clusters().size() << " clusters";

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, err);
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_COMMIT_ERR, err);
    }

    add_cluster_to_index(instance_id, cluster.cluster);

    return std::make_pair(MetaServiceCode::OK, "");
}

std::pair<MetaServiceCode, std::string> ResourceManager::drop_cluster(
        const std::string& instance_id, const ClusterInfo& cluster) {
    std::stringstream ss;
    std::string err;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    if (cluster_id.empty()) {
        ss << "missing cluster_id=" << cluster_id;
        err = ss.str();
        LOG(INFO) << err;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, err);
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_CREATE_ERR, err);
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = m0;
        LOG(WARNING) << err;
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, err);
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        err = "instance status has been set delete, plz check it";
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, err);
    }

    bool found = false;
    int idx = -1;
    ClusterPB to_del;
    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        ++idx;
        if (i.cluster_id() == cluster.cluster.cluster_id()) {
            to_del.CopyFrom(i);
            LOG(INFO) << "found a cluster to drop,"
                      << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                      << " cluster_name=" << i.cluster_name() << " cluster=" << proto_to_json(i);
            found = true;
            break;
        }
    }

    if (!found) {
        ss << "failed to find cluster to drop,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name();
        err = ss.str();
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, err);
    }

    auto& clusters = const_cast<std::decay_t<decltype(instance.clusters())>&>(instance.clusters());
    clusters.DeleteSubrange(idx, 1); // Remove it

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        LOG(WARNING) << err;
        return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, err);
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return std::make_pair(MetaServiceCode::KV_TXN_COMMIT_ERR, err);
    }

    remove_cluster_from_index(instance_id, to_del);

    return std::make_pair(MetaServiceCode::OK, "");
}

std::string ResourceManager::update_cluster(
        const std::string& instance_id, const ClusterInfo& cluster,
        std::function<bool(const ::selectdb::ClusterPB&)> filter,
        std::function<std::string(::selectdb::ClusterPB&, std::set<std::string>& cluster_names)>
                action) {
    std::stringstream ss;
    std::string err;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    std::string cluster_name =
            cluster.cluster.has_cluster_name() ? cluster.cluster.cluster_name() : "";
    if (cluster_id.empty()) {
        ss << "missing cluster_id=" << cluster_id;
        err = ss.str();
        LOG(INFO) << err;
        return err;
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        err = m0;
        return err;
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        err = "instance status has been set delete, plz check it";
        return err;
    }

    std::set<std::string> cluster_names;
    // collect cluster_names
    for (auto& i : instance.clusters()) {
        cluster_names.emplace(i.cluster_name());
    }

    bool found = false;
    int idx = -1;
    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        ++idx;
        if (filter(i)) {
            LOG(INFO) << "found a cluster to update,"
                      << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                      << " cluster_name=" << i.cluster_name() << " cluster=" << proto_to_json(i);
            found = true;
            break;
        }
    }

    if (!found) {
        ss << "failed to find cluster to update,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name();
        err = ss.str();
        return err;
    }

    auto& clusters = const_cast<std::decay_t<decltype(instance.clusters())>&>(instance.clusters());

    // do update
    ClusterPB original = clusters[idx];
    err = action(clusters[idx], cluster_names);
    if (!err.empty()) {
        return err;
    }
    ClusterPB now = clusters[idx];
    LOG(INFO) << "before update cluster original: " << proto_to_json(original)
              << " after update now: " << proto_to_json(now);

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return err;
    }

    txn->put(key, val);
    LOG(INFO) << "put instnace_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    LOG(INFO) << "update cluster instance_id=" << instance_id
              << " instance json=" << proto_to_json(instance);

    update_cluster_to_index(instance_id, original, now);

    return err;
}

void ResourceManager::update_cluster_to_index(const std::string& instance_id,
                                              const ClusterPB& original, const ClusterPB& now) {
    std::lock_guard l(mtx_);
    remove_cluster_from_index_no_lock(instance_id, original);
    add_cluster_to_index_no_lock(instance_id, now);
}

void ResourceManager::add_cluster_to_index_no_lock(const std::string& instance_id,
                                                   const ClusterPB& c) {
    auto type = c.has_type() ? c.type() : -1;
    Role role = (type == ClusterPB::SQL
                         ? Role::SQL_SERVER
                         : (type == ClusterPB::COMPUTE ? Role::COMPUTE_NODE : Role::UNDEFINED));
    LOG(INFO) << "add cluster to index, instance_id=" << instance_id << " cluster_type=" << type
              << " cluster_name=" << c.cluster_name() << " cluster_id=" << c.cluster_id();

    for (auto& i : c.nodes()) {
        bool existed = node_info_.count(i.cloud_unique_id());
        NodeInfo n {.role = role,
                    .instance_id = instance_id,
                    .cluster_name = c.cluster_name(),
                    .cluster_id = c.cluster_id(),
                    .node_info = i};
        LOG(WARNING) << (existed ? "duplicated cloud_unique_id " : "")
                     << "instance_id=" << instance_id << " cloud_unique_id=" << i.cloud_unique_id()
                     << " node_info=" << proto_to_json(i);
        node_info_.insert({i.cloud_unique_id(), std::move(n)});
    }
}

void ResourceManager::add_cluster_to_index(const std::string& instance_id, const ClusterPB& c) {
    std::lock_guard l(mtx_);
    add_cluster_to_index_no_lock(instance_id, c);
}

void ResourceManager::remove_cluster_from_index_no_lock(const std::string& instance_id,
                                                        const ClusterPB& c) {
    std::string cluster_name = c.cluster_name();
    std::string cluster_id = c.cluster_id();
    int cnt = 0;
    for (auto it = node_info_.begin(); it != node_info_.end();) {
        auto& [_, n] = *it;
        if (n.instance_id != instance_id || n.cluster_id != cluster_id ||
            n.cluster_name != cluster_name) {
            ++it;
            continue;
        }
        ++cnt;
        LOG(INFO) << "remove node from index, instance_id=" << instance_id
                  << " role=" << static_cast<int>(n.role) << " cluster_name=" << n.cluster_name
                  << " cluster_id=" << n.cluster_id << " node_info=" << proto_to_json(n.node_info);
        it = node_info_.erase(it);
    }
    LOG(INFO) << cnt << " nodes removed from index, cluster_id=" << cluster_id
              << " cluster_name=" << cluster_name << " instance_id=" << instance_id;
}

void ResourceManager::remove_cluster_from_index(const std::string& instance_id,
                                                const ClusterPB& c) {
    std::lock_guard l(mtx_);
    remove_cluster_from_index_no_lock(instance_id, c);
}

std::pair<int, std::string> ResourceManager::get_instance(std::shared_ptr<Transaction> txn,
                                                          const std::string& instance_id,
                                                          InstanceInfoPB* inst_pb) {
    std::pair<int, std::string> ec {0, ""};
    [[maybe_unused]] auto& [code, msg] = ec;
    std::stringstream ss;

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    int ret = 0;
    if (txn == nullptr) {
        std::unique_ptr<Transaction> txn0;
        ret = txn_kv_->create_txn(&txn0);
        if (ret != 0) {
            code = -1;
            msg = "failed to create txn";
            LOG(WARNING) << msg << " ret=" << ret;
            return ec;
        }
        txn.reset(txn0.release());
    }

    ret = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (ret != 0) {
        code = -2;
        ss << "failed to get instance, instance_id=" << instance_id << " ret=" << ret;
        msg = ss.str();
        return ec;
    }

    if (!inst_pb->ParseFromString(val)) {
        code = -3;
        msg = "failed to parse InstanceInfoPB";
        return ec;
    }

    return ec;
}

std::string ResourceManager::modify_nodes(const std::string& instance_id,
                                          const std::vector<NodeInfo>& to_add,
                                          const std::vector<NodeInfo>& to_del) {
    std::string err;
    std::stringstream ss;
    std::unique_ptr<int, std::function<void(int*)>> defer(
            (int*)0x01, [&err](int*) { LOG(INFO) << "modify_nodes err=" << err; });

    if ((to_add.size() && to_del.size()) || (!to_add.size() && !to_del.size())) {
        err = "to_add and to_del both empty or both not empty";
        LOG(WARNING) << err;
        return err;
    }

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        err = "failed to create txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    {
        TEST_SYNC_POINT_CALLBACK("modify_nodes:get_instance", &c0);
        TEST_SYNC_POINT_CALLBACK("modify_nodes:get_instance_ret", &instance);
    }
    if (c0 != 0) {
        err = m0;
        return err;
    }

    if (instance.status() != InstanceInfoPB::NORMAL) {
        err = "instance status has been set delete, plz check it";
        LOG(WARNING) << err;
        return err;
    }

    LOG(INFO) << "instance json=" << proto_to_json(instance);
    std::vector<std::pair<ClusterPB, ClusterPB>> vec;
    using modify_impl_func = std::function<std::string(const ClusterPB& c, const NodeInfo& n)>;
    using check_func = std::function<std::string(const NodeInfo& n)>;
    auto modify_func = [&](const NodeInfo& node, check_func check,
                           modify_impl_func action) -> std::string {
        std::string cluster_id = node.cluster_id;
        std::string cluster_name = node.cluster_name;

        {
            std::shared_lock l(mtx_);
            err = check(node);
            if (err != "") {
                return err;
            }
        }

        LOG(INFO) << "node to modify json=" << proto_to_json(node.node_info);

        for (auto& c : instance.clusters()) {
            if ((c.has_cluster_name() && c.cluster_name() == cluster_name) ||
                (c.has_cluster_id() && c.cluster_id() == cluster_id)) {
                err = action(c, node);
                if (err != "") {
                    return err;
                }
            }
        }
        return "";
    };

    check_func check_to_add = [&](const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        auto [start, end] = node_info_.equal_range(n.node_info.cloud_unique_id());
        if (start == node_info_.end() || start->first != n.node_info.cloud_unique_id()) {
            return "";
        }
        for (auto it = start; it != end; ++it) {
            if (it->second.instance_id != n.instance_id) {
                // different instance, but has same cloud_unique_id
                s << "cloud_unique_id is already occupied by an instance,"
                  << " instance_id=" << it->second.instance_id
                  << " cluster_name=" << it->second.cluster_name
                  << " cluster_id=" << it->second.cluster_id
                  << " cloud_unique_id=" << n.node_info.cloud_unique_id();
                err = s.str();
                LOG(INFO) << err;
                return err;
            }
        }
        return "";
    };

    modify_impl_func modify_to_add = [&](const ClusterPB& c, const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        ClusterPB copied_original_cluster;
        ClusterPB copied_cluster;
        bool is_compute_node = n.node_info.has_heartbeat_port();
        for (auto it = c.nodes().begin(); it != c.nodes().end(); ++it) {
            if (it->has_ip() && n.node_info.has_ip()) {
                std::string c_endpoint = it->ip() + ":" +
                                         (is_compute_node ? std::to_string(it->heartbeat_port())
                                                          : std::to_string(it->edit_log_port()));
                std::string n_endpoint =
                    n.node_info.ip() + ":" +
                    (is_compute_node ? std::to_string(n.node_info.heartbeat_port())
                                     : std::to_string(n.node_info.edit_log_port()));
                if (c_endpoint == n_endpoint) {
                    // replicate request, do nothing
                    return "";
                }
            }

            if (it->has_host() && n.node_info.has_host()) {
                std::string c_endpoint_host = it->host() + ":" +
                                              (is_compute_node ? std::to_string(it->heartbeat_port())
                                                               : std::to_string(it->edit_log_port()));
                std::string n_endpoint_host =
                    n.node_info.host() + ":" +
                    (is_compute_node ? std::to_string(n.node_info.heartbeat_port())
                                     : std::to_string(n.node_info.edit_log_port()));
                if (c_endpoint_host == n_endpoint_host) {
                    // replicate request, do nothing
                    return "";
                }
            }
        }

        // add ctime and mtime
        auto& node = const_cast<std::decay_t<decltype(n.node_info)>&>(n.node_info);
        auto now_time = std::chrono::system_clock::now();
        uint64_t time =
                std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch())
                        .count();
        if (!node.has_ctime()) {
            node.set_ctime(time);
        }
        node.set_mtime(time);
        copied_original_cluster.CopyFrom(c);
        auto& change_cluster = const_cast<std::decay_t<decltype(c)>&>(c);
        change_cluster.add_nodes()->CopyFrom(node);
        copied_cluster.CopyFrom(change_cluster);
        vec.emplace_back(std::move(copied_original_cluster), std::move(copied_cluster));
        return "";
    };

    for (auto& it : to_add) {
        err = modify_func(it, check_to_add, modify_to_add);
        if (err != "") {
            LOG(WARNING) << err;
            return err;
        }
    }

    check_func check_to_del = [&](const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        auto [start, end] = node_info_.equal_range(n.node_info.cloud_unique_id());
        if (start == node_info_.end() || start->first != n.node_info.cloud_unique_id()) {
            s << "cloud_unique_id can not find to drop node,"
              << " instance_id=" << n.instance_id << " cluster_name=" << n.cluster_name
              << " cluster_id=" << n.cluster_id
              << " cloud_unique_id=" << n.node_info.cloud_unique_id();
            err = s.str();
            LOG(WARNING) << err;
            return err;
        }

        bool found = false;
        for (auto it = start; it != end; ++it) {
            const auto& m_node = it->second.node_info;
            if (m_node.has_ip() && n.node_info.has_ip()) {
                std::string m_endpoint =
                    m_node.ip() + ":" +
                    (m_node.has_heartbeat_port() ? std::to_string(m_node.heartbeat_port())
                                                 : std::to_string(m_node.edit_log_port()));

                std::string n_endpoint = n.node_info.ip() + ":" +
                                         (n.node_info.has_heartbeat_port()
                                          ? std::to_string(n.node_info.heartbeat_port())
                                          : std::to_string(n.node_info.edit_log_port()));

                if (m_endpoint == n_endpoint) {
                    found = true;
                    break;
                }
            }

            if (m_node.has_host() && n.node_info.has_host()) {
                std::string m_endpoint_host =
                    m_node.host() + ":" +
                    (m_node.has_heartbeat_port() ? std::to_string(m_node.heartbeat_port())
                                                 : std::to_string(m_node.edit_log_port()));

                std::string n_endpoint_host = n.node_info.host() + ":" +
                                              (n.node_info.has_heartbeat_port()
                                               ? std::to_string(n.node_info.heartbeat_port())
                                               : std::to_string(n.node_info.edit_log_port()));

                if (m_endpoint_host == n_endpoint_host) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            s << "cloud_unique_id can not find to drop node,"
              << " instance_id=" << n.instance_id << " cluster_name=" << n.cluster_name
              << " cluster_id=" << n.cluster_id
              << " cloud_unique_id=" << n.node_info.cloud_unique_id();
            err = s.str();
            LOG(WARNING) << err;
            return err;
        }
        return "";
    };

    modify_impl_func modify_to_del = [&](const ClusterPB& c, const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        ClusterPB copied_original_cluster;
        ClusterPB copied_cluster;

        bool found = false;
        int idx = -1;
        const auto& ni = n.node_info;
        for (auto& cn : c.nodes()) {
            idx++;
            if (cn.has_ip() && ni.has_ip()) {
                std::string cn_endpoint =
                    cn.ip() + ":" +
                    (cn.has_heartbeat_port() ? std::to_string(cn.heartbeat_port())
                                             : std::to_string(cn.edit_log_port()));

                std::string ni_endpoint =
                    ni.ip() + ":" +
                    (ni.has_heartbeat_port() ? std::to_string(ni.heartbeat_port())
                                             : std::to_string(ni.edit_log_port()));

                if (ni.cloud_unique_id() == cn.cloud_unique_id() && cn_endpoint == ni_endpoint) {
                    found = true;
                    break;
                }
            }

            if (cn.has_host() && ni.has_host()) {
                std::string cn_endpoint_host =
                    cn.host() + ":" +
                    (cn.has_heartbeat_port() ? std::to_string(cn.heartbeat_port())
                                             : std::to_string(cn.edit_log_port()));

                std::string ni_endpoint_host =
                    ni.host() + ":" +
                    (ni.has_heartbeat_port() ? std::to_string(ni.heartbeat_port())
                                             : std::to_string(ni.edit_log_port()));

                if (ni.cloud_unique_id() == cn.cloud_unique_id() && cn_endpoint_host == ni_endpoint_host) {
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            s << "failed to find node to drop,"
              << " instance_id=" << instance.instance_id() << " cluster_id=" << c.cluster_id()
              << " cluster_name=" << c.cluster_name() << " cluster=" << proto_to_json(c);
            err = s.str();
            LOG(WARNING) << err;
            // not found return ok.
            return "";
        }
        copied_original_cluster.CopyFrom(c);
        auto& change_nodes = const_cast<std::decay_t<decltype(c.nodes())>&>(c.nodes());
        change_nodes.DeleteSubrange(idx, 1); // Remove it
        copied_cluster.CopyFrom(c);
        vec.emplace_back(std::move(copied_original_cluster), std::move(copied_cluster));
        return "";
    };

    for (auto& it : to_del) {
        err = modify_func(it, check_to_del, modify_to_del);
        if (err != "") {
            LOG(WARNING) << err;
            // not found, just return OK to cloud control
            return "";
        }
    }

    LOG(INFO) << "instance " << instance_id << " info: " << instance.DebugString();

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        err = "failed to serialize";
        return err;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_key=" << hex(key);
    ret = txn->commit();
    if (ret != 0) {
        err = "failed to commit kv txn";
        LOG(WARNING) << err << " ret=" << ret;
        return err;
    }

    for (auto& it : vec) {
        update_cluster_to_index(instance_id, it.first, it.second);
    }

    return "";
}

std::pair<MetaServiceCode, std::string> ResourceManager::refresh_instance(
        const std::string& instance_id) {
    LOG(INFO) << "begin to refresh instance, instance_id=" << instance_id << " seq=" << ++seq;
    std::pair<MetaServiceCode, std::string> ret0 {MetaServiceCode::OK, "OK"};
    auto& [code, msg] = ret0;
    std::unique_ptr<int, std::function<void(int*)>> defer_log(
            (int*)0x01, [&ret0, &instance_id](int*) {
                LOG(INFO) << (std::get<0>(ret0) == MetaServiceCode::OK ? "succ to " : "failed to ")
                          << "refresh_instance, instance_id=" << instance_id
                          << " code=" << std::get<0>(ret0) << " msg=" << std::get<1>(ret0);
            });

    std::unique_ptr<Transaction> txn0;
    int ret = txn_kv_->create_txn(&txn0);
    if (ret != 0) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " ret=" << ret;
        return ret0;
    }
    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != 0) {
        code = MetaServiceCode::KV_TXN_GET_ERR;
        msg = m0;
        return ret0;
    }
    std::vector<ClusterInfo> clusters;
    clusters.reserve(instance.clusters_size());

    std::lock_guard l(mtx_);
    for (auto i = node_info_.begin(); i != node_info_.end();) {
        if (i->second.instance_id != instance_id) {
            ++i;
            continue;
        }
        i = node_info_.erase(i);
    }
    for (int i = 0; i < instance.clusters_size(); ++i) {
        add_cluster_to_index_no_lock(instance_id, instance.clusters(i));
    }
    LOG(INFO) << "finish refreshing instance, instance_id=" << instance_id << " seq=" << seq;
    return ret0;
}

} // namespace selectdb
// vim: et tw=100 ts=4 sw=4 cc=80:
