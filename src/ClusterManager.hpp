#ifndef KVICK_CLUSTER_MANAGER_HPP
#define KVICK_CLUSTER_MANAGER_HPP

#include <string>
#include <vector>
#include <unordered_map>
#include <mutex>
#include <thread>
#include <atomic>
#include <map>
#include <memory>
#include <random>

#include <grpcpp/grpcpp.h>
#include "kvick_internal.grpc.pb.h"

namespace kvick {

class ClusterManager {
public:
    ClusterManager(const std::string& node_id, const std::string& address, const std::vector<std::string>& seed_nodes);
    ~ClusterManager();

    void start();
    void stop();

    // Gossip RPC Handlers
    grpc::Status Ping(grpc::ServerContext* context, const PingMessage* request, AckMessage* response);
    grpc::Status PingReq(grpc::ServerContext* context, const PingReqMessage* request, AckMessage* response);

    // Consistent Hashing
    std::string getOwnerNode(const std::string& key) const;
    std::vector<NodeInfo> getActiveNodes() const;
    std::string getNodeId() const { return node_id_; }
    std::string getAddress() const { return address_; }

private:
    void gossipLoop();
    void updateNode(const NodeInfo& info);
    void rebuildHashRing();
    std::shared_ptr<GossipService::Stub> getStub(const std::string& address);

    std::string node_id_;
    std::string address_;
    uint64_t incarnation_;
    std::atomic<bool> stop_flag_{false};

    std::unordered_map<std::string, NodeInfo> nodes_;
    mutable std::mutex nodes_mutex_;

    // Consistent Hashing Ring: hash(node_id + replica_id) -> node_id
    std::map<size_t, std::string> hash_ring_;
    mutable std::mutex ring_mutex_;
    static constexpr int VIRTUAL_NODES = 128;

    std::thread gossip_thread_;
    std::vector<std::string> seed_nodes_;
    
    std::unordered_map<std::string, std::shared_ptr<GossipService::Stub>> stubs_;
    std::mutex stubs_mutex_;

    std::mt19937 rng_;
};

} // namespace kvick

#endif
