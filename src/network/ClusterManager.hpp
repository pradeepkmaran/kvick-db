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
#include <chrono>

#include <grpcpp/grpcpp.h>
#include "kvick_internal.grpc.pb.h"
#include "../utils/Hash.hpp"

namespace kvick {

class ClusterManager {
public:
    ClusterManager(const std::string& node_id, const std::string& address,
                   const std::vector<std::string>& seed_nodes);
    ~ClusterManager();

    void start();
    void stop();

    // Gossip RPC Handlers
    grpc::Status Ping(grpc::ServerContext* context, const PingMessage* request, AckMessage* response);
    grpc::Status PingReq(grpc::ServerContext* context, const PingReqMessage* request, AckMessage* response);

    // Consistent Hashing (used for read load balancing)
    std::string getOwnerNode(const std::string& key) const;
    std::vector<NodeInfo> getReplicaNodes(const std::string& key, int N = 3) const;
    std::vector<NodeInfo> getActiveNodes() const;
    std::string getNodeId() const { return node_id_; }
    std::string getAddress() const { return address_; }

    // Lookup a node's gRPC address by its node ID
    std::string getAddressByNodeId(const std::string& node_id) const;

private:
    void gossipLoop();
    void updateNode(const NodeInfo& info);
    void rebuildHashRing();
    void checkSuspectTimeouts();
    std::shared_ptr<GossipService::Stub> getStub(const std::string& address);

    // Select up to K random active nodes (excluding exclude_id)
    std::vector<NodeInfo> selectRandomNodes(size_t k, const std::string& exclude_id);

    std::string node_id_;
    std::string address_;
    uint64_t incarnation_;
    std::atomic<bool> stop_flag_{false};

    std::unordered_map<std::string, NodeInfo> nodes_;
    mutable std::mutex nodes_mutex_;

    // Suspect timers
    using time_point = std::chrono::steady_clock::time_point;
    std::unordered_map<std::string, time_point> suspect_timers_;
    static constexpr int SUSPECT_TIMEOUT_MS = 3000;
    static constexpr int PING_REQ_FANOUT = 3;
    static constexpr int MAX_PIGGYBACK_UPDATES = 10;

    // Consistent Hashing Ring
    std::map<uint32_t, std::string> hash_ring_;
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
