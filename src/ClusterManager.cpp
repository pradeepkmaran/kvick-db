#include "ClusterManager.hpp"
#include <iostream>
#include <chrono>
#include <functional>

namespace kvick {

ClusterManager::ClusterManager(const std::string& node_id, const std::string& address, const std::vector<std::string>& seed_nodes)
    : node_id_(node_id), address_(address), incarnation_(0), seed_nodes_(seed_nodes), rng_(std::random_device{}()) {
    
    NodeInfo self_info;
    self_info.set_node_id(node_id_);
    self_info.set_address(address_);
    self_info.set_incarnation(incarnation_);
    self_info.set_state(NodeInfo::ALIVE);
    
    nodes_[node_id_] = self_info;
    rebuildHashRing();
}

ClusterManager::~ClusterManager() {
    stop();
}

void ClusterManager::start() {
    gossip_thread_ = std::thread(&ClusterManager::gossipLoop, this);
}

void ClusterManager::stop() {
    stop_flag_ = true;
    if (gossip_thread_.joinable()) {
        gossip_thread_.join();
    }
}

void ClusterManager::updateNode(const NodeInfo& info) {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto it = nodes_.find(info.node_id());
    
    bool ring_needs_rebuild = false;

    if (it == nodes_.end()) {
        nodes_[info.node_id()] = info;
        ring_needs_rebuild = true;
    } else {
        if (info.incarnation() > it->second.incarnation()) {
            it->second = info;
            if (info.state() == NodeInfo::DEAD || info.state() == NodeInfo::LEFT) {
                ring_needs_rebuild = true;
            }
        }
    }

    if (ring_needs_rebuild) {
        rebuildHashRing();
    }
}

void ClusterManager::rebuildHashRing() {
    std::lock_guard<std::mutex> lock(ring_mutex_);
    hash_ring_.clear();

    for (const auto& [id, info] : nodes_) {
        if (info.state() == NodeInfo::ALIVE || info.state() == NodeInfo::SUSPECT) {
            for (int i = 0; i < VIRTUAL_NODES; ++i) {
                std::string vnode_key = id + "#" + std::to_string(i);
                size_t hash = std::hash<std::string>{}(vnode_key);
                hash_ring_[hash] = id;
            }
        }
    }
}

std::string ClusterManager::getOwnerNode(const std::string& key) const {
    std::lock_guard<std::mutex> lock(ring_mutex_);
    if (hash_ring_.empty()) return node_id_; // Fallback to self

    size_t hash = std::hash<std::string>{}(key);
    auto it = hash_ring_.lower_bound(hash);
    
    if (it == hash_ring_.end()) {
        it = hash_ring_.begin(); // Wrap around
    }
    return it->second;
}

std::shared_ptr<GossipService::Stub> ClusterManager::getStub(const std::string& address) {
    std::lock_guard<std::mutex> lock(stubs_mutex_);
    auto it = stubs_.find(address);
    if (it == stubs_.end()) {
        auto channel = grpc::CreateChannel(address, grpc::InsecureChannelCredentials());
        auto stub = GossipService::NewStub(channel);
        stubs_[address] = std::move(stub);
        return stubs_[address];
    }
    return it->second;
}

void ClusterManager::gossipLoop() {
    // Initial join: Ping seeds
    for (const auto& seed : seed_nodes_) {
        if (seed == address_) continue;
        
        PingMessage req;
        req.set_sender_id(node_id_);
        req.set_sender_address(address_);
        
        AckMessage res;
        grpc::ClientContext context;
        // set 1 sec timeout
        auto deadline = std::chrono::system_clock::now() + std::chrono::seconds(1);
        context.set_deadline(deadline);
        
        auto stub = getStub(seed);
        grpc::Status status = stub->Ping(&context, req, &res);
        if (status.ok()) {
            for (const auto& update : res.piggybacked_updates()) {
                updateNode(update);
            }
        }
    }

    // Periodic gossip loop
    while (!stop_flag_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        std::vector<NodeInfo> active_nodes;
        {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            for (const auto& [id, info] : nodes_) {
                if (id != node_id_ && info.state() != NodeInfo::DEAD && info.state() != NodeInfo::LEFT) {
                    active_nodes.push_back(info);
                }
            }
        }

        if (active_nodes.empty()) continue;

        // Select a random node to ping
        std::uniform_int_distribution<size_t> dist(0, active_nodes.size() - 1);
        NodeInfo target = active_nodes[dist(rng_)];

        PingMessage req;
        req.set_sender_id(node_id_);
        req.set_sender_address(address_);

        // Piggyback updates (for simplicity, piggyback all alive nodes; in reality, send diffs)
        for (const auto& n : active_nodes) {
            *req.add_piggybacked_updates() = n;
        }

        AckMessage res;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(300));
        
        auto stub = getStub(target.address());
        grpc::Status status = stub->Ping(&context, req, &res);

        if (status.ok()) {
            for (const auto& update : res.piggybacked_updates()) {
                updateNode(update);
            }
        } else {
            // Failed to ping. Mark as suspect and attempt PingReq via a proxy (omitted for brevity, just marking dead)
            target.set_incarnation(target.incarnation() + 1);
            target.set_state(NodeInfo::DEAD); // SWIM usually goes to Suspect first
            updateNode(target);
        }
    }
}

grpc::Status ClusterManager::Ping(grpc::ServerContext* context, const PingMessage* request, AckMessage* response) {
    for (const auto& update : request->piggybacked_updates()) {
        updateNode(update);
    }
    
    // Add sender to nodes if new
    NodeInfo sender_info;
    sender_info.set_node_id(request->sender_id());
    sender_info.set_address(request->sender_address());
    sender_info.set_incarnation(1);
    sender_info.set_state(NodeInfo::ALIVE);
    updateNode(sender_info);

    response->set_sender_id(node_id_);
    
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    for (const auto& [id, info] : nodes_) {
        *response->add_piggybacked_updates() = info;
    }

    return grpc::Status::OK;
}

grpc::Status ClusterManager::PingReq(grpc::ServerContext* context, const PingReqMessage* request, AckMessage* response) {
    return grpc::Status(grpc::StatusCode::UNIMPLEMENTED, "PingReq not implemented yet");
}

std::vector<NodeInfo> ClusterManager::getActiveNodes() const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    std::vector<NodeInfo> active;
    for (const auto& [id, info] : nodes_) {
        if (info.state() == NodeInfo::ALIVE || info.state() == NodeInfo::SUSPECT) {
            active.push_back(info);
        }
    }
    return active;
}

} // namespace kvick
