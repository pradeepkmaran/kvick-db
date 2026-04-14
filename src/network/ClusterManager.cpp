#include "ClusterManager.hpp"
#include <iostream>
#include <chrono>
#include <functional>
#include <algorithm>

namespace kvick {

ClusterManager::ClusterManager(const std::string& node_id, const std::string& address,
                               const std::vector<std::string>& seed_nodes)
    : node_id_(node_id), address_(address), incarnation_(0),
      seed_nodes_(seed_nodes), rng_(std::random_device{}()) {

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
        std::cout << "[SWIM] Discovered new node: " << info.node_id()
                  << " at " << info.address() << std::endl;
    } else {
        if (info.incarnation() > it->second.incarnation()) {
            auto old_state = it->second.state();
            it->second = info;

            if (info.state() == NodeInfo::ALIVE && old_state != NodeInfo::ALIVE) {
                ring_needs_rebuild = true;
                suspect_timers_.erase(info.node_id());
            }
            if (info.state() == NodeInfo::DEAD || info.state() == NodeInfo::LEFT) {
                ring_needs_rebuild = true;
                suspect_timers_.erase(info.node_id());
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
                uint32_t hash = hash::hash_string(vnode_key);
                hash_ring_[hash] = id;
            }
        }
    }
}

std::string ClusterManager::getOwnerNode(const std::string& key) const {
    std::lock_guard<std::mutex> lock(ring_mutex_);
    if (hash_ring_.empty()) return node_id_;

    uint32_t hash = hash::hash_string(key);
    auto it = hash_ring_.lower_bound(hash);

    if (it == hash_ring_.end()) {
        it = hash_ring_.begin();
    }
    return it->second;
}

std::vector<NodeInfo> ClusterManager::getReplicaNodes(const std::string& key, int N) const {
    std::lock_guard<std::mutex> ring_lock(ring_mutex_);
    std::lock_guard<std::mutex> nodes_lock(nodes_mutex_);
    
    if (hash_ring_.empty()) return {};

    std::vector<NodeInfo> replicas;
    uint32_t hash = hash::hash_string(key);
    auto it = hash_ring_.lower_bound(hash);

    size_t visited_vnodes = 0;
    while (replicas.size() < (size_t)N && visited_vnodes < hash_ring_.size()) {
        if (it == hash_ring_.end()) {
            it = hash_ring_.begin();
        }

        const std::string& node_id = it->second;
        bool already_added = false;
        for (const auto& r : replicas) {
            if (r.node_id() == node_id) {
                already_added = true;
                break;
            }
        }

        if (!already_added) {
            auto node_it = nodes_.find(node_id);
            if (node_it != nodes_.end() && 
                (node_it->second.state() == NodeInfo::ALIVE || node_it->second.state() == NodeInfo::SUSPECT)) {
                replicas.push_back(node_it->second);
            }
        }
        
        ++it;
        visited_vnodes++;
    }

    return replicas;
}

std::string ClusterManager::getAddressByNodeId(const std::string& node_id) const {
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    auto it = nodes_.find(node_id);
    if (it != nodes_.end()) return it->second.address();
    return "";
}

#include <netdb.h>
#include <arpa/inet.h>

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

std::vector<NodeInfo> ClusterManager::selectRandomNodes(size_t k, const std::string& exclude_id) {
    std::vector<NodeInfo> candidates;
    {
        std::lock_guard<std::mutex> lock(nodes_mutex_);
        for (const auto& [id, info] : nodes_) {
            if (id != node_id_ && id != exclude_id &&
                info.state() != NodeInfo::DEAD && info.state() != NodeInfo::LEFT) {
                candidates.push_back(info);
            }
        }
    }
    std::shuffle(candidates.begin(), candidates.end(), rng_);
    if (candidates.size() > k) candidates.resize(k);
    return candidates;
}

void ClusterManager::checkSuspectTimeouts() {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    std::vector<std::string> to_remove;

    for (auto& [id, deadline] : suspect_timers_) {
        if (now >= deadline) {
            auto it = nodes_.find(id);
            if (it != nodes_.end() && it->second.state() == NodeInfo::SUSPECT) {
                std::cout << "[SWIM] Node " << id << " suspect timeout — marking DEAD" << std::endl;
                it->second.set_incarnation(it->second.incarnation() + 1);
                it->second.set_state(NodeInfo::DEAD);
                to_remove.push_back(id);
            }
        }
    }
    for (const auto& id : to_remove) {
        suspect_timers_.erase(id);
    }
    if (!to_remove.empty()) {
        rebuildHashRing();
    }
}

void ClusterManager::gossipLoop() {
    // Initial join: Ping seeds with retries
    bool joined = false;
    for (int attempt = 0; attempt < 10 && !joined && !stop_flag_; ++attempt) {
        for (const auto& seed : seed_nodes_) {
            if (seed == address_) continue;

            PingMessage req;
            req.set_sender_id(node_id_);
            req.set_sender_address(address_);

            // Piggyback self info so seed learns about us
            NodeInfo self_info;
            {
                std::lock_guard<std::mutex> lock(nodes_mutex_);
                self_info = nodes_[node_id_];
            }
            *req.add_piggybacked_updates() = self_info;

            AckMessage res;
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
            // context.set_wait_for_ready(true); // Wait for channel to connect instead of fast-failing during backoff

            auto stub = getStub(seed);
            grpc::Status status = stub->Ping(&context, req, &res);
            if (status.ok()) {
                for (const auto& update : res.piggybacked_updates()) {
                    updateNode(update);
                }
                std::cout << "[SWIM] Joined cluster via seed " << seed << std::endl;
                joined = true;
                break;
            } else {
                std::cout << "[SWIM] Seed ping to " << seed << " failed (attempt "
                          << attempt + 1 << "): " << status.error_message() 
                          << " (code " << status.error_code() << ")" << std::endl;
            }
        }
        if (!joined) {
            std::this_thread::sleep_for(std::chrono::milliseconds(500));
        }
    }

    // Periodic gossip loop
    while (!stop_flag_) {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));

        checkSuspectTimeouts();

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

        // Bounded piggybacking — send at most MAX_PIGGYBACK_UPDATES nodes
        {
            std::lock_guard<std::mutex> lock(nodes_mutex_);
            int count = 0;
            // Always include self
            *req.add_piggybacked_updates() = nodes_[node_id_];
            count++;
            for (const auto& [id, info] : nodes_) {
                if (id == node_id_) continue;
                if (count >= MAX_PIGGYBACK_UPDATES) break;
                *req.add_piggybacked_updates() = info;
                count++;
            }
        }

        AckMessage res;
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

        auto stub = getStub(target.address());
        grpc::Status status = stub->Ping(&context, req, &res);

        if (status.ok()) {
            for (const auto& update : res.piggybacked_updates()) {
                updateNode(update);
            }
        } else {
            // Failed to ping — SWIM Suspect phase
            std::string target_id = target.node_id();

            // Check if already suspect
            {
                std::lock_guard<std::mutex> lock(nodes_mutex_);
                auto it = nodes_.find(target_id);
                if (it != nodes_.end() && it->second.state() == NodeInfo::ALIVE) {
                    std::cout << "[SWIM] Direct ping to " << target_id
                              << " failed — trying indirect PingReq" << std::endl;
                }
            }

            // Indirect probe via PingReq to K random peers
            auto proxies = selectRandomNodes(PING_REQ_FANOUT, target_id);
            bool probed_alive = false;

            for (auto& proxy : proxies) {
                PingReqMessage ping_req;
                ping_req.set_sender_id(node_id_);
                ping_req.set_target_id(target_id);
                ping_req.set_target_address(target.address());

                AckMessage ping_req_ack;
                grpc::ClientContext ctx;
                ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(500));

                auto proxy_stub = getStub(proxy.address());
                grpc::Status prs = proxy_stub->PingReq(&ctx, ping_req, &ping_req_ack);
                if (prs.ok()) {
                    probed_alive = true;
                    for (const auto& update : ping_req_ack.piggybacked_updates()) {
                        updateNode(update);
                    }
                    break;
                }
            }

            if (!probed_alive) {
                // Mark as SUSPECT (not DEAD) and start timer
                std::lock_guard<std::mutex> lock(nodes_mutex_);
                auto it = nodes_.find(target_id);
                if (it != nodes_.end() && it->second.state() == NodeInfo::ALIVE) {
                    it->second.set_incarnation(it->second.incarnation() + 1);
                    it->second.set_state(NodeInfo::SUSPECT);
                    suspect_timers_[target_id] = std::chrono::steady_clock::now() +
                        std::chrono::milliseconds(SUSPECT_TIMEOUT_MS);
                    std::cout << "[SWIM] Node " << target_id << " marked SUSPECT" << std::endl;
                }
            }
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

    // Bounded piggybacking
    std::lock_guard<std::mutex> lock(nodes_mutex_);
    int count = 0;
    for (const auto& [id, info] : nodes_) {
        if (count >= MAX_PIGGYBACK_UPDATES) break;
        *response->add_piggybacked_updates() = info;
        count++;
    }

    return grpc::Status::OK;
}

grpc::Status ClusterManager::PingReq(grpc::ServerContext* context, const PingReqMessage* request, AckMessage* response) {
    // Indirect probe: ping the target on behalf of the requester
    PingMessage probe;
    probe.set_sender_id(node_id_);
    probe.set_sender_address(address_);
    for (const auto& update : request->piggybacked_updates()) {
        *probe.add_piggybacked_updates() = update;
    }

    AckMessage probe_ack;
    grpc::ClientContext ctx;
    ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(1000));

    auto stub = getStub(request->target_address());
    grpc::Status status = stub->Ping(&ctx, probe, &probe_ack);

    if (status.ok()) {
        response->set_sender_id(node_id_);
        for (const auto& update : probe_ack.piggybacked_updates()) {
            *response->add_piggybacked_updates() = update;
            updateNode(update);
        }
        return grpc::Status::OK;
    }

    return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Target unreachable via indirect probe");
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
