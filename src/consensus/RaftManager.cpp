#include "RaftManager.hpp"
#include "../utils/Hash.hpp"
#include <iostream>
#include <chrono>

namespace kvick {

// Simple console logger for NuRaft
class KVickRaftLogger : public nuraft::logger {
public:
    KVickRaftLogger() {}

    void set_level(int l) override { level_ = l; }
    int get_level() override { return level_; }

    void put_details(int level, const char* source_file, const char* func_name,
                     size_t line_number, const std::string& msg) override {
        if (level <= level_) {
            std::cout << "[Raft] " << msg << std::endl;
        }
    }

private:
    int level_ = 4; // Warning level
};

RaftManager::RaftManager(const std::string& node_id, int raft_port,
                         const std::string& advertise_endpoint,
                         KVick* store, const std::string& data_dir)
    : node_id_(node_id), raft_port_(raft_port), data_dir_(data_dir) {

    server_id_ = hash::node_id_to_server_id(node_id);

    state_machine_ = nuraft::cs_new<KVickStateMachine>(store);

    // Use advertise_endpoint for config so peers can reach us
    // (NuRaft still binds on 0.0.0.0:raft_port via the launcher)
    state_manager_ = nuraft::cs_new<KVickStateManager>(server_id_, advertise_endpoint, data_dir);
}

RaftManager::~RaftManager() {
    stop();
}

void RaftManager::start(bool is_seed) {
    nuraft::raft_params params;
    params.heart_beat_interval_ = 500;
    params.election_timeout_lower_bound_ = 1000;
    params.election_timeout_upper_bound_ = 2000;
    params.reserved_log_items_ = 5;
    params.snapshot_distance_ = 5000;
    params.client_req_timeout_ = 3000;
    params.return_method_ = nuraft::raft_params::blocking;

    nuraft::asio_service::options asio_opt;
    asio_opt.thread_pool_size_ = 4;

    auto logger = nuraft::cs_new<KVickRaftLogger>();

    nuraft::raft_server::init_options opt;
    // If not a seed node, wait for the actual leader to contact us instead of forming an isolated size-1 cluster.
    if (!is_seed) {
        opt.skip_initial_election_timeout_ = true;
    }

    raft_instance_ = launcher_.init(
        state_machine_,
        state_manager_,
        logger,
        raft_port_,
        asio_opt,
        params,
        opt
    );

    if (!raft_instance_) {
        throw std::runtime_error("Failed to initialize NuRaft");
    }

    std::cout << "[Raft] Server " << server_id_ << " (" << node_id_
              << ") started on port " << raft_port_ << std::endl;

    // Wait a bit to allow leader election
    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    if (isLeader()) {
        std::cout << "[Raft] This node is the leader" << std::endl;
    }
}

void RaftManager::stop() {
    if (raft_instance_) {
        launcher_.shutdown(5);
        raft_instance_ = nullptr;
        std::cout << "[Raft] Server shut down" << std::endl;
    }
}

RaftManager::WriteResult RaftManager::proposeWrite(const std::string& command) {
    if (!raft_instance_) {
        return {false, "Raft not initialized"};
    }

    if (!isLeader()) {
        return {false, "Not the leader"};
    }

    // Serialize command into a buffer
    auto buf = nuraft::buffer::alloc(command.size());
    buf->put_raw(reinterpret_cast<const nuraft::byte*>(command.data()), command.size());
    buf->pos(0);

    // Append to Raft log (blocking mode — waits for commit)
    auto result = raft_instance_->append_entries({buf});

    if (!result->get_accepted()) {
        return {false, "Request not accepted — possibly not the leader"};
    }

    auto commit_result = result->get();
    if (commit_result) {
        std::string msg(reinterpret_cast<const char*>(commit_result->data_begin()),
                        commit_result->size());
        // Clean up trailing nulls if any (for backward compatibility)
        while (!msg.empty() && msg.back() == '\0') msg.pop_back();
        
        bool ok = (msg == "OK");
        return {ok, msg};
    }

    return {true, "OK"};
}

bool RaftManager::isLeader() const {
    if (!raft_instance_) return false;
    return raft_instance_->is_leader();
}

int32_t RaftManager::getLeaderId() const {
    if (!raft_instance_) return -1;
    return raft_instance_->get_leader();
}

bool RaftManager::addServer(int32_t new_server_id, const std::string& endpoint) {
    if (!raft_instance_ || !isLeader()) {
        std::cerr << "[Raft] Cannot add server — not the leader" << std::endl;
        return false;
    }

    nuraft::srv_config new_srv(new_server_id, endpoint);
    auto result = raft_instance_->add_srv(new_srv);

    if (!result->get_accepted()) {
        std::cerr << "[Raft] add_srv not accepted" << std::endl;
        return false;
    }

    result->get(); // Wait for completion
    std::cout << "[Raft] Added server " << new_server_id
              << " at " << endpoint << std::endl;
    return true;
}

} // namespace kvick
