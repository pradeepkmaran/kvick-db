#ifndef KVICK_RAFT_MANAGER_HPP
#define KVICK_RAFT_MANAGER_HPP

#include <libnuraft/nuraft.hxx>
#include "../core/KVick.hpp"
#include "KVickStateMachine.hpp"
#include "KVickStateManager.hpp"
#include <string>
#include <mutex>

namespace kvick {

class RaftManager {
public:
    struct WriteResult {
        bool success;
        std::string message;
    };

    RaftManager(const std::string& node_id, int raft_port,
                const std::string& advertise_endpoint,
                KVick* store, const std::string& data_dir);
    ~RaftManager();

    void start(bool is_seed);
    void stop();

    // Propose a write command ("SET key value" or "DEL key").
    // Blocks until committed or timeout (3 seconds).
    WriteResult proposeWrite(const std::string& command);

    bool isLeader() const;
    int32_t getLeaderId() const;
    int32_t getServerId() const { return server_id_; }

    // Add a new server to the Raft cluster (called on leader only)
    bool addServer(int32_t server_id, const std::string& endpoint);

private:
    std::string node_id_;
    int raft_port_;
    int32_t server_id_;
    std::string data_dir_;

    nuraft::raft_launcher launcher_;
    nuraft::ptr<nuraft::raft_server> raft_instance_;
    nuraft::ptr<KVickStateMachine> state_machine_;
    nuraft::ptr<KVickStateManager> state_manager_;
};

} // namespace kvick

#endif
