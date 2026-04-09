#ifndef KVICK_STATE_MANAGER_HPP
#define KVICK_STATE_MANAGER_HPP

#include <libnuraft/nuraft.hxx>
#include <libnuraft/in_memory_log_store.hxx>
#include <string>
#include <fstream>
#include <mutex>

namespace kvick {

class KVickStateManager : public nuraft::state_mgr {
public:
    KVickStateManager(int32_t server_id, const std::string& endpoint,
                      const std::string& data_dir);

    nuraft::ptr<nuraft::cluster_config> load_config() override;
    void save_config(const nuraft::cluster_config& config) override;
    void save_state(const nuraft::srv_state& state) override;
    nuraft::ptr<nuraft::srv_state> read_state() override;
    nuraft::ptr<nuraft::log_store> load_log_store() override;
    int32_t server_id() override { return server_id_; }
    void system_exit(const int exit_code) override {}

private:
    int32_t server_id_;
    std::string endpoint_;
    std::string data_dir_;
    nuraft::ptr<nuraft::inmem_log_store> log_store_;
    nuraft::ptr<nuraft::cluster_config> saved_config_;
    nuraft::ptr<nuraft::srv_state> saved_state_;
    std::mutex config_mutex_;

    std::string configPath() const { return data_dir_ + "/raft_config.bin"; }
    std::string statePath() const { return data_dir_ + "/raft_state.bin"; }
};

} // namespace kvick

#endif
