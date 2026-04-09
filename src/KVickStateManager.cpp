#include "KVickStateManager.hpp"
#include <iostream>

namespace kvick {

KVickStateManager::KVickStateManager(int32_t server_id, const std::string& endpoint,
                                     const std::string& data_dir)
    : server_id_(server_id), endpoint_(endpoint), data_dir_(data_dir),
      log_store_(nuraft::cs_new<nuraft::inmem_log_store>()) {}

nuraft::ptr<nuraft::cluster_config> KVickStateManager::load_config() {
    std::lock_guard<std::mutex> lock(config_mutex_);

    // Try to load from disk
    std::ifstream ifs(configPath(), std::ios::binary);
    if (ifs.is_open()) {
        ifs.seekg(0, std::ios::end);
        size_t sz = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        if (sz > 0) {
            auto buf = nuraft::buffer::alloc(sz);
            ifs.read(reinterpret_cast<char*>(buf->data_begin()), sz);
            saved_config_ = nuraft::cluster_config::deserialize(*buf);
            return saved_config_;
        }
    }

    // Default: single-node cluster with just this server
    saved_config_ = nuraft::cs_new<nuraft::cluster_config>();
    saved_config_->get_servers().push_back(
        nuraft::cs_new<nuraft::srv_config>(server_id_, endpoint_)
    );
    return saved_config_;
}

void KVickStateManager::save_config(const nuraft::cluster_config& config) {
    std::lock_guard<std::mutex> lock(config_mutex_);
    auto buf = config.serialize();
    std::ofstream ofs(configPath(), std::ios::binary | std::ios::trunc);
    if (ofs.is_open()) {
        ofs.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());
        ofs.flush();
    }
    saved_config_ = nuraft::cluster_config::deserialize(*buf);
}

void KVickStateManager::save_state(const nuraft::srv_state& state) {
    auto buf = state.serialize();
    std::ofstream ofs(statePath(), std::ios::binary | std::ios::trunc);
    if (ofs.is_open()) {
        ofs.write(reinterpret_cast<const char*>(buf->data_begin()), buf->size());
        ofs.flush();
    }
    saved_state_ = nuraft::srv_state::deserialize(*buf);
}

nuraft::ptr<nuraft::srv_state> KVickStateManager::read_state() {
    std::ifstream ifs(statePath(), std::ios::binary);
    if (ifs.is_open()) {
        ifs.seekg(0, std::ios::end);
        size_t sz = ifs.tellg();
        ifs.seekg(0, std::ios::beg);
        if (sz > 0) {
            auto buf = nuraft::buffer::alloc(sz);
            ifs.read(reinterpret_cast<char*>(buf->data_begin()), sz);
            saved_state_ = nuraft::srv_state::deserialize(*buf);
            return saved_state_;
        }
    }
    saved_state_ = nuraft::cs_new<nuraft::srv_state>();
    return saved_state_;
}

nuraft::ptr<nuraft::log_store> KVickStateManager::load_log_store() {
    return log_store_;
}

} // namespace kvick
