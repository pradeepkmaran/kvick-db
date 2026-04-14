#include "KVickStateMachine.hpp"
#include <sstream>
#include <iostream>

namespace kvick {

KVickStateMachine::KVickStateMachine(KVick* store)
    : store_(store), last_committed_idx_(0) {}

nuraft::ptr<nuraft::buffer> KVickStateMachine::commit(const uint64_t log_idx, nuraft::buffer& data) {
    std::string str(reinterpret_cast<const char*>(data.data_begin()), data.size());
    // Robustly handle optional null terminators in the log
    while (!str.empty() && str.back() == '\0') str.pop_back();

    std::istringstream iss(str);
    std::string op, key, val_str;
    iss >> op >> key;

    nuraft::ptr<nuraft::buffer> result = nullptr;

    if (op == "SET") {
        std::getline(iss, val_str);
        if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0, 1);
        store_->set(key, KVick::parseLiteral(val_str));
        // Return "OK" in result buffer
        std::string ok = "OK";
        result = nuraft::buffer::alloc(ok.size());
        result->put_raw(reinterpret_cast<const nuraft::byte*>(ok.data()), ok.size());
    } else if (op == "DEL") {
        bool deleted = store_->del(key);
        std::string msg = deleted ? "OK" : "ERR Not Found";
        result = nuraft::buffer::alloc(msg.size());
        result->put_raw(reinterpret_cast<const nuraft::byte*>(msg.data()), msg.size());
    }

    last_committed_idx_ = log_idx;
    return result;
}

nuraft::ptr<nuraft::buffer> KVickStateMachine::pre_commit(const uint64_t log_idx, nuraft::buffer& data) {
    return nullptr;
}

void KVickStateMachine::rollback(const uint64_t log_idx, nuraft::buffer& data) {
    // No-op — rollbacks are rare and the store will eventually converge
}

void KVickStateMachine::save_logical_snp_obj(nuraft::snapshot& s, uint64_t& obj_id,
                                              nuraft::buffer& data, bool is_first_obj, bool is_last_obj) {
    // Store the snapshot data (single object model)
    // The snapshot was created in create_snapshot and serialized in read_logical_snp_obj
}

bool KVickStateMachine::apply_snapshot(nuraft::snapshot& s) {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(
        s.get_last_log_idx(), s.get_last_log_term(),
        s.get_last_config());
    last_committed_idx_ = s.get_last_log_idx();
    return true;
}

int KVickStateMachine::read_logical_snp_obj(nuraft::snapshot& s, void*& user_snp_ctx,
                                             uint64_t obj_id, nuraft::ptr<nuraft::buffer>& data_out,
                                             bool& is_last_obj) {
    // For simplicity: single snapshot object (obj_id == 0)
    is_last_obj = true;

    // Serialize current store state to buffer
    // Use an in-memory stringstream as temporary storage
    std::ostringstream oss(std::ios::binary);
    auto all_keys = store_->keys();
    uint64_t count = all_keys.size();
    oss.write(reinterpret_cast<const char*>(&count), sizeof(count));

    for (const auto& key : all_keys) {
        try {
            auto val_ptr = store_->get(key);
            // Write key
            uint32_t klen = static_cast<uint32_t>(key.size());
            oss.write(reinterpret_cast<const char*>(&klen), sizeof(klen));
            oss.write(key.data(), klen);
            // Write serialized value
            std::string serialized = std::visit([](const auto& v) -> std::string {
                return JSONSerializer::serialize(v);
            }, *val_ptr);
            uint32_t vlen = static_cast<uint32_t>(serialized.size());
            oss.write(reinterpret_cast<const char*>(&vlen), sizeof(vlen));
            oss.write(serialized.data(), vlen);
        } catch (...) {
            // Key might have been deleted concurrently
        }
    }

    std::string data = oss.str();
    data_out = nuraft::buffer::alloc(data.size());
    data_out->put_raw(reinterpret_cast<const nuraft::byte*>(data.data()), data.size());
    data_out->pos(0);

    return 0;
}

nuraft::ptr<nuraft::snapshot> KVickStateMachine::last_snapshot() {
    std::lock_guard<std::mutex> lock(snapshot_mutex_);
    return last_snapshot_;
}

uint64_t KVickStateMachine::last_commit_index() {
    return last_committed_idx_;
}

void KVickStateMachine::create_snapshot(nuraft::snapshot& s,
                                         nuraft::async_result<bool>::handler_type& when_done) {
    {
        std::lock_guard<std::mutex> lock(snapshot_mutex_);
        last_snapshot_ = nuraft::cs_new<nuraft::snapshot>(
            s.get_last_log_idx(), s.get_last_log_term(),
            s.get_last_config());
    }
    if (when_done) {
        nuraft::ptr<std::exception> ex(nullptr);
        bool result = true;
        when_done(result, ex);
    }
}

} // namespace kvick
