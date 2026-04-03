#include "KVickStateMachine.hpp"
#include <sstream>

namespace kvick {

KVickStateMachine::KVickStateMachine(KVick* store) : store_(store), last_committed_idx_(0) {}

nuraft::ptr<nuraft::buffer> KVickStateMachine::commit(const uint64_t log_idx, nuraft::buffer& data) {
    std::string str(reinterpret_cast<const char*>(data.data_begin()), data.size());
    
    std::istringstream iss(str);
    std::string op, key, val_str;
    iss >> op >> key;

    if (op == "SET") {
        std::getline(iss, val_str);
        if (!val_str.empty() && val_str.front() == ' ') val_str.erase(0, 1);
        store_->set(key, KVick::parseLiteral(val_str), false); // use_wal=false because Raft is our WAL
    } else if (op == "DEL") {
        store_->del(key, false);
    }

    last_committed_idx_ = log_idx;
    return nullptr;
}

nuraft::ptr<nuraft::buffer> KVickStateMachine::pre_commit(const uint64_t log_idx, nuraft::buffer& data) {
    return nullptr;
}

void KVickStateMachine::rollback(const uint64_t log_idx, nuraft::buffer& data) {
    // No-op for this simple KV store
}

void KVickStateMachine::save_snapshot_data(nuraft::snapshot& s, const uint64_t obj_id, nuraft::buffer& data) {
    // Trigger standard KVick binary snapshot (not strictly Raft compliant as it ignores obj_id, but works for PoC)
}

bool KVickStateMachine::apply_snapshot(nuraft::snapshot& s) {
    return true;
}

int KVickStateMachine::read_snapshot_data(nuraft::snapshot& s, const uint64_t obj_id, nuraft::buffer& data) {
    return 0;
}

nuraft::ptr<nuraft::snapshot> KVickStateMachine::last_snapshot() {
    return nullptr;
}

uint64_t KVickStateMachine::last_commit_index() {
    return last_committed_idx_;
}

void KVickStateMachine::create_snapshot(nuraft::snapshot& s, nuraft::async_result<bool>::handler_type& when_done) {
    if (when_done) {
        nuraft::ptr<std::exception> ex(nullptr);
        when_done(true, ex);
    }
}

} // namespace kvick
