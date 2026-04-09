#ifndef KVICK_STATE_MACHINE_HPP
#define KVICK_STATE_MACHINE_HPP

#include <libnuraft/nuraft.hxx>
#include "KVick.hpp"
#include <mutex>

namespace kvick {

class KVickStateMachine : public nuraft::state_machine {
public:
    KVickStateMachine(KVick* store);
    ~KVickStateMachine() {}

    nuraft::ptr<nuraft::buffer> commit(const uint64_t log_idx, nuraft::buffer& data) override;
    nuraft::ptr<nuraft::buffer> pre_commit(const uint64_t log_idx, nuraft::buffer& data) override;
    void rollback(const uint64_t log_idx, nuraft::buffer& data) override;

    void save_logical_snp_obj(nuraft::snapshot& s, uint64_t& obj_id,
                              nuraft::buffer& data, bool is_first_obj, bool is_last_obj) override;
    bool apply_snapshot(nuraft::snapshot& s) override;
    int read_logical_snp_obj(nuraft::snapshot& s, void*& user_snp_ctx,
                             uint64_t obj_id, nuraft::ptr<nuraft::buffer>& data_out,
                             bool& is_last_obj) override;
    nuraft::ptr<nuraft::snapshot> last_snapshot() override;
    uint64_t last_commit_index() override;
    void create_snapshot(nuraft::snapshot& s,
                         nuraft::async_result<bool>::handler_type& when_done) override;

private:
    KVick* store_;
    std::atomic<uint64_t> last_committed_idx_{0};
    nuraft::ptr<nuraft::snapshot> last_snapshot_;
    std::mutex snapshot_mutex_;
};

} // namespace kvick

#endif
