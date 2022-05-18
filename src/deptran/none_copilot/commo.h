#pragma once

#include "deptran/communicator.h"

namespace janus
{

class CommunicatorNoneCopilot : public Communicator {
public:
    uint32_t n_pending_rpc_[2] = {0,0};
    const uint32_t max_pending_rpc_ = 200;
    SharedIntEvent dispatch_quota{};

    CommunicatorNoneCopilot(PollMgr* poll_mgr = nullptr)
     :Communicator(poll_mgr) {
        dispatch_quota.value_ = 3 * max_pending_rpc_;
    }

    std::vector<SiteProxyPair>
    PilotProxyForPartition(parid_t par_id) const;

    void BroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> vec_piece_data,
                           Coordinator *coo,
                           const std::function<void(int res, TxnOutput &)> &callback) override;

};
    
} // namespace janus
