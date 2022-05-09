#pragma once

#include "deptran/communicator.h"

namespace janus
{

class CommunicatorNoneCopilot : public Communicator {
public:
    using Communicator::Communicator;

    std::vector<SiteProxyPair>
    PilotProxyForPartition(parid_t par_id) const;

    void BroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> vec_piece_data,
                           Coordinator *coo,
                           const std::function<void(int res, TxnOutput &)> &callback) override;

};
    
} // namespace janus
