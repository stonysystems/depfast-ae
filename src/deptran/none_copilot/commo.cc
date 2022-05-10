#include "commo.h"

namespace janus
{

std::vector<SiteProxyPair>
CommunicatorNoneCopilot::PilotProxyForPartition(parid_t par_id) const {
  /**
   * ad-hoc. No leader election. fixed pilot(id=0) and copilot(id=1)
   */
  auto it  = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  auto config = Config::GetConfig();
  auto pilot_it =
      std::find_if(partition_proxies.begin(), partition_proxies.end(),
                   [config](const std::pair<siteid_t, ClassicProxy*>& p) {
                     verify(p.second != nullptr);
                     auto& site = config->SiteById(p.first);
                     return site.locale_id == 0;
                   });
  if (pilot_it == partition_proxies.end())
    Log_fatal("couldn't find pilot for partition %d", par_id);
  verify(pilot_it->second);

  auto copilot_it =
      std::find_if(partition_proxies.begin(), partition_proxies.end(),
                   [config](const std::pair<siteid_t, ClassicProxy*>& p) {
                     verify(p.second != nullptr);
                     auto& site = config->SiteById(p.first);
                     return site.locale_id == 1;
                   });
  if (copilot_it == partition_proxies.end())
    Log_fatal("couldn't find copilot for partition %d", par_id);
  verify(copilot_it->second);

  return { *pilot_it, *copilot_it };  
}

void CommunicatorNoneCopilot::BroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> sp_vec_piece,
                                                Coordinator *coo,
                                                const std::function<void(int res, TxnOutput &)> &callback) {
  cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
  verify(!sp_vec_piece->empty());
  auto par_id = sp_vec_piece->at(0)->PartitionId();
  rrr::FutureAttr fuattr;
  fuattr.callback = [coo, this, callback](Future *fu) {
    int32_t ret;
    TxnOutput outputs;
    fu->get_reply() >> ret >> outputs;
    n_pending_rpc_--;
    verify(n_pending_rpc_ >= 0);
    callback(ret, outputs);
  };
  // auto pair_leader_proxy = LeaderProxyForPartition(par_id);
  // Log_debug("send dispatch to site %ld",
  //           pair_leader_proxy.first);
  // auto proxy = pair_leader_proxy.second;
  auto pair_proxies = PilotProxyForPartition(par_id);
  verify(pair_proxies.size() == 2);
  Log_debug("send dispatch to site %d, %d", pair_proxies[0].first,
            pair_proxies[1].first);
  shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
  sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
  MarshallDeputy md(sp_vpd);

  struct DepId di;
  di.id = cmd_id;
  di.str = __func__;

  if (n_pending_rpc_ < max_pending_rpc_) {
    // if (true) {
    auto future =
        pair_proxies[0].second->async_Dispatch(cmd_id, di, md, fuattr);
    Future::safe_release(future);
    n_pending_rpc_++;
  }

  rrr::FutureAttr fu2;
  fu2.callback = [coo, this, callback](Future *fu) {
    int32_t ret;
    TxnOutput outputs;
    fu->get_reply() >> ret >> outputs;
    callback(ret, outputs);
  };
  Future::safe_release(
      pair_proxies[1].second->async_Dispatch(cmd_id, di, md, fu2));
}
    
} // namespace janus
