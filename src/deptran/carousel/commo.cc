
#include "command.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "commo.h"
#include "coordinator.h"
#include "../rcc_rpc.h"
#include "deptran/service.h"

namespace janus {

CarouselCommo::CarouselCommo(PollMgr* poll_mgr):
  Communicator(poll_mgr),
  using_basic_(Config::GetConfig()->carousel_basic_mode()) {}

void CarouselCommo::BroadcastReadAndPrepare(
    shared_ptr<vector<shared_ptr<TxPieceData>>> sp_vec_piece,
    Coordinator* coo,
    const function<void(bool,int, TxnOutput&)> & callback) {
  cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
  verify(!sp_vec_piece->empty());
  auto par_id = sp_vec_piece->at(0)->PartitionId();
  auto pair_leader_proxy = LeaderProxyForPartition(par_id);
  Log_debug("send dispatch to site %ld",
            pair_leader_proxy.first);
  auto proxy = pair_leader_proxy.second;
  shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
  sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
  MarshallDeputy md(sp_vpd); // ????

  // TODO: yidawu check the diff from inner id and par id
  auto super_maj = ceil((rpc_par_proxies_[par_id].size() - 1)  * 3 /4) + 1;
  ((CoordinatorCarousel*)coo)->n_prepare_super_maj_[par_id] = super_maj;
  auto maj = ceil((rpc_par_proxies_[par_id].size() - 1) /2) + 1;
  ((CoordinatorCarousel*)coo)->n_prepare_no_maj_[par_id] = 
    rpc_par_proxies_[par_id].size() - maj;

  for (auto& pair : rpc_par_proxies_[par_id]) {
    bool leader = false;
    if (pair.first == pair_leader_proxy.first) {
      leader = true;
    } else if (using_basic_) {
      // basic mode don't send msg to followers.
      continue;
    } else { // do nothing.
    }
    rrr::FutureAttr fu2;
    fu2.callback =
        [coo, this, leader, callback](Future* fu) {
          int32_t ret;
          TxnOutput outputs;
          fu->get_reply() >> ret >> outputs;
          callback(leader, ret, outputs);
        };
    Future::safe_release(pair.second->async_CarouselReadAndPrepare(cmd_id, md, leader, fu2));
  }
}

void CarouselCommo::BroadcastDecide(parid_t par_id,
                                 cmdid_t cmd_id,
                                 int32_t decision) {
  auto pair_leader_proxy = LeaderProxyForPartition(par_id);
  Log_debug("send dispatch to site %ld",
            pair_leader_proxy.first);
  auto proxy = pair_leader_proxy.second;
  FutureAttr fuattr;
  fuattr.callback = [] (Future* fu) {} ;
  Future::safe_release(proxy->async_CarouselDecide(cmd_id, decision, fuattr));
}

} // namespace janus
