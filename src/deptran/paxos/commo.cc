
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace janus {

MultiPaxosCommo::MultiPaxosCommo(PollMgr* poll) : Communicator(poll) {
//  verify(poll != nullptr);
}

shared_ptr<PaxosPrepareQuorumEvent>
MultiPaxosCommo::SendForward(uint64_t tx_id,
                                  int ret,
                                  parid_t par_id,
                                  uint64_t follower_id,
                                  uint64_t dep_id,
                                  shared_ptr<Marshallable> cmd){
  auto e = Reactor::CreateSpEvent<PaxosPrepareQuorumEvent>(1, 1);
  auto src_coroid = e->GetCoroId();
  auto leader_id = LeaderProxyForPartition(par_id).first;
  auto leader_proxy = (MultiPaxosProxy*) LeaderProxyForPartition(par_id).second;

  FutureAttr fuattr;
  fuattr.callback = [e, leader_id, src_coroid, follower_id](Future* fu) {
    uint64_t coro_id = 0;
    fu->get_reply() >> coro_id;
    e->FeedResponse(1);
    e->add_dep(follower_id, src_coroid, leader_id, coro_id);
  };
  
  int prepare_or_commit = 0;
  if(cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE){
    prepare_or_commit = 1;
  }
  else if(cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT){
    prepare_or_commit = 0;
  }
  else{verify(0);}
  Future::safe_release(leader_proxy->async_Forward(tx_id, ret, prepare_or_commit, dep_id));

  return e;
}

void MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                       slotid_t slot_id,
                                       ballot_t ballot,
                                       const function<void(Future*)>& cb) {
  verify(0); // deprecated function
  auto proxies = rpc_par_proxies_[par_id];
  auto leader_id = LeaderProxyForPartition(par_id).first;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }
}

shared_ptr<PaxosPrepareQuorumEvent>
MultiPaxosCommo::BroadcastPrepare(parid_t par_id,
                                  slotid_t slot_id,
                                  ballot_t ballot) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<PaxosPrepareQuorumEvent>(n, n/2+1);
  auto src_coroid = e->GetCoroId();
  auto proxies = rpc_par_proxies_[par_id];

  WAN_WAIT;
  auto leader_id = LeaderProxyForPartition(par_id).first;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    auto follower_id = p.first;

    e->add_dep(leader_id, src_coroid, follower_id, -1);

    FutureAttr fuattr;
    fuattr.callback = [e, ballot, leader_id, src_coroid, follower_id](Future* fu) {
      ballot_t b = 0;
      uint64_t coro_id = 0;
      fu->get_reply() >> b >> coro_id;
      e->FeedResponse(b==ballot);
      e->deps[leader_id][src_coroid][follower_id].erase(-1);
      e->deps[leader_id][src_coroid][follower_id].insert(coro_id);
      // TODO add max accepted value.
    };
    Future::safe_release(proxy->async_Prepare(slot_id, ballot, fuattr));
  }
  return e;
}

shared_ptr<PaxosAcceptQuorumEvent>
MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                 slotid_t slot_id,
                                 ballot_t ballot,
                                 shared_ptr<Marshallable> cmd) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, n/2+1);
//  auto e = Reactor::CreateSpEvent<PaxosAcceptQuorumEvent>(n, n);

  auto src_coroid = e->GetCoroId();
  auto proxies = rpc_par_proxies_[par_id];
  auto leader_id = LeaderProxyForPartition(par_id).first; // might need to be changed to coordinator's id
  vector<Future*> fus;
  WAN_WAIT;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    auto follower_id = p.first;

    e->add_dep(leader_id, src_coroid, follower_id, -1);

    FutureAttr fuattr;
    fuattr.callback = [e, ballot, leader_id, src_coroid, follower_id] (Future* fu) {
      ballot_t b = 0;
      uint64_t coro_id = 0;
      fu->get_reply() >> b >> coro_id;
      e->FeedResponse(b==ballot);
      e->deps[leader_id][src_coroid][follower_id].erase(-1);
      e->deps[leader_id][src_coroid][follower_id].insert(coro_id);
    };
    MarshallDeputy md(cmd);
    auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
    Future::safe_release(f);
  }
  return e;
}

void MultiPaxosCommo::BroadcastAccept(parid_t par_id,
                                      slotid_t slot_id,
                                      ballot_t ballot,
                                      shared_ptr<Marshallable> cmd,
                                      const function<void(Future*)>& cb) {
  verify(0); // deprecated function
  auto proxies = rpc_par_proxies_[par_id];
  auto leader_id = LeaderProxyForPartition(par_id).first;
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    MarshallDeputy md(cmd);
    auto f = proxy->async_Accept(slot_id, ballot, md, fuattr);
    Future::safe_release(f);
  }
//  verify(0);
}

void MultiPaxosCommo::BroadcastDecide(const parid_t par_id,
                                      const slotid_t slot_id,
                                      const ballot_t ballot,
                                      const shared_ptr<Marshallable> cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  auto leader_id = LeaderProxyForPartition(par_id).first;
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (MultiPaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};
    MarshallDeputy md(cmd);
    auto f = proxy->async_Decide(slot_id, ballot, md, fuattr);
    //sp_quorum_event->add_dep(leader_id, p.first);
    Future::safe_release(f);
  }
}

} // namespace janus
