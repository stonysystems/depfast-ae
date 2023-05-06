
#include "commo.h"
#include "epaxos_rpc.h"
#include "macros.h"


namespace janus {

EpaxosCommo::EpaxosCommo(PollMgr* poll) : Communicator(poll) {
}

shared_ptr<EpaxosPreAcceptQuorumEvent> 
EpaxosCommo::SendPreAccept(const siteid_t& site_id,
                           const parid_t& par_id,
                           const bool& is_recovery,
                           const epoch_t& epoch,
                           const ballot_t& ballot_no,
                           const uint64_t& ballot_replica_id,
                           const uint64_t& leader_replica_id,
                           const uint64_t& instance_no,
                           const shared_ptr<Marshallable>& cmd,
                           const string& dkey,
                           const uint64_t& seq,
                           const unordered_map<uint64_t, uint64_t>& deps) {
  auto proxies = rpc_par_proxies_[par_id];
  int n_total = NSERVERS - 1;
  if (thrifty) {
    n_total = FAST_PATH_QUORUM - 1;
  }
  int fast_path_quorum = FAST_PATH_QUORUM - 1;
  int slow_path_quorum = SLOW_PATH_QUORUM - 1;
  auto ev = Reactor::CreateSpEvent<EpaxosPreAcceptQuorumEvent>(n_total, thrifty, is_recovery, fast_path_quorum, slow_path_quorum);
  int sent = 0;
  int id = site_id;
  while (proxies.size() != 0 && sent < n_total) {
    id = (id + 1) % proxies.size();
    if (proxies[id].first == site_id) {
      continue;
    }
    sent++;
    EpaxosProxy *proxy = (EpaxosProxy*) proxies[id].second;
    FutureAttr fuattr;
    fuattr.callback = [ev](Future* fu) {
      status_t status;
      fu->get_reply() >> status;
      epoch_t highest_seen_epoch;
      ballot_t highest_seen_ballot_no;
      uint64_t highest_seen_replica_id;
      uint64_t updated_seq;
      unordered_map<uint64_t, uint64_t> updated_deps;
      unordered_set<uint64_t> committed_deps;
      fu->get_reply() >> highest_seen_epoch >> highest_seen_ballot_no >> highest_seen_replica_id >> updated_seq >> updated_deps >> committed_deps;
      EpaxosPreAcceptReply reply(static_cast<EpaxosPreAcceptStatus>(status), 
                                  highest_seen_epoch, 
                                  highest_seen_ballot_no, 
                                  highest_seen_replica_id, 
                                  updated_seq, 
                                  updated_deps,
                                  committed_deps);
      if (status == EpaxosPreAcceptStatus::IDENTICAL) {
        ev->VoteIdentical(reply);
      } else if (status == EpaxosPreAcceptStatus::NON_IDENTICAL) {
        ev->VoteNonIdentical(reply);
      } else {
        ev->VoteNo(reply);
      }
    };
    MarshallDeputy md_cmd(cmd);
    Call_Async(proxy, 
                PreAccept, 
                epoch, 
                ballot_no, 
                ballot_replica_id,
                leader_replica_id, 
                instance_no,
                md_cmd,
                dkey,
                seq, 
                deps, 
                fuattr);
  }
  return ev;
}

shared_ptr<EpaxosAcceptQuorumEvent>
EpaxosCommo::SendAccept(const siteid_t& site_id,
                        const parid_t& par_id,
                        const epoch_t& epoch,
                        const ballot_t& ballot_no,
                        const uint64_t& ballot_replica_id,
                        const uint64_t& leader_replica_id,
                        const uint64_t& instance_no,
                        const shared_ptr<Marshallable>& cmd,
                        const string& dkey,
                        const uint64_t& seq,
                        const unordered_map<uint64_t, uint64_t>& deps) {
  auto proxies = rpc_par_proxies_[par_id];
  int n_total = NSERVERS - 1;
  if (thrifty) {
    n_total = NSERVERS/2;
  }
  int quorum = SLOW_PATH_QUORUM - 1;
  auto ev = Reactor::CreateSpEvent<EpaxosAcceptQuorumEvent>(n_total, quorum);
  int sent = 0;
  int id = site_id;
  while (proxies.size() != 0 && sent < n_total) {
    id = (id + 1) % proxies.size();
    if (proxies[id].first == site_id) {
      continue;
    }
    sent++;
    EpaxosProxy *proxy = (EpaxosProxy*) proxies[id].second;
    FutureAttr fuattr;
    fuattr.callback = [ev](Future* fu) {
      bool_t status;
      fu->get_reply() >> status;
      epoch_t highest_seen_epoch;
      ballot_t highest_seen_ballot_no;
      uint64_t highest_seen_replica_id;
      fu->get_reply() >> highest_seen_epoch >> highest_seen_ballot_no >> highest_seen_replica_id;
      EpaxosAcceptReply reply(status, 
                              highest_seen_epoch, 
                              highest_seen_ballot_no, 
                              highest_seen_replica_id);
      if (status) {
        ev->VoteYes(reply);
      } else {
        ev->VoteNo(reply);
      }
    };
    MarshallDeputy md_cmd(cmd);
    Call_Async(proxy, 
                Accept, 
                epoch, 
                ballot_no, 
                ballot_replica_id, 
                leader_replica_id, 
                instance_no, 
                md_cmd, 
                dkey,
                seq, 
                deps, 
                fuattr);
  }
  return ev;
}

void
EpaxosCommo::SendCommit(const siteid_t& site_id,
                        const parid_t& par_id,
                        const epoch_t& epoch,
                        const ballot_t& ballot_no,
                        const uint64_t& ballot_replica_id,
                        const uint64_t& leader_replica_id,
                        const uint64_t& instance_no,
                        const shared_ptr<Marshallable>& cmd,
                        const string& dkey,
                        const uint64_t& seq,
                        const unordered_map<uint64_t, uint64_t>& deps) {
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    if (p.first == site_id) {
      continue;
    }
    EpaxosProxy *proxy = (EpaxosProxy*) p.second;
    FutureAttr fuattr;
    MarshallDeputy md_cmd(cmd);
    Call_Async(proxy, 
                Commit, 
                epoch, 
                ballot_no, 
                ballot_replica_id,
                leader_replica_id,
                instance_no, 
                md_cmd, 
                dkey,
                seq, 
                deps, 
                fuattr);
  }
  return;
}

shared_ptr<EpaxosTryPreAcceptQuorumEvent> 
EpaxosCommo::SendTryPreAccept(const siteid_t& site_id,
                              const parid_t& par_id,
                              const unordered_set<siteid_t>& preaccepted_sites,
                              const epoch_t& epoch,
                              const ballot_t& ballot_no,
                              const uint64_t& ballot_replica_id,
                              const uint64_t& leader_replica_id,
                              const uint64_t& instance_no,
                              const shared_ptr<Marshallable>& cmd,
                              const string& dkey,
                              const uint64_t& seq,
                              const unordered_map<uint64_t, uint64_t>& deps) {
  auto proxies = rpc_par_proxies_[par_id];
  int n_total = NSERVERS - preaccepted_sites.size();
  int quorum = (NSERVERS/2) - preaccepted_sites.size();
  auto ev = Reactor::CreateSpEvent<EpaxosTryPreAcceptQuorumEvent>(n_total, quorum);
  for (auto& p : proxies) {
      if (p.first == site_id || p.first == leader_replica_id || preaccepted_sites.count(p.first) != 0) {
        continue;
      }
      EpaxosProxy *proxy = (EpaxosProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [ev](Future* fu) {
        status_t status;
        epoch_t highest_seen_epoch;
        ballot_t highest_seen_ballot_no;
        uint64_t highest_seen_replica_id;
        uint64_t conflict_replica_id;
        uint64_t conflict_instance_no;
        fu->get_reply() >> status >> highest_seen_epoch >> highest_seen_ballot_no >> highest_seen_replica_id;
        fu->get_reply() >> conflict_replica_id >> conflict_instance_no;
        EpaxosTryPreAcceptReply reply(static_cast<EpaxosTryPreAcceptStatus>(status), 
                                      highest_seen_epoch, 
                                      highest_seen_ballot_no, 
                                      highest_seen_replica_id,
                                      conflict_replica_id,
                                      conflict_instance_no);
        if (status != EpaxosTryPreAcceptStatus::REJECTED) {
          ev->VoteYes(reply);
        } else {
          ev->VoteNo(reply);
        }
      };
      MarshallDeputy md_cmd(cmd);
      Call_Async(proxy, 
                 TryPreAccept, 
                 epoch, 
                 ballot_no, 
                 ballot_replica_id,
                 leader_replica_id, 
                 instance_no, 
                 md_cmd, 
                 dkey,
                 seq, 
                 deps, 
                 fuattr);
  }
  return ev;
}

shared_ptr<EpaxosPrepareQuorumEvent>
EpaxosCommo::SendPrepare(const siteid_t& site_id,
                         const parid_t& par_id,
                         const epoch_t& epoch,
                         const ballot_t& ballot_no,
                         const uint64_t& ballot_replica_id,
                         const uint64_t& leader_replica_id,
                         const uint64_t& instance_no) {
  auto proxies = rpc_par_proxies_[par_id];
  int n_total = NSERVERS - 1;
  if (thrifty) {
    n_total = FAST_PATH_QUORUM - 1;
  }
  int quorum = SLOW_PATH_QUORUM - 1;
  auto ev = Reactor::CreateSpEvent<EpaxosPrepareQuorumEvent>(n_total, quorum);
  int sent = 0;
  int id = site_id;
  while (proxies.size() != 0 && sent < n_total) {
    id = (id + 1) % proxies.size();
    if (proxies[id].first == site_id) {
      continue;
    }
    sent++;
    siteid_t curr_site_id = proxies[id].first;
    EpaxosProxy *proxy = (EpaxosProxy*) proxies[id].second;
    FutureAttr fuattr;
    fuattr.callback = [ev, curr_site_id](Future* fu) {
      bool_t status;
      fu->get_reply() >> status;
      MarshallDeputy md_cmd;
      string dkey;
      uint64_t seq;
      unordered_map<uint64_t, uint64_t> deps;
      status_t cmd_state;
      uint64_t acceptor_replica_id;
      epoch_t highest_seen_epoch;
      ballot_t highest_seen_ballot_no;
      uint64_t highest_seen_replica_id;
      fu->get_reply() >> md_cmd >> dkey >> seq >> deps >> cmd_state >> acceptor_replica_id;
      fu->get_reply() >> highest_seen_epoch >> highest_seen_ballot_no >> highest_seen_replica_id;
      shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
      EpaxosPrepareReply reply(status, 
                               cmd, 
                               dkey, 
                               seq,
                               deps,
                               cmd_state,
                               acceptor_replica_id,
                               highest_seen_epoch,
                               highest_seen_ballot_no,
                               highest_seen_replica_id);
      if (status) {
        ev->VoteYes(curr_site_id, reply);
      } else {
        ev->VoteNo(reply);
      }
    };
    Call_Async(proxy, 
                Prepare, 
                epoch, 
                ballot_no, 
                ballot_replica_id,
                leader_replica_id,
                instance_no, 
                fuattr);
  }
  return ev;
}

} // namespace janus
