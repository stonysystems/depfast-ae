
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
  int n_total = NSERVERS-1;
  int fast_path_quorum = NSERVERS-2;
  int slow_path_quorum = NSERVERS/2;
  auto ev = Reactor::CreateSpEvent<EpaxosPreAcceptQuorumEvent>(n_total, is_recovery, fast_path_quorum, slow_path_quorum);
  for (auto& p : proxies) {
      if (p.first == site_id) {
        continue;
      }
      EpaxosProxy *proxy = (EpaxosProxy*) p.second;
      FutureAttr fuattr;
      fuattr.callback = [ev](Future* fu) {
        status_t status;
        fu->get_reply() >> status;
        epoch_t highest_seen_epoch;
        ballot_t highest_seen_ballot_no;
        uint64_t highest_seen_replica_id;
        uint64_t updated_seq;
        unordered_map<uint64_t, uint64_t> updated_deps;
        fu->get_reply() >> highest_seen_epoch >> highest_seen_ballot_no >> highest_seen_replica_id >> updated_seq >> updated_deps;
        EpaxosPreAcceptReply reply(static_cast<EpaxosPreAcceptStatus>(status), 
                                   highest_seen_epoch, 
                                   highest_seen_ballot_no, 
                                   highest_seen_replica_id, 
                                   updated_seq, 
                                   updated_deps);
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
  int n_total = NSERVERS-1;
  int quorum = NSERVERS/2;
  auto ev = Reactor::CreateSpEvent<EpaxosAcceptQuorumEvent>(n_total, quorum);
  for (auto& p : proxies) {
    if (p.first == site_id) {
      continue;
    }
    EpaxosProxy *proxy = (EpaxosProxy*) p.second;
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

shared_ptr<QuorumEvent>
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
  int n_total = NSERVERS-1;
  auto ev = Reactor::CreateSpEvent<QuorumEvent>(n_total, n_total);
  for (auto& p : proxies) {
    if (p.first == site_id) {
      continue;
    }
    EpaxosProxy *proxy = (EpaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [ev](Future* fu) {
      ev->VoteYes();
    };
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
  int n_total = NSERVERS-1;
  int quorum = NSERVERS/2;
  auto ev = Reactor::CreateSpEvent<EpaxosPrepareQuorumEvent>(n_total, quorum);
  for (auto& p : proxies) {
    if (p.first == site_id) {
      continue;
    }
    EpaxosProxy *proxy = (EpaxosProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [ev](Future* fu) {
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
        ev->VoteYes(reply);
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
