#include "../__dep__.h"
#include "../constants.h"
#include "commo.h"

// #define SKIP

namespace janus {

void CopilotFastAcceptQuorumEvent::FeedResponse(bool y, bool ok) {
  if (y) {
    VoteYes();
    if (ok)
      n_fastac_ok_++;
    else
      n_fastac_reply_++;
  } else {
    VoteNo();
  }
}

void CopilotFastAcceptQuorumEvent::FeedRetDep(uint64_t dep) {
  verify(ret_deps_.size() < n_total_);
  ret_deps_.push_back(dep);
}

uint64_t CopilotFastAcceptQuorumEvent::GetFinalDep() {
  verify(ret_deps_.size() >= n_total_ / 2 + 1);
  std::sort(ret_deps_.begin(), ret_deps_.end());
  return ret_deps_[n_total_ / 2];
}

bool CopilotFastAcceptQuorumEvent::FastYes() {
  return n_fastac_ok_ >= CopilotCommo::fastQuorumSize(n_total_);
}

bool CopilotFastAcceptQuorumEvent::FastNo() {
  return Yes() && !FastYes();
}


inline void CopilotPrepareQuorumEvent::FeedRetCmd(ballot_t ballot,
                                                  uint64_t dep,
                                                  uint8_t is_pilot, slotid_t slot,
                                                  shared_ptr<Marshallable> cmd,
                                                  enum Status status) {
  uint32_t int_status = GET_STATUS(static_cast<uint32_t>(status));
  // int_status &= CLR_FLAG_TAKEOVER;
  verify(int_status <= n_status);
  if (int_status >= Status::COMMITED) { // committed or executed
    committed_seen_ = true;
    int_status = Status::COMMITED;  // reduce all status greater than COMMIT to COMMIT
  }
  ret_cmds_by_status_[int_status].emplace_back(CopilotData{cmd, dep, is_pilot, slot, ballot, int_status, 0, 0});
}

inline size_t CopilotPrepareQuorumEvent::GetCount(enum Status status) {
  return ret_cmds_by_status_[status].size();
}

vector<CopilotData>& CopilotPrepareQuorumEvent::GetCmds(enum Status status) {
  return ret_cmds_by_status_[status];
}

bool CopilotPrepareQuorumEvent::IsReady() {
  if (timeouted_) {
    // TODO add time out support
    return true;
  }
  if (committed_seen_) {
    return true;
  }
  if (Yes()) {
    //      Log_info("voted: %d is equal or greater than quorum: %d",
    //                (int)n_voted_yes_, (int) quorum_);
    // ready_time = std::chrono::steady_clock::now();
    return true;
  } else if (No()) {
    return true;
  }
  //    Log_debug("voted: %d is smaller than quorum: %d",
  //              (int)n_voted_, (int) quorum_);
  return false;
}

void CopilotPrepareQuorumEvent::Show() {
  std::cout << committed_seen_ << std::endl;
  for (int i = 0; i < ret_cmds_by_status_.size(); i++)
    std::cout << i << ":" << ret_cmds_by_status_[i].size() << std::endl;
}


CopilotCommo::CopilotCommo(PollMgr *poll) : Communicator(poll) {}

shared_ptr<CopilotPrepareQuorumEvent>
CopilotCommo::BroadcastPrepare(parid_t par_id,
                               uint8_t is_pilot,
                               slotid_t slot_id,
                               ballot_t ballot) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<CopilotPrepareQuorumEvent>(n, quorumSize(n));
  auto proxies = rpc_par_proxies_[par_id];
  struct DepId di;

  // WAN_WAIT
  for (auto& p : proxies) {
    auto proxy = (CopilotProxy *)p.second;
    auto site = p.first;

    FutureAttr fuattr;
    fuattr.callback = [e, ballot, is_pilot, slot_id, site](Future *fu) {
      MarshallDeputy md;
      ballot_t b;
      uint64_t dep;
      status_t status;

      fu->get_reply() >> md >> b >> dep >> status;
      bool ok = (ballot == b);
      e->FeedResponse(ok);
      if (ok) {
        e->FeedRetCmd(ballot,
                      dep,
                      is_pilot, slot_id,
                      const_cast<MarshallDeputy&>(md).sp_data_,
                      static_cast<enum Status>(status));
      }

      e->RemoveXid(site);
    };

    Future *f = proxy->async_Prepare(is_pilot, slot_id, ballot, di, fuattr);
    e->AddXid(site, f->get_xid());
    Future::safe_release(f);
  }

  return e;
}

shared_ptr<CopilotFastAcceptQuorumEvent>
CopilotCommo::BroadcastFastAccept(parid_t par_id,
                                  uint8_t is_pilot,
                                  slotid_t slot_id,
                                  ballot_t ballot,
                                  uint64_t dep,
                                  shared_ptr<Marshallable> cmd) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<CopilotFastAcceptQuorumEvent>(n, fastQuorumSize(n));
  auto proxies = rpc_par_proxies_[par_id];
  struct DepId di;

  // WAN_WAIT
  for (auto& p : proxies) {
    auto proxy = (CopilotProxy *)p.second;
    auto site = p.first;

#ifdef SKIP
    if (site == 2) continue;
#endif
    if (site == loc_id_) {
      ballot_t b;
      slotid_t sgst_dep;
      static_cast<CopilotServer *>(rep_sched_)->OnFastAccept(
        is_pilot, slot_id, ballot, dep, cmd, di, &b, &sgst_dep, nullptr);
      e->FeedResponse(true, true);
      e->FeedRetDep(dep);
    } else {
      FutureAttr fuattr;
      fuattr.callback = [e, dep, ballot, site](Future *fu) {
        ballot_t b;
        slotid_t sgst_dep;

        fu->get_reply() >> b >> sgst_dep;
        bool ok = (ballot == b);
        e->FeedResponse(ok, sgst_dep == dep);
        if (ok) {
          e->FeedRetDep(sgst_dep);
        }

        e->RemoveXid(site);
      };

      verify(cmd);
      MarshallDeputy md(cmd);
      Future *f = proxy->async_FastAccept(is_pilot, slot_id, ballot, dep, md, di, fuattr);
      e->AddXid(site, f->get_xid());
      Future::safe_release(f);
    }
  }

  return e;
}

shared_ptr<CopilotAcceptQuorumEvent>
CopilotCommo::BroadcastAccept(parid_t par_id,
                              uint8_t is_pilot,
                              slotid_t slot_id,
                              ballot_t ballot,
                              uint64_t dep,
                              shared_ptr<Marshallable> cmd) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<CopilotAcceptQuorumEvent>(n, quorumSize(n));
  auto proxies = rpc_par_proxies_[par_id];
  struct DepId di;

  // WAN_WAIT
  for (auto& p : proxies) {
    auto proxy = (CopilotProxy *)p.second;
    auto site = p.first;

#ifdef SKIP
    if (site == 2) continue;
#endif
    if (site == loc_id_) {
      ballot_t b;
      static_cast<CopilotServer *>(rep_sched_)->OnAccept(
        is_pilot, slot_id, ballot, dep, cmd, di, &b, nullptr);
      e->FeedResponse(true);
    } else {
      FutureAttr fuattr;
      fuattr.callback = [e, ballot, site](Future *fu) {
        ballot_t b;
        fu->get_reply() >> b;
        e->FeedResponse(ballot == b);

        e->RemoveXid(site);
      };

      MarshallDeputy md(cmd);
      Future *f = proxy->async_Accept(is_pilot, slot_id, ballot, dep, md, di, fuattr);
      e->AddXid(site, f->get_xid());
      Future::safe_release(f);
    }
  }

  return e;
}

shared_ptr<CopilotFakeQuorumEvent>
CopilotCommo::BroadcastCommit(parid_t par_id,
                                   uint8_t is_pilot,
                                   slotid_t slot_id,
                                   uint64_t dep,
                                   shared_ptr<Marshallable> cmd) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<CopilotFakeQuorumEvent>(n);
  auto proxies = rpc_par_proxies_[par_id];
  
  for (auto& p : proxies) {
    auto proxy = (CopilotProxy*) p.second;
    auto site = p.first;

#ifdef SKIP
    if (site == 2) continue;
#endif
    FutureAttr fuattr;
    fuattr.callback = [e, site](Future* fu) {
      e->RemoveXid(site);
    };
    MarshallDeputy md(cmd);
    Future *f = proxy->async_Commit(is_pilot, slot_id, dep, md, fuattr);
    e->AddXid(site, f->get_xid());
    Future::safe_release(f);
  }

  return e;
}

inline int CopilotCommo::maxFailure(int total) {
  // TODO: now only for odd number
  return total / 2;
}

inline int CopilotCommo::fastQuorumSize(int total) {
  int max_fail = maxFailure(total);
  return max_fail + (max_fail + 1) / 2;
}

inline int CopilotCommo::quorumSize(int total) {
  return maxFailure(total) + 1;
}

} // namespace janus
