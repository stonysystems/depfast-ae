
#include "quorum_event.h"
#include "coroutine.h"

namespace janus {

using rrr::Coroutine;

QuorumEvent::QuorumEvent(int n_total, int quorum)
    : Event(), n_total_(n_total), quorum_(quorum) {
  finalize_event_ = std::make_shared<IntEvent>(n_total_);
  finalize_event_->__debug_creator = 1;
}

void QuorumEvent::Finalize(
    uint64_t timeout,
    function<bool(vector<std::pair<uint16_t, rrr::i64> > &)> finalize_func) {
  vector<std::pair<uint16_t, rrr::i64> > dangling_rpc;
  for (auto &it : xids_)
    dangling_rpc.push_back(it);  // fetch out dangling rpc info before it's freed

  auto final_ev = finalize_event_;  // have to make a copy of finalized event (for reason, see next comment)
  Coroutine::CreateRun([final_ev, timeout, finalize_func, &dangling_rpc]() {
    bool ret = false;
    final_ev->Wait(timeout);
    /* by the time this fires, the quorum event could have been freed. Thus,
     avoid accesing the quorum event object or its members after this line */

    // didn't receive all RPC replies
    if (final_ev->status_ == Event::TIMEOUT) {
      // Log_info("finalized timeout");
      ret = finalize_func(dangling_rpc);
    }
  });
}

void QuorumEvent::AddXid(uint16_t site, rrr::i64 xid) {
  xids_[site] = xid;
}

void QuorumEvent::RemoveXid(uint16_t site) {
  auto it = xids_.find(site);
  if (it != xids_.end())
    xids_.erase(it);
}

void QuorumEvent::VoteYes() {
  n_voted_yes_++;
  Test();

  if (finalize_event_->status_ != Event::TIMEOUT)
    finalize_event_->Set(n_voted_yes_ + n_voted_no_);
}

void QuorumEvent::VoteNo() {
  n_voted_no_++;
  Test();

  if (finalize_event_->status_ != Event::TIMEOUT)
    finalize_event_->Set(n_voted_yes_ + n_voted_no_);
}

} // namespace janus
