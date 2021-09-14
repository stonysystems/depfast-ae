#include "../__dep__.h"
#include "../constants.h"
#include "coordinator.h"
#include "commo.h"
#include "server.h"

namespace janus {

const char* indicator[] = {"COPILOT", "PILOT"};

CoordinatorCopilot::CoordinatorCopilot(uint32_t coo_id,
                                       int32_t benchmark,
                                       ClientControlServiceImpl *ccsi,
                                       uint32_t thread_id)
  : Coordinator(coo_id, benchmark, ccsi, thread_id) {}

CoordinatorCopilot::~CoordinatorCopilot() {
  // Log_debug("copilot coord %d destroyed", (int)coo_id_);
}

inline ballot_t CoordinatorCopilot::makeUniqueBallot(ballot_t ballot) {
  /**
   * ballot format:
   * 63           8 7        0
   * ballot number | server id
   */
  return ballot << 8 | loc_id_;
}

inline ballot_t CoordinatorCopilot::pickGreaterBallot(ballot_t ballot) {
  return makeUniqueBallot((ballot >> 8) + 1);
}

void CoordinatorCopilot::Submit(shared_ptr<Marshallable> &cmd,
                                const std::function<void()> &func,
                                const std::function<void()> &exe_callback) {
  verify(IsPilot() || IsCopilot());  // only pilot or copilot can initiate command submission
  done_ = false;
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!cmd_now_);

  cmd_now_ = cmd;
  auto slot_and_dep = sch_->PickInitSlotAndDep();
  // TODO: check if this is correct, whether we can always set initial ballot as 0
  curr_ballot_ = makeUniqueBallot(0);
  is_pilot_ = IsPilot() ? YES : NO;
  slot_id_ = slot_and_dep.first;
  dep_ = slot_and_dep.second;
  verify(cmd_now_->kind_ != MarshallDeputy::UNKNOWN);
  commit_callback_ = func;
  GotoNextPhase();
}

void CoordinatorCopilot::Prepare() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  current_phase_ = Phase::PREPARE;
  ballot_t new_ballot = pickGreaterBallot(curr_ballot_);
  int n_fastac = 0;

  auto sq_quorum = commo()->BroadcastPrepare(par_id_,
                                             is_pilot_, slot_id_,
                                             new_ballot);
  Log_debug(
      "Copilot coordinator %u broadcast PREPARE for"
      "partition: %u, %s slot: %lu ballot %ld", coo_id_,
      par_id_, indicator[is_pilot_], slot_id_, new_ballot);
  // sq_quorum->id_ = dep_id_;

  sq_quorum->Wait();
  // sq_quorum->log();
  /**
   * TODO: very complex
   * The recovery value picking procedure is complex and its
   * full details appear in our accompanying technical report
   */
  direct_commit_ = false;
  if (sq_quorum->committed_seen_) {
    /**
     * If any of the PrepareOk messages indicate an entry is committed,
     * the pilot short-circuits waiting and commits that entry with the
     * same command and dependency.
     */
    auto& slct_cmd = sq_quorum->GetCmds(Status::COMMITED)[0];
    cmd_now_ = slct_cmd.cmd;
    dep_ = slct_cmd.dep_id;
    direct_commit_ = true;
  } else if (sq_quorum->GetCmds(Status::ACCEPTED).size() > 0) {
    /**
     * There are one or more replies r with accepted as their
     * progress. Then pick r’s command and dependency.
     */
    auto& slct_cmd = sq_quorum->GetCmds(Status::ACCEPTED)[0];
    cmd_now_ = slct_cmd.cmd;
    dep_ = slct_cmd.dep_id;
  } else if ((n_fastac = sq_quorum->GetCmds(Status::FAST_ACCEPTED).size()) > 0) {
    if (n_fastac < (maxFail() + 1) / 2) {
      /**
       * There are < [f+1]/2 replies r 2 S with fast-accepted as their
       * progress. Then pick no-op with an empty dependency.
       * TODO
       */
      cmd_now_ = sq_quorum->GetCmds(Status::ACCEPTED)[0].cmd;  // no-op
      dep_ = 0;
    } else if (n_fastac >= maxFail()) {
      auto& slct_cmd = sq_quorum->GetCmds(Status::FAST_ACCEPTED)[0];
      cmd_now_ = slct_cmd.cmd;
      dep_ = slct_cmd.dep_id;
    } else {
      // TODO
      cmd_now_ = sch_->GetInstance(slot_id_, is_pilot_)->cmd;
      dep_ = sch_->GetInstance(slot_id_, is_pilot_)->dep_id;
    }
  } else {
    verify(0);
  }

  if (sch_->GetInstance(slot_id_, is_pilot_)->status >= Status::COMMITED) {
    // instance already committed, end fast-takeover in advance
    current_phase_ = Phase::COMMIT;
  } else {
    GotoNextPhase();
  }
}

void CoordinatorCopilot::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug(
      "Copilot coordinator %u broadcast FAST_ACCEPT, "
      "partition: %u, %s : %lu -> %lu, tx: %x",
      coo_id_, par_id_, indicator[is_pilot_], slot_id_, dep_,
      dynamic_pointer_cast<TpcCommitCommand>(cmd_now_)->tx_id_);

  auto sq_quorum = commo()->BroadcastFastAccept(par_id_,
                                                is_pilot_, slot_id_,
                                                curr_ballot_,
                                                dep_,
                                                cmd_now_);
  // sq_quorum->id_ = dep_id_;
  // Log_debug("current coroutine's dep_id: %d", Coroutine::CurrentCoroutine()->dep_id_);

  sq_quorum->Wait();
  // sq_quorum->log();

  fast_path_ = false;
  if (sq_quorum->FastYes()) {
    /**
     * If a pilot gathers a fast quorum, then enough replicas have
     * agreed to its initial dependency that it will always be recovered
     * from any majority quorum of replicas. Thus, it is safe for the
     * pilot to commit this entry on the fast path and continue to execution.
     */
    fast_path_ = true;
    committed_ = true; // fast-path
    Log_debug("commit on fast path");
  } else {
    if (sq_quorum->Yes()) {
      /**
       * go to accept phase (regular-path):
       * it must use the (f+1)-th dependency to ensure quorum intersection
       * with any command that has already been committed and potentially
       * executed by the other pilot
       */
      dep_ = sq_quorum->GetFinalDep();
      Log_debug("Final dep: %lu, continue on regular path", dep_);
    } else if (sq_quorum->No()) {
      // TODO process the case: failed to get a majority.
      verify(0);
    } else {
      // TODO process timeout.
      verify(0);
    }
  }
  GotoNextPhase();
}

void CoordinatorCopilot::Accept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(current_phase_ == Phase::ACCEPT);
  Log_debug(
      "Copilot coordinator %u broadcast ACCEPT, "
      "partition: %u, %s : %lu -> %lu",
      coo_id_, par_id_, indicator[is_pilot_], slot_id_, dep_);

  auto sp_quorum = commo()->BroadcastAccept(par_id_,
                                            is_pilot_, slot_id_,
                                            curr_ballot_,
                                            dep_,
                                            cmd_now_);
  // sp_quorum->id_ = dep_id_;
  // Log_debug("current coroutine's dep_id: %d", Coroutine::CurrentCoroutine()->dep_id_);

  sp_quorum->Wait();
  // sp_quorum->log();

  if (sp_quorum->Yes()) {
    committed_ = true;
  } else if (sp_quorum->No()) {
    /**
     * TODO: process the case: failed to get a majority.
     * An consensus instance with higher ballot is ongoing,
     * abandon this one
     */
  } else {
    // TODO process timeout.
    verify(0);
  }
  GotoNextPhase();
}

void CoordinatorCopilot::Commit() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(current_phase_ == Phase::COMMIT);
  commit_callback_();
  Log_debug("Copilot coordinator %u broadcast COMMIT for partition: %d, %s : %lu -> %lu",
            coo_id_, (int)par_id_, indicator[is_pilot_], slot_id_, dep_);
  commo()->BroadcastCommit(par_id_,
                           is_pilot_, slot_id_,
                           dep_,
                           cmd_now_);
  /**
   * A pilot sets a takeover-timeout when it has a committed
   * command but does not know the final dependencies of all
   * potentially preceding entries, i.e., it has not seen a
   * commit for this entry’s final dependency.
   * 
   * ? should fast takeover continue indefinitely?
   */
  if (!in_fast_takeover_ && dep_ != 0) {
    auto dep_ins = sch_->GetInstance(dep_, !is_pilot_);
    if (dep_ins->status < Status::COMMITED) {
      verify(IsPilot() || IsCopilot());
      Log_debug(
          "initiate fast-TAKEOVER on %s for slot %lu 's dep:"
          " %s, %lu, status: %d",
          indicator[(int)IsPilot()], slot_id_, indicator[dep_ins->is_pilot],
          dep_ins->slot_id, dep_ins->status);
      initFastTakeover(dep_ins);
    }
  }

  clearStatus();
}

void CoordinatorCopilot::GotoNextPhase() {
  phase_++;
  switch (current_phase_) {
  case Phase::INIT_END:
    current_phase_ = Phase::FAST_ACCEPT;
    if (IsPilot() || IsCopilot()) {
      FastAccept();
    } else {
      // TODO
      verify(0);
    }
    break;
  case Phase::PREPARE:
    if (direct_commit_) {
      current_phase_ = Phase::COMMIT;
      Commit();
    } else {
      current_phase_ = Phase::ACCEPT;
      Accept();
    }
    break;
  case Phase::FAST_ACCEPT:
    if (fast_path_) {
      current_phase_ = Phase::COMMIT;
      Commit();
    } else {
      current_phase_ = Phase::ACCEPT;
      Accept();
    }
    break;
  case Phase::ACCEPT:
    current_phase_ = Phase::COMMIT;
    Commit();
    break;
  default:
    break;
  }
}

void CoordinatorCopilot::initFastTakeover(shared_ptr<CopilotData>& ins) {
  // one coordiator is taking over this instance
  if (ins->status == Status::TAKEOVER)
    return;
  auto e = Reactor::CreateSpEvent<TimeoutEvent>(takeover_timeout);
  e->Wait();

  if (ins->status >= Status::COMMITED)
    return;

  ins->status = Status::TAKEOVER;  // prevent multiple takeover on the same instance
  // reuse current coordinator
  curr_ballot_ = ins->ballot;
  // is_pilot_ = !is_pilot_;
  is_pilot_ = ins->is_pilot;  // takeover another pilot
  slot_id_ = ins->slot_id;
  dep_ = 0; // dependency doesn't matter
  in_fast_takeover_ = true;
  Prepare();
}

inline void CoordinatorCopilot::clearStatus() {
  if (done_)
    return;
  done_ = true;
  curr_ballot_ = 0;
  cmd_now_ = nullptr;
  current_phase_ = INIT_END;
  fast_path_ = false;
  direct_commit_ = false;
  in_fast_takeover_ = false;

  is_pilot_ = 0;
  slot_id_ = 0;
  slot_hint_ = nullptr;
  dep_ = 0;
}

} // namespace janus
