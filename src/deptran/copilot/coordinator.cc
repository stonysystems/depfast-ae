#include "../__dep__.h"
#include "../constants.h"
#include "coordinator.h"
#include "commo.h"
#include "server.h"
#include "frame.h"

// #define DO_FINALIZE

namespace janus {

const char* indicator[] = {"COPILOT", "PILOT"};

bool FreeDangling(Communicator* comm, vector<std::pair<uint16_t, rrr::i64> > &dangling) {
  for (auto &dang : dangling) {
    comm->rpc_clients_[dang.first]->handle_free(dang.second);
  }

  return true;
}

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

  begin = Time::now(true);

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
start_prepare:
  static_cast<CopilotFrame*>(frame_)->n_prepare_++;
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
#ifdef DO_FINALIZE
  sq_quorum->Finalize(finalize_timeout_us,
                      std::bind(FreeDangling, commo(), std::placeholders::_1));
#endif
  // sq_quorum->log();
  auto curr_ins = sch_->GetInstance(slot_id_, is_pilot_);
  if(!curr_ins)
    return;
  /**
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
  } else if ((n_fastac = sq_quorum->GetCmds(Status::FAST_ACCEPTED_EQ).size()) > 0) {
    if (n_fastac < (maxFail() + 1) / 2) {
      /**
       * There are < [f+1]/2 replies r 2 S with fast-accepted as their
       * progress. Then pick no-op with an empty dependency.
       */
      cmd_now_ = make_shared<TpcNoopCommand>();  // no-op
      dep_ = 0;
    } else if (n_fastac >= maxFail()) {
      /**
       * There are >= f replies r 2 S with fast-accepted as their progress.
       * Then pick r's comand and dependency
       */
      auto& slct_cmd = sq_quorum->GetCmds(Status::FAST_ACCEPTED_EQ)[0];
      cmd_now_ = slct_cmd.cmd;
      dep_ = slct_cmd.dep_id;
    } else {
      /**
       * The remaining case is when there are in the range of [ [f+1]/2, f) replies r 2 S
       * with fast-accepted as their progress
       * 
       * In 3-replica(f=1) this case won't happen [1,1)
       */
      cmd_now_ = curr_ins->cmd;
      dep_ = curr_ins->dep_id;
    }
  } else if (sq_quorum->GetCmds(Status::NOT_ACCEPTED).size() >= maxFail() + 1) {
    cmd_now_ = make_shared<TpcNoopCommand>();  // no-op
    dep_ = 0;
  } else {
    // retry with higher ballot number
    sq_quorum->Show();
    Log_warn("%s : %lu Prepare failed, retry",
              indicator[is_pilot_], slot_id_);
    goto start_prepare;
  }

  if (GET_STATUS(curr_ins->status) >= Status::COMMITED) {
    // instance already committed, end fast-takeover in advance
    current_phase_ = Phase::COMMIT;
  } else {
    GotoNextPhase();
  }
}

void CoordinatorCopilot::FastAccept() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  static_cast<CopilotFrame*>(frame_)->n_fast_accept_++;
  if ((static_cast<CopilotFrame*>(frame_)->n_fast_accept_ & 0xfff) == 0)
      Log_info("fast/reg/total %u/%u/%u", static_cast<CopilotFrame*>(frame_)->n_fast_path_,
        static_cast<CopilotFrame*>(frame_)->n_regular_path_,
        static_cast<CopilotFrame*>(frame_)->n_fast_accept_);
  Log_debug(
      "Copilot coordinator %u broadcast FAST_ACCEPT, "
      "partition: %u, %s : %lu -> %lu",
      coo_id_, par_id_, indicator[is_pilot_], slot_id_, dep_);
      // dynamic_pointer_cast<TpcCommitCommand>(cmd_now_)->tx_id_);
  begin = Time::now(true);
  auto sq_quorum = commo()->BroadcastFastAccept(par_id_,
                                                is_pilot_, slot_id_,
                                                curr_ballot_,
                                                dep_,
                                                cmd_now_);
  // sq_quorum->id_ = dep_id_;
  // Log_debug("current coroutine's dep_id: %d", Coroutine::CurrentCoroutine()->dep_id_);

  sq_quorum->Wait();
  fac = Time::now(true) - begin;
#ifdef DO_FINALIZE
  sq_quorum->Finalize(finalize_timeout_us,
                      std::bind(FreeDangling, commo(), std::placeholders::_1));
#endif
  // cout << "fac";
  // sq_quorum->Log();

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
    static_cast<CopilotFrame*>(frame_)->n_fast_path_++;
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
      static_cast<CopilotFrame*>(frame_)->n_regular_path_++;
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
  static_cast<CopilotFrame*>(frame_)->n_accept_++;
  verify(current_phase_ == Phase::ACCEPT);
  Log_debug(
      "Copilot coordinator %u broadcast ACCEPT, "
      "partition: %u, %s : %lu -> %lu",
      coo_id_, par_id_, indicator[is_pilot_], slot_id_, dep_);

  begin = Time::now(true);
  auto sp_quorum = commo()->BroadcastAccept(par_id_,
                                            is_pilot_, slot_id_,
                                            curr_ballot_,
                                            dep_,
                                            cmd_now_);
  // sp_quorum->id_ = dep_id_;
  // Log_debug("current coroutine's dep_id: %d", Coroutine::CurrentCoroutine()->dep_id_);

  sp_quorum->Wait();
#ifdef DO_FINALIZE
  sp_quorum->Finalize(finalize_timeout_us,
                      std::bind(FreeDangling, commo(), std::placeholders::_1));
#endif
  // cout << "ac";
  // sp_quorum->Log();
  // if ((static_cast<CopilotFrame*>(frame_)->n_accept_ & 0x3ff) == 0)
  ac = Time::now(true) - begin;

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
  static_cast<CopilotFrame*>(frame_)->n_commit_++;
  verify(current_phase_ == Phase::COMMIT);
  commit_callback_();
  Log_debug("Copilot coordinator %u broadcast COMMIT for partition: %d, %s : %lu -> %lu",
            coo_id_, (int)par_id_, indicator[is_pilot_], slot_id_, dep_);

  auto sp_quorum = commo()->BroadcastCommit(par_id_,
                                            is_pilot_, slot_id_,
                                            dep_,
                                            cmd_now_);
  sp_quorum->Wait();  // in fact this doesn't wait since it's a fake quorum event
#ifdef DO_FINALIZE
  sp_quorum->Finalize(finalize_timeout_us,
                      std::bind(FreeDangling, commo(), std::placeholders::_1));
#endif
  /**
   * A pilot sets a takeover-timeout when it has a committed
   * command but does not know the final dependencies of all
   * potentially preceding entries, i.e., it has not seen a
   * commit for this entry’s final dependency.
   */
  auto dep_ins = sch_->GetInstance(dep_, REVERSE(is_pilot_));
  int take = 0;
  if (dep_ins && !in_fast_takeover_ && dep_ != 0) {
      begin = Time::now(true);
  // if (false) {
    // auto dep_ins = sch_->GetInstance(dep_, REVERSE(is_pilot_));
    /* It must proceed after all entries before its dependency have committed
     */
    if (!sch_->AllDepsEliminated(REVERSE(is_pilot_), dep_) &&
        !sch_->WaitMaxCommittedGT(REVERSE(is_pilot_), dep_, takeover_timeout_us)) {
      /*
       if timeout but the final dependency is still not committed,
       start takeover for all uncommitted entries
      */
      slotid_t start = sch_->GetMaxCommittedSlot(REVERSE(is_pilot_)) + 1;
      slotid_t end = dep_;
      uint8_t cur_pilot = is_pilot_;
      slotid_t cur_slot = slot_id_;  // save the property of current insatnce , cause initFastTakeover resets the property
      
      Log_info("TAKEOVER on %s for %lu from %lu to %lu", indicator[cur_pilot], cur_slot, start, end);
      for (auto i = start; i <= end; i++) {
        auto ucmit_ins = sch_->GetInstance(i, REVERSE(cur_pilot));
        if (ucmit_ins
            && (GET_TAKEOVER(ucmit_ins->status) == 0) // another coordiator is not taking over this instance
            && GET_STATUS(ucmit_ins->status) < Status::COMMITED
            && !sch_->EliminateNullDep(ucmit_ins)) {
          verify(IsPilot() || IsCopilot());
          take++;
          Log_info(
              "initiate fast-TAKEOVER on %s for slot %lu 's dep:"
              " %s, %lu, status: %x",
              indicator[cur_pilot], cur_slot, indicator[ucmit_ins->is_pilot],
              ucmit_ins->slot_id, ucmit_ins->status);
          initFastTakeover(ucmit_ins);
        }
      }
    }
    uint64_t finish = Time::now(true) - begin;
    // Log_info("takeover %lldus", finish);
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
  // another coordiator is already taking over this instance
  if (GET_TAKEOVER(ins->status) != 0)
    return;

  if (ins->status >= Status::COMMITED)
    return;
  
  /* When we reach this step, the shared int event should be either READY or TIMEOUT
  thus, update on max_committed_evt should have no effect on it */

  ins->status |= FLAG_TAKEOVER;  // prevent multiple takeover on the same instance
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
