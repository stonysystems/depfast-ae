
#include "../__dep__.h"
#include "../constants.h"
#include "coordinator.h"
#include "commo.h"

#include "server.h"

namespace janus {

CoordinatorFpgaRaft::CoordinatorFpgaRaft(uint32_t coo_id,
                                             int32_t benchmark,
                                             ClientControlServiceImpl* ccsi,
                                             uint32_t thread_id)
    : Coordinator(coo_id, benchmark, ccsi, thread_id) {
}

bool CoordinatorFpgaRaft::IsLeader() {
   //return this->loc_id_ == 2;
   return this->sch_->IsLeader() ;
}

void CoordinatorFpgaRaft::Forward(shared_ptr<Marshallable>& cmd,
                                   const function<void()>& func,
                                   const function<void()>& exe_callback) {
    // TODO do we need a lock here ?
    auto e = commo()->SendForward(par_id_, loc_id_, cmd);
    e->Wait();
    uint64_t cmt_idx = e->CommitIdx() ;
    cmt_idx_ = cmt_idx ;
    Coroutine::CreateRun([&] () {
      this->sch_->SpCommit(cmt_idx) ;
    }) ;
}


void CoordinatorFpgaRaft::Submit(shared_ptr<Marshallable>& cmd,
                                   const function<void()>& func,
                                   const function<void()>& exe_callback) {
  if (!IsLeader()) {
    //Log_fatal("i am not the leader; site %d; locale %d",
    //          frame_->site_info_->id, loc_id_);
    Forward(cmd, func, exe_callback) ;
    return ;
  }

  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!in_submission_);
  verify(cmd_ == nullptr);
//  verify(cmd.self_cmd_ != nullptr);
  in_submission_ = true;
  cmd_ = cmd;
  verify(cmd_->kind_ != MarshallDeputy::UNKNOWN);
  commit_callback_ = func;
  GotoNextPhase();
}

void CoordinatorFpgaRaft::AppendEntries() {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    verify(!in_append_entries);
    // verify(this->sch_->IsLeader()); TODO del it yidawu
    in_append_entries = true;
    Log_debug("fpga-raft coordinator broadcasts append entries, "
                  "par_id_: %lx, slot_id: %llx, lastLogIndex: %d",
              par_id_, slot_id_, this->sch_->lastLogIndex);
    /* Should we use slot_id instead of lastLogIndex and balot instead of term? */
    uint32_t prevLogIndex = this->sch_->lastLogIndex;

    this->sch_->lastLogIndex += 1;
    auto instance = this->sch_->GetFpgaRaftInstance(this->sch_->lastLogIndex);

    instance->log_ = cmd_;
    instance->term = this->sch_->currentTerm;

    /* TODO: get prevLogTerm based on the logs */
    uint64_t prevLogTerm = this->sch_->currentTerm;
retry:
    auto sp_quorum = commo()->BroadcastAppendEntries(par_id_,
                                                     slot_id_,
                                                     curr_ballot_,
                                                     this->sch_->IsLeader(),
                                                     this->sch_->currentTerm,
                                                     prevLogIndex,
                                                     prevLogTerm,
                                                     /* ents, */
                                                     this->sch_->commitIndex,
                                                     cmd_);
    sp_quorum->Wait();
    if (sp_quorum->Yes()) {
        minIndex = sp_quorum->minIndex;
        committed_ = true;
    }
    else if (sp_quorum->No()) {
        //verify(0);
        // TODO should become a follower if the term is smaller
        if(!IsLeader())
        {
            Forward(cmd_,commit_callback_) ;
        }
    }
    else {
        //verify(0);
        // TODO recheck
        goto retry ;
    }
}

void CoordinatorFpgaRaft::Commit() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  commit_callback_();
  Log_debug("fpga-raft broadcast commit for partition: %d, slot %d",
            (int) par_id_, (int) slot_id_);
  commo()->BroadcastDecide(par_id_, slot_id_, curr_ballot_, cmd_);
  verify(phase_ == Phase::COMMIT);
  GotoNextPhase();
}

void CoordinatorFpgaRaft::LeaderLearn() {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    commit_callback_();
    uint64_t prevCommitIndex = this->sch_->commitIndex;
    this->sch_->commitIndex = std::max(this->sch_->commitIndex, minIndex);
    Log_debug("fpga-raft commit for partition: %d, slot %d, commit %d in loc:%d",
              (int) par_id_, (int) slot_id_, sch_->commitIndex, loc_id_);

    /* if (prevCommitIndex < this->sch_->commitIndex) { */
    /*     auto instance = this->sch_->GetFpgaRaftInstance(this->sch_->commitIndex); */
    /*     this->sch_->app_next_(*instance->log_); */
    /* } */

    commo()->BroadcastDecide(par_id_, slot_id_, curr_ballot_, cmd_);
    verify(phase_ == Phase::COMMIT);
    GotoNextPhase();
}

void CoordinatorFpgaRaft::GotoNextPhase() {
  int n_phase = 4;
  int current_phase = phase_ % n_phase;
  phase_++;
  switch (current_phase) {
    case Phase::INIT_END:
      if (IsLeader()) {
        phase_++; // skip prepare phase for "leader"
        verify(phase_ % n_phase == Phase::ACCEPT);
        //Accept();
        AppendEntries();
        phase_++;
        verify(phase_ % n_phase == Phase::COMMIT);
      } else {
        // TODO
        //verify(0);
        Forward(cmd_,commit_callback_) ;
        phase_ = Phase::COMMIT;
      }
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      if (committed_) {
        //Commit();
        LeaderLearn();
      } else {
        //verify(0);
        Forward(cmd_,commit_callback_) ;
        phase_ = Phase::COMMIT;
      }
      break;
    case Phase::PREPARE:
      verify(phase_ % n_phase == Phase::ACCEPT);
      /* Accept(); */
      AppendEntries();
      break;
    case Phase::COMMIT:
      // do nothing.
      break;
    default:
      verify(0);
  }
}

} // namespace janus
