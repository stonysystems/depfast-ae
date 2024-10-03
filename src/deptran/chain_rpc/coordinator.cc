
#include "../__dep__.h"
#include "../constants.h"
#include "coordinator.h"
#include "commo.h"
#include "server.h"
#include <chrono>

namespace janus {

CoordinatorChainRPC::CoordinatorChainRPC(uint32_t coo_id,
                                             int32_t benchmark,
                                             ClientControlServiceImpl* ccsi,
                                             uint32_t thread_id)
    : Coordinator(coo_id, benchmark, ccsi, thread_id) {
}

bool CoordinatorChainRPC::IsLeader() {
   return this->sch_->IsLeader() ;
}

bool CoordinatorChainRPC::IsFPGALeader() {
   return this->sch_->IsFPGALeader() ;
}

void CoordinatorChainRPC::Forward(shared_ptr<Marshallable>& cmd,
                                   const function<void()>& func,
                                   const function<void()>& exe_callback) {
    //for(int i = 0; i < 100; i++) Log_info("inside forward");
		verify(0) ; // TODO delete it
    auto e = commo()->SendForward(par_id_, loc_id_, cmd);
    e->Wait();
    uint64_t cmt_idx = e->CommitIdx() ;
    cmt_idx_ = cmt_idx ;
    Coroutine::CreateRun([&] () {
      this->sch_->SpCommit(cmt_idx) ;
    }) ;
}


void CoordinatorChainRPC::Submit(shared_ptr<Marshallable>& cmd,
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

// One coordinator instance per concurrency.
void CoordinatorChainRPC::AppendEntries() {
    std::lock_guard<std::recursive_mutex> lock(mtx_); // This looks like unnecessary, especially when we ave Wait() in the loop
    verify(!in_append_entries);
    // verify(this->sch_->IsLeader()); TODO del it yidawu
    in_append_entries = true;
    Log_track("fpga-raft coordinator broadcasts append entries, "
                  "par_id_: %lu, slot_id: %llu, lastLogIndex: %d",
              par_id_, slot_id_, this->sch_->lastLogIndex);
    /* Should we use slot_id instead of lastLogIndex and balot instead of term? */
    uint64_t prevLogIndex = this->sch_->lastLogIndex;

    /* TODO: get prevLogTerm based on the logs */
    uint64_t prevLogTerm = this->sch_->currentTerm;
		this->sch_->SetLocalAppend(cmd_, &prevLogTerm, &prevLogIndex, slot_id_, curr_ballot_) ;
    auto sp_quorum = commo()->BroadcastAppendEntries(par_id_,
                                                     this->sch_->site_id_,
                                                     slot_id_,
                                                     dep_id_,
                                                     curr_ballot_,
                                                     this->sch_->IsLeader(),
                                                     this->sch_->currentTerm,
                                                     prevLogIndex,
                                                     prevLogTerm,
                                                     /* ents, */
                                                     this->sch_->commitIndex,
                                                     cmd_);

		auto start = std::chrono::high_resolution_clock::now();
    sp_quorum->Wait(1000*1000);
    Log_track("Completed waiting for append entries");
		std::chrono::duration<double, std::nano> duration = std::chrono::high_resolution_clock::now() - start; // in nanoseconds
    Log_track("Time of append entries on coordinator: %f ms, path_id: %d, uniq_id_:%d", duration.count()/1000.0/1000.0, sp_quorum->ongoingPickedPath, sp_quorum->uniq_id_);

#ifdef CHAIN_RPC_ENABLED
    commo()->updateResponseTime(par_id_, duration.count());
    commo()->updatePathWeights(par_id_, sp_quorum->ongoingPickedPath, duration.count());
#endif

    if (sp_quorum->Yes()) {
        minIndex = sp_quorum->minIndex;
				//Log_info("sp_quorum: minIndex %d vs %d", minIndex, this->sch_->commitIndex);
        //verify(minIndex >= this->sch_->commitIndex) ;
        committed_ = true;
        Log_debug("fpga-raft append commited loc:%d minindex:%d", loc_id_, minIndex) ;
    }
    else if (sp_quorum->No()) {
        Log_info("failed to have a quorum for append entries, uniq_id_:%d", sp_quorum->uniq_id_);
        verify(0);
    }
    else { // TODO: if timeout, we retry it in different backward path.
        verify(0);
    }
}

void CoordinatorChainRPC::Commit() {
  verify(0);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  commit_callback_();
  Log_debug("fpga-raft broadcast commit for partition: %d, slot %d",
            (int) par_id_, (int) slot_id_);
  commo()->BroadcastDecide(par_id_, slot_id_, dep_id_, curr_ballot_, cmd_);
  verify(phase_ == Phase::COMMIT);
  GotoNextPhase();
}

void CoordinatorChainRPC::LeaderLearn() {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    commit_callback_();
    uint64_t prevCommitIndex = this->sch_->commitIndex;
    //verify(minIndex >= prevCommitIndex);
    this->sch_->commitIndex = std::max(this->sch_->commitIndex, minIndex);
    Log_debug("fpga-raft commit for partition: %d, slot %d, commit %d minIndex %d in loc:%d", 
      (int) par_id_, (int) slot_id_, sch_->commitIndex, minIndex, loc_id_);

    /* if (prevCommitIndex < this->sch_->commitIndex) { */
    /*     auto instance = this->sch_->GetChainRPCInstance(this->sch_->commitIndex); */
    /*     this->sch_->app_next_(*instance->log_); */
    /* } */

    // Not necessary to carray cmd_ in both AppendEntries and Commit. We removed it from the Commit RPC.
    auto dummy = make_shared<CmdData>();
    auto dummy_cmd = dynamic_pointer_cast<Marshallable>(dummy);
    commo()->BroadcastDecide(par_id_, slot_id_, dep_id_, curr_ballot_, dummy_cmd);
    verify(phase_ == Phase::COMMIT);
    GotoNextPhase();
}

void CoordinatorChainRPC::GotoNextPhase() {
  int n_phase = 4;
  int current_phase = phase_ % n_phase;
  phase_++;
  switch (current_phase) {
    case Phase::INIT_END:
      if (IsLeader()) {
        phase_++; // skip prepare phase for "leader"
        verify(phase_ % n_phase == Phase::ACCEPT);
        AppendEntries();
        phase_++;
        verify(phase_ % n_phase == Phase::COMMIT);
      } else {
        // TODO
        verify(0);
        Forward(cmd_,commit_callback_) ;
        phase_ = Phase::COMMIT;
      }
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      if (committed_) {
        LeaderLearn();
      } else {
        // verify(0);
        // Forward(cmd_,commit_callback_) ;
        phase_ = Phase::COMMIT;
      }
      break;
    case Phase::PREPARE:
      verify(phase_ % n_phase == Phase::ACCEPT);
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
