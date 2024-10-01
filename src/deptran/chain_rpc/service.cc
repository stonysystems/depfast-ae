
#include "service.h"
#include "server.h"
#include "utils.h"

namespace janus {

ChainRPCServiceImpl::ChainRPCServiceImpl(TxLogServer *sched)
    : sched_((ChainRPCServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}

void ChainRPCServiceImpl::Heartbeat(const uint64_t& leaderPrevLogIndex,
																		const DepId& dep_id,
																		uint64_t* followerPrevLogIndex,
																		rrr::DeferredReply* defer) {
	//Log_info("received heartbeat");
	*followerPrevLogIndex = sched_->lastLogIndex;
	defer->reply();
}

void ChainRPCServiceImpl::Forward(const MarshallDeputy& cmd,
                                    uint64_t* cmt_idx, 
                                    rrr::DeferredReply* defer) {
   verify(sched_ != nullptr);
   sched_->OnForward(const_cast<MarshallDeputy&>(cmd).sp_data_, cmt_idx,
                      std::bind(&rrr::DeferredReply::reply, defer));

}

void ChainRPCServiceImpl::Vote(const uint64_t& lst_log_idx,
                                    const ballot_t& lst_log_term,
                                    const parid_t& can_id,
                                    const ballot_t& can_term,
                                    ballot_t* reply_term,
                                    bool_t *vote_granted,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnVote(lst_log_idx,lst_log_term, can_id, can_term,
                    reply_term, vote_granted,
                    std::bind(&rrr::DeferredReply::reply, defer));
}

void ChainRPCServiceImpl::Vote2FPGA(const uint64_t& lst_log_idx,
                                    const ballot_t& lst_log_term,
                                    const parid_t& can_id,
                                    const ballot_t& can_term,
                                    ballot_t* reply_term,
                                    bool_t *vote_granted,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnVote2FPGA(lst_log_idx,lst_log_term, can_id, can_term,
                    reply_term, vote_granted,
                    std::bind(&rrr::DeferredReply::reply, defer));
}

void ChainRPCServiceImpl::AppendEntries(const uint64_t& slot,
                                        const ballot_t& ballot,
                                        const uint64_t& leaderCurrentTerm,
                                        const uint64_t& leaderPrevLogIndex,
                                        const uint64_t& leaderPrevLogTerm,
                                        const uint64_t& leaderCommitIndex,
																				const DepId& dep_id,
                                        const MarshallDeputy& md_cmd,
                                        uint64_t *followerAppendOK,
                                        uint64_t *followerCurrentTerm,
                                        uint64_t *followerLastLogIndex,
                                        rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
	//Log_info("CreateRunning2");


	/*if (ballot == 1000000000 || leaderPrevLogIndex + 1 < sched_->lastLogIndex) {
		*followerAppendOK = 1;
		*followerCurrentTerm = leaderCurrentTerm;
		*followerLastLogIndex = sched_->lastLogIndex + 1;
		/*for (int i = 0; i < 1000000; i++) {
			for (int j = 0; j < 1000; j++) {
				Log_info("wow: %d %d", leaderPrevLogIndex, sched_->lastLogIndex);
			}
		}
		defer->reply();
		return;
	}*/


  Coroutine::CreateRun([&] () {
    sched_->OnAppendEntries(slot,
                            ballot,
                            leaderCurrentTerm,
                            leaderPrevLogIndex,
                            leaderPrevLogTerm,
                            leaderCommitIndex,
														dep_id,
                            const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                            followerAppendOK,
                            followerCurrentTerm,
                            followerLastLogIndex,
                            std::bind(&rrr::DeferredReply::reply, defer));

  });
	
}

// Chain version AppendEntries on the receiver side
void ChainRPCServiceImpl::AppendEntriesChain(const uint64_t& slot,
                                        const ballot_t& ballot,
                                        const uint64_t& leaderCurrentTerm,
                                        const uint64_t& leaderPrevLogIndex,
                                        const uint64_t& leaderPrevLogTerm,
                                        const uint64_t& leaderCommitIndex,
																				const DepId& dep_id,
                                        const MarshallDeputy& md_cmd,
                                        const MarshallDeputy& cu_cmd,
                                        uint64_t *followerAppendOK,
                                        uint64_t *followerCurrentTerm,
                                        uint64_t *followerLastLogIndex,
                                        rrr::DeferredReply* defer) {
#ifdef IN_ORDER_ENABLED
    // Best-effort: in current implementation, we don't really do a retry for ChainRPC.
    auto cux = const_cast<MarshallDeputy&>(cu_cmd).sp_data_;
    auto cu_cmd_ptr = dynamic_pointer_cast<ControlUnit>(cux);
    while (leaderPrevLogIndex > 0) {
      if (sequencer_tracker_.find(leaderPrevLogIndex - 1) == sequencer_tracker_.end()) {
        Reactor::CreateSpEvent<NeverEvent>()->Wait(10);
      } else {
        break;
      }
    }
    sequencer_tracker_[leaderPrevLogIndex] = 1;
    sequencer_tracker_.erase(leaderPrevLogIndex - 1000);
#endif

    Coroutine::CreateRun([&] () {
      sched_->OnAppendEntriesChain(slot,
                            ballot,
                            leaderCurrentTerm,
                            leaderPrevLogIndex,
                            leaderPrevLogTerm,
                            leaderCommitIndex,
														dep_id,
                            const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                            const_cast<MarshallDeputy&>(cu_cmd).sp_data_,
                            followerAppendOK,
                            followerCurrentTerm,
                            followerLastLogIndex,
                            std::bind(&rrr::DeferredReply::reply, defer));

  });
}

// Replicas return acculumated results to the leader.
void ChainRPCServiceImpl::AccBack2LeaderChain(const uint64_t& slot, const ballot_t& ballot, const MarshallDeputy& uc, rrr::DeferredReply* defer) {
  
}

void ChainRPCServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
																	 const DepId& dep_id,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
	//Log_info("Deciding with string: %s and id: %d", dep_id.str.c_str(), dep_id.id);
  Coroutine::CreateRun([&] () {
    sched_->OnCommit(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_);
    defer->reply();
  });
}


} // namespace janus;
