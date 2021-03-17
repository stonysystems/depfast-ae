
#include "service.h"
#include "server.h"

namespace janus {

FpgaRaftServiceImpl::FpgaRaftServiceImpl(TxLogServer *sched)
    : sched_((FpgaRaftServer*)sched) {

}

void FpgaRaftServiceImpl::Heartbeat(const uint64_t& leaderPrevLogIndex,
																		const DepId& dep_id,
																		uint64_t* followerPrevLogIndex,
																		rrr::DeferredReply* defer) {
	Log_info("received heartbeat");
	*followerPrevLogIndex = sched_->lastLogIndex;
	defer->reply();
}

void FpgaRaftServiceImpl::Forward(const MarshallDeputy& cmd,
                                    uint64_t* cmt_idx, 
                                    rrr::DeferredReply* defer) {
   verify(sched_ != nullptr);
   sched_->OnForward(const_cast<MarshallDeputy&>(cmd).sp_data_, cmt_idx,
                      std::bind(&rrr::DeferredReply::reply, defer));

}

void FpgaRaftServiceImpl::Vote(const uint64_t& lst_log_idx,
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

void FpgaRaftServiceImpl::Vote2FPGA(const uint64_t& lst_log_idx,
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


void FpgaRaftServiceImpl::AppendEntries(const uint64_t& slot,
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

void FpgaRaftServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
																	 const DepId& dep_id,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
	Log_info("Deciding with string: %s and id: %d", dep_id.str.c_str(), dep_id.id);
  Coroutine::CreateRun([&] () {
    sched_->OnCommit(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_);
		
		struct timespec begin, end;
    defer->reply();
  });
}


} // namespace janus;
