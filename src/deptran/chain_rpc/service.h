#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

class SimpleCommand;
namespace janus {

class TxLogServer;
class ChainRPCServer;
class ChainRPCServiceImpl : public ChainRPCService {
 public:
  ChainRPCServer* sched_;
  ChainRPCServiceImpl(TxLogServer* sched);
	void Heartbeat(const uint64_t& leaderPrevLogIndex,
								 const DepId& dep_id,
								 uint64_t* followerPrevLogIndex,
								 rrr::DeferredReply* defer) override;
  void Forward(const MarshallDeputy& cmd,
               uint64_t *cmt_idx,
               rrr::DeferredReply* defer) override;

  void Vote(const uint64_t& lst_log_idx,
                  const ballot_t& lst_log_term,
                  const parid_t& can_id,
                  const ballot_t& can_term,
                  ballot_t* reply_term,
                  bool_t *vote_granted,
                  rrr::DeferredReply* defer) override;

  void Vote2FPGA(const uint64_t& lst_log_idx,
                  const ballot_t& lst_log_term,
                  const parid_t& can_id,
                  const ballot_t& can_term,
                  ballot_t* reply_term,
                  bool_t *vote_granted,
                  rrr::DeferredReply* defer) override;

	void AppendEntries2(const uint64_t& slot,
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
                      rrr::DeferredReply* defer);
  
	void AppendEntries(const uint64_t& slot,
                     const ballot_t& ballot,
                     const uint64_t& leaderCurrentTerm,
                     const uint64_t& leaderPrevLogIndex,
                     const uint64_t& leaderPrevLogTerm,
                     const uint64_t& leaderCommitIndex,
										 const DepId& dep_id,
                     const MarshallDeputy& cmd,
                     uint64_t *followerAppendOK,
                     uint64_t *followerCurrentTerm,
                     uint64_t *followerLastLogIndex,
                     rrr::DeferredReply* defer) override;

void AppendEntriesChain(const uint64_t& slot,
                     const ballot_t& ballot,
                     const uint64_t& leaderCurrentTerm,
                     const uint64_t& leaderPrevLogIndex,
                     const uint64_t& leaderPrevLogTerm,
                     const uint64_t& leaderCommitIndex,
										 const DepId& dep_id,
                     const MarshallDeputy& cmd,
                     const MarshallDeputy& uc,
                     uint64_t *followerAppendOK,
                     uint64_t *followerCurrentTerm,
                     uint64_t *followerLastLogIndex,
                     rrr::DeferredReply* defer) override;

  void AccBack2LeaderChain(const uint64_t& slot, 
                     const ballot_t& ballot, 
                     const MarshallDeputy& uc, 
                     rrr::DeferredReply* defer) override;

  void Decide(const uint64_t& slot,
              const ballot_t& ballot,
							const DepId& dep_id,
              const MarshallDeputy& cmd,
              rrr::DeferredReply* defer) override;

};

} // namespace janus
