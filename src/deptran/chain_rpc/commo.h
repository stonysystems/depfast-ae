#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../communicator.h"
#include <deque>
#include <chrono>

namespace janus {

class TxData;

class ChainRPCForwardQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  uint64_t CommitIdx()
  {
    return cmt_idx_ ;
  }
  void FeedResponse(uint64_t cmt_idx) {
    VoteYes();
    cmt_idx_ = cmt_idx ;
  }
};

class ChainRPCPrepareQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
//  ballot_t max_ballot_{0};
  bool HasAcceptedValue() {
    // TODO implement this
    return false;
  }
  void FeedResponse(bool y) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
    }
  }
};

class ChainRPCVoteQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  bool HasAcceptedValue() {
    return false;
  }
  void FeedResponse(bool y, ballot_t term) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
      if(term > highest_term_)
      {
        highest_term_ = term ;
      }      
    }
  }
  
  int64_t Term() {
    return highest_term_;
  }
};

class ChainRPCVote2FPGAQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  bool HasAcceptedValue() {
    return false;
  }
  void FeedResponse(bool y, ballot_t term) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
      if(term > highest_term_)
      {
        highest_term_ = term ;
      }      
    }
  }
  
  int64_t Term() {
    return highest_term_;
  }
};

class ChainRPCAcceptQuorumEvent: public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  void FeedResponse(bool y) {
    if (y) {
      VoteYes();
    } else {
      VoteNo();
    }
    /*Log_debug("multi-paxos comm accept event, "
              "yes vote: %d, no vote: %d",
              n_voted_yes_, n_voted_no_);*/
  }
};

class ChainRPCAppendQuorumEvent: public QuorumEvent {
 public:
    uint64_t minIndex;
    using QuorumEvent::QuorumEvent;

    // A message between commo.cpp and coordinator.cc
    int ongoingPickedPath = -1;
    // uuid_ for a path (Debugging purpose)
    //std::string uuid_ = "";
    int uniq_id_ = -1;

    void FeedResponse(bool appendOK, uint64_t index, std::string ip_addr = "") {
        if (appendOK) {
            if ((n_voted_yes_ == 0) && (n_voted_no_ == 0))
                minIndex = index;
            else
                minIndex = std::min(minIndex, index);
            VoteYes();
        } else {
            VoteNo();
        }
        /*Log_debug("fpga-raft comm accept event, "
                  "yes vote: %d, no vote: %d, min index: %d",
                  n_voted_yes_, n_voted_no_, minIndex);*/
    }
};



class ChainRPCCommo : public Communicator {

friend class ChainRPCProxy;
 public:
	std::unordered_map<siteid_t, uint64_t> matchedIndex {};
	int index;
	
  ChainRPCCommo() = delete;
  ChainRPCCommo(PollMgr*);
  shared_ptr<ChainRPCForwardQuorumEvent>
  SendForward(parid_t par_id, parid_t self_id, shared_ptr<Marshallable> cmd);  
	void BroadcastHeartbeat(parid_t par_id,
													uint64_t logIndex);
	void SendHeartbeat(parid_t par_id,
										 siteid_t site_id,
										 uint64_t logIndex);
	//ONLY FOR SIMULATION
  void SendAppendEntriesAgain(siteid_t site_id,
															parid_t par_id,
															slotid_t slot_id,
															ballot_t ballot,
															bool isLeader,
															uint64_t currentTerm,
															uint64_t prevLogIndex,
															uint64_t prevLogTerm,
															uint64_t commitIndex,
															shared_ptr<Marshallable> cmd);
  shared_ptr<ChainRPCPrepareQuorumEvent>
  BroadcastPrepare(parid_t par_id,
                   slotid_t slot_id,
                   ballot_t ballot);
  void BroadcastPrepare(parid_t par_id,
                        slotid_t slot_id,
                        ballot_t ballot,
                        const function<void(Future *fu)> &callback);
  shared_ptr<ChainRPCVoteQuorumEvent>
  BroadcastVote(parid_t par_id,
                        slotid_t lst_log_idx,
                        ballot_t lst_log_term,
                        parid_t self_id,
                        ballot_t cur_term );
  void BroadcastVote(parid_t par_id,
                        slotid_t lst_log_idx,
                        ballot_t lst_log_term,
                        parid_t self_id,
                        ballot_t cur_term,
                        const function<void(Future *fu)> &callback);  
  shared_ptr<ChainRPCVote2FPGAQuorumEvent>
  BroadcastVote2FPGA(parid_t par_id,
                        slotid_t lst_log_idx,
                        ballot_t lst_log_term,
                        parid_t self_id,
                        ballot_t cur_term );
  void BroadcastVote2FPGA(parid_t par_id,
                        slotid_t lst_log_idx,
                        ballot_t lst_log_term,
                        parid_t self_id,
                        ballot_t cur_term,
                        const function<void(Future *fu)> &callback);  
  shared_ptr<ChainRPCAcceptQuorumEvent>
  BroadcastAccept(parid_t par_id,
                  slotid_t slot_id,
                  ballot_t ballot,
                  shared_ptr<Marshallable> cmd);
  void BroadcastAccept(parid_t par_id,
                       slotid_t slot_id,
                       ballot_t ballot,
                       shared_ptr<Marshallable> cmd,
                       const function<void(Future*)> &callback);
  shared_ptr<ChainRPCAppendQuorumEvent>
  BroadcastAppendEntries(parid_t par_id,
                         siteid_t leader_site_id,
                         slotid_t slot_id,
                         i64 dep_id,
                         ballot_t ballot,
                         bool isLeader,
                         uint64_t currentTerm,
                         uint64_t prevLogIndex,
                         uint64_t prevLogTerm,
                         uint64_t commitIndex,
                         shared_ptr<Marshallable> cmd);
  void BroadcastAppendEntries(parid_t par_id,
                              slotid_t slot_id,
															i64 dep_id,
                              ballot_t ballot,
                              uint64_t currentTerm,
                              uint64_t prevLogIndex,
                              uint64_t prevLogTerm,
                              uint64_t commitIndex,
                              shared_ptr<Marshallable> cmd,
                              const function<void(Future*)> &callback);
  void BroadcastDecide(const parid_t par_id,
                       const slotid_t slot_id,
											 const i64 dep_id,
                       const ballot_t ballot,
                       const shared_ptr<Marshallable> cmd);


  // Utility functions
  // Execute on the leader
  // par_id: [{path:[loc_id -> loc_id -> ...], weight:double},...]
  unordered_map<parid_t, vector<std::tuple<vector<int>, double>>> pathsWeights;
  // We keep recent response times for each path for updating the weights.
  // par_id: [{path: (availIndex, hist_data)}]
  unordered_map<parid_t, vector<std::tuple<int,vector<uint64_t>>>> pathResponeTime_ ;
  chrono::time_point<chrono::high_resolution_clock> initializtion_time;

  // I used many shared variables, that's the problem
  // Update the response time of the path.
  std::recursive_mutex mtx_{};
  void _initializePathResponseTime();
  void appendResponseTime(int par_id, int i, uint64_t latency) ;
  void _preAllocatePathsWithWeights();
  int getNextAvailablePath(int par_id) ;
  // According to the responsiveness of current path, update the weights of the paths
  void updatePathWeights(int par_id, uint64_t, int i, uint64_t response_time) ;
  
  int availablePath;
  atomic<int> retry_rpc_cnt = {0};
  atomic<int> received_quorum_ok_cnt = {0};
  atomic<int> received_quorum_fail_cnt = {0};

  // Event mapping in ChainRPC
  unordered_map<int, shared_ptr<ChainRPCAppendQuorumEvent>> event_append_map_{};
  using AppendParametersTuple = std::tuple<
    uint64_t,                  // slot_id 0
    ballot_t,                  // ballot 1
    uint64_t,                  // currentTerm 2
    uint64_t,        // prevLogIndex 3
    uint64_t,        // prevLogTerm 4
    uint64_t,        // commitIndex 5
    DepId,         // di 6
    MarshallDeputy // md 7
  >; 
  // Request mapping in ChainRPC for resend/just references
  unordered_map<int, AppendParametersTuple> data_append_map_{};

  void Statistics() const override;
};

} // namespace janus

