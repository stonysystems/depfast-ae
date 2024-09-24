#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"

namespace janus {
class Command;
class CmdData;

#define INVALID_PARID  ((parid_t)-1)
#define NUM_BATCH_TIMER_RESET  (100)
#define SEC_BATCH_TIMER_RESET  (1)

struct ChainRPCData {
  ballot_t max_ballot_seen_ = 0;
  ballot_t max_ballot_accepted_ = 0;
  shared_ptr<Marshallable> accepted_cmd_{nullptr};
  shared_ptr<Marshallable> committed_cmd_{nullptr};

  ballot_t term;
  shared_ptr<Marshallable> log_{nullptr};

	//for retries
	ballot_t prevTerm;
	slotid_t slot_id;
	ballot_t ballot;
};

struct KeyValue {
	int key;
	i32 value;
};

class ChainRPCServer : public TxLogServer {
 private:
   std::vector<std::thread> timer_threads_ = {};
  void timer_thread(bool *vote) ;
  Timer *timer_;
  bool stop_ = false ;
  parid_t vote_for_ = INVALID_PARID ;
  bool init_ = false ;
  bool is_leader_ = false ;
  bool fpga_is_leader_ = false ;  
  uint64_t fpga_lastLogIndex = 0;
  uint64_t fpga_commitIndex = 0;
  slotid_t snapidx_ = 0 ;
  ballot_t snapterm_ = 0 ;
  int32_t wait_int_ = 1 * 1000 * 1000 ; // 1s
  bool paused_ = false ;
  bool req_voting_ = false ;
  bool in_applying_logs_ = false ;
  bool failover_{false} ;
  atomic<int64_t> counter_{0};
  const char *filename = "/db/data.txt";

	static bool looping;
	bool heartbeat_ = false;
	enum { STOPPED, RUNNING } status_;
	pthread_t loop_th_;
  
	bool RequestVote() ;
  void RequestVote2FPGA() ;

	void Setup();
	static void* HeartbeatLoop(void* args) ;
  
	void setIsLeader(bool isLeader)
  {
    Log_debug("set loc_id %d is leader %d", loc_id_, isLeader) ;
    is_leader_ = isLeader ;
  }

  void setIsFPGALeader(bool isLeader)
  {
    Log_debug("set loc_id %d is fpga leader %d", loc_id_, isLeader) ;
    fpga_is_leader_ = isLeader ;

    if (isLeader) 
    {
      fpga_lastLogIndex = lastLogIndex ;
      fpga_commitIndex = commitIndex ;
    }
    
  }
  
  void doVote(const slotid_t& lst_log_idx,
                            const ballot_t& lst_log_term,
                            const parid_t& can_id,
                            const ballot_t& can_term,
                            ballot_t *reply_term,
                            bool_t *vote_granted,
                            bool_t vote,
                            const function<void()> &cb) {
      *vote_granted = vote ;
      *reply_term = currentTerm ;
      Log_debug("loc %d vote decision %d, for can_id %d canterm %d curterm %d isleader %d lst_log_idx %d lst_log_term %d", 
            loc_id_, vote, can_id, can_term, currentTerm, is_leader_, lst_log_idx, lst_log_term );
                    
      if( can_term > currentTerm)
      {
          // is_leader_ = false ;  // TODO recheck
          currentTerm = can_term ;
      }

      if(vote)
      {
          setIsLeader(false) ;
          vote_for_ = can_id ;
          //reset timeout
          //resetTimer() ;
      }
      n_vote_++ ;
      cb() ;
  }

  void resetTimerBatch()
  {
    if (!failover_) return ;
    auto cur_count = counter_++;
    if (cur_count > NUM_BATCH_TIMER_RESET ) {
      if (timer_->elapsed() > SEC_BATCH_TIMER_RESET) {
        resetTimer();
      }
      counter_.store(0);
    }
  }

  void resetTimer() {
    if (failover_) timer_->start() ;
  }

  int32_t randDuration() 
  {
    return 4 + RandomGenerator::rand(0, 6) ;
  }
 public:
  slotid_t min_active_slot_ = 1; // anything before (lt) this slot is freed
  slotid_t max_executed_slot_ = 0;
  slotid_t max_committed_slot_ = 0;
  map<slotid_t, shared_ptr<ChainRPCData>> logs_{};
  int n_vote_ = 0;
  int n_prepare_ = 0;
  int n_accept_ = 0;
  int n_commit_ = 0;

  /* NOTE: I think I should move these to the ChainRPCData class */
  /* TODO: talk to Shuai about it */
  uint64_t lastLogIndex = 0;
  uint64_t currentTerm = 0;
  uint64_t commitIndex = 0;
  uint64_t executeIndex = 0;
  map<slotid_t, shared_ptr<ChainRPCData>> raft_logs_{};
//  vector<shared_ptr<ChainRPCData>> raft_logs_{};

  void StartTimer() ;

  bool IsLeader()
  {
    return is_leader_ ;
  }

  bool IsFPGALeader()
  {
    return fpga_is_leader_ ;
  }
  
  void SetLocalAppend(shared_ptr<Marshallable>& cmd, uint64_t* term, uint64_t* index, slotid_t slot_id = -1, ballot_t ballot = 1 ){
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    *index = lastLogIndex ;
    lastLogIndex += 1;
    auto instance = GetChainRPCInstance(lastLogIndex);
    instance->log_ = cmd;
		instance->prevTerm = currentTerm;
    instance->term = currentTerm;
		instance->slot_id = slot_id;
		instance->ballot = ballot;

    if (cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT){
      auto p_cmd = dynamic_pointer_cast<TpcCommitCommand>(cmd);
      auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(p_cmd->cmd_)->sp_vec_piece_data_;
			vector<struct KeyValue> kv_vector;
			int index = 0;
			for (auto it = sp_vec_piece->begin(); it != sp_vec_piece->end(); it++){
				auto cmd_input = (*it)->input.values_;
				for (auto it2 = cmd_input->begin(); it2 != cmd_input->end(); it2++) {
					struct KeyValue key_value = {it2->first, it2->second.get_i32()};
					kv_vector.push_back(key_value);
				}
			}

			struct KeyValue key_values[kv_vector.size()];
			std::copy(kv_vector.begin(), kv_vector.end(), key_values);

			auto de = IO::write(filename, key_values, sizeof(struct KeyValue), kv_vector.size());
			
			struct timespec begin, end;
			//clock_gettime(CLOCK_MONOTONIC, &begin);
      de->Wait();
			//clock_gettime(CLOCK_MONOTONIC, &end);
			//Log_info("Time of Write: %d", end.tv_nsec - begin.tv_nsec);
    } else {
			int value = -1;
			int value_;
			// auto de = IO::write(filename, &value, sizeof(int), 1);
			struct timespec begin, end;
			//clock_gettime(CLOCK_MONOTONIC, &begin);
      // de->Wait();
			//clock_gettime(CLOCK_MONOTONIC, &end);
			//Log_info("Time of Write: %d", end.tv_nsec - begin.tv_nsec);
    }
    *term = currentTerm ;
  }
  
  shared_ptr<ChainRPCData> GetInstance(slotid_t id) {
    verify(id >= min_active_slot_);
    auto& sp_instance = logs_[id];
    if(!sp_instance)
      sp_instance = std::make_shared<ChainRPCData>();
    return sp_instance;
  }

 /* shared_ptr<ChainRPCData> GetChainRPCInstance(slotid_t id) {
    if ( id <= raft_logs_.size() )
    {
        return raft_logs_[id-1] ;
    }
    auto sp_instance = std::make_shared<ChainRPCData>();
    raft_logs_.push_back(sp_instance) ;
    return sp_instance;
  }*/
   shared_ptr<ChainRPCData> GetChainRPCInstance(slotid_t id) {
     verify(id >= min_active_slot_);
     auto& sp_instance = raft_logs_[id];
     if(!sp_instance)
       sp_instance = std::make_shared<ChainRPCData>();
     return sp_instance;
   }


  ChainRPCServer(Frame *frame) ;
  ~ChainRPCServer() ;

  void OnVote(const slotid_t& lst_log_idx,
                      const ballot_t& lst_log_term,
                      const parid_t& can_id,
                      const ballot_t& can_term,
                      ballot_t *reply_term,
                      bool_t *vote_granted,
                      const function<void()> &cb) ;

  void OnVote2FPGA(const slotid_t& lst_log_idx,
                      const ballot_t& lst_log_term,
                      const parid_t& can_id,
                      const ballot_t& can_term,
                      ballot_t *reply_term,
                      bool_t *vote_granted,
                      const function<void()> &cb) ;


  void OnAppendEntries(const slotid_t slot_id,
                       const ballot_t ballot,
                       const uint64_t leaderCurrentTerm,
                       const uint64_t leaderPrevLogIndex,
                       const uint64_t leaderPrevLogTerm,
                       const uint64_t leaderCommitIndex,
											 const struct DepId dep_id,
                       shared_ptr<Marshallable> &cmd,
                       uint64_t *followerAppendOK,
                       uint64_t *followerCurrentTerm,
                       uint64_t *followerLastLogIndex,
                       const function<void()> &cb);

  void OnCommit(const slotid_t slot_id,
                const ballot_t ballot,
                shared_ptr<Marshallable> &cmd);

  void OnForward(shared_ptr<Marshallable> &cmd, 
                          uint64_t *cmt_idx,
                          const function<void()> &cb) ;

  void SpCommit(const uint64_t cmt_idx) ;

  virtual void Pause() override { 
    paused_ = true ;
  }
  virtual void Resume() override {
    paused_ = false ;
    resetTimer() ;
  }

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };

  void removeCmd(slotid_t slot);
};
} // namespace janus
