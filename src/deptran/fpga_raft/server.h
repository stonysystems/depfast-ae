#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"

namespace janus {
class Command;
class CmdData;

#define INVALID_PARID  ((parid_t)-1)

struct FpgaRaftData {
  ballot_t max_ballot_seen_ = 0;
  ballot_t max_ballot_accepted_ = 0;
  shared_ptr<Marshallable> accepted_cmd_{nullptr};
  shared_ptr<Marshallable> committed_cmd_{nullptr};

  ballot_t term;
  shared_ptr<Marshallable> log_{nullptr};
};

class FpgaRaftServer : public TxLogServer {
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

  bool RequestVote() ;
  void RequestVote2FPGA() ;

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

  void resetTimer()
  {
      timer_->start() ;
  }

  int32_t randDuration() 
  {
    return 4 + RandomGenerator::rand(0, 6) ;
  }
  
 public:
  slotid_t min_active_slot_ = 0; // anything before (lt) this slot is freed
  slotid_t max_executed_slot_ = 0;
  slotid_t max_committed_slot_ = 0;
  map<slotid_t, shared_ptr<FpgaRaftData>> logs_{};
  int n_vote_ = 0;
  int n_prepare_ = 0;
  int n_accept_ = 0;
  int n_commit_ = 0;

  /* NOTE: I think I should move these to the FpgaRaftData class */
  /* TODO: talk to Shuai about it */
  uint64_t lastLogIndex = 0;
  uint64_t currentTerm = 0;
  uint64_t commitIndex = 0;
  uint64_t executeIndex = 0;
  map<slotid_t, shared_ptr<FpgaRaftData>> raft_logs_{};
//  vector<shared_ptr<FpgaRaftData>> raft_logs_{};

  void StartTimer() ;

  bool IsLeader()
  {
    return is_leader_ ;
  }

  bool IsFPGALeader()
  {
    return fpga_is_leader_ ;
  }
  
  void SetLocalAppend(shared_ptr<Marshallable>& cmd, uint64_t* term, uint64_t* index ){
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    *index = lastLogIndex ;
    lastLogIndex += 1;
    auto instance = GetFpgaRaftInstance(lastLogIndex);
    instance->log_ = cmd;
    instance->term = currentTerm;
    if (cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE){
      auto p_cmd = dynamic_pointer_cast<TpcPrepareCommand>(cmd);
      auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(p_cmd->cmd_)->sp_vec_piece_data_;
      map<int, i32> key_values {};
      for(auto it = sp_vec_piece->begin(); it != sp_vec_piece->end(); it++){
        auto cmd_input = (*it)->input.values_;
        for(auto it2 = cmd_input->begin(); it2 != cmd_input->end(); it2++){
          key_values[it2->first] = it2->second.get_i64();
        }
      }
              
      auto de = Reactor::CreateSpEvent<DiskEvent>(key_values);
      de->AddToList();
      de->Wait();
    }
    *term = currentTerm ;
  }
  
  shared_ptr<FpgaRaftData> GetInstance(slotid_t id) {
		//for(int i = 0; i < 100; i++) Log_info("what is id?: %d", id);
    verify(id >= min_active_slot_);
    auto& sp_instance = logs_[id];
    if(!sp_instance)
      sp_instance = std::make_shared<FpgaRaftData>();
    return sp_instance;
  }

 /* shared_ptr<FpgaRaftData> GetFpgaRaftInstance(slotid_t id) {
    if ( id <= raft_logs_.size() )
    {
        return raft_logs_[id-1] ;
    }
    auto sp_instance = std::make_shared<FpgaRaftData>();
    raft_logs_.push_back(sp_instance) ;
    return sp_instance;
  }*/
   shared_ptr<FpgaRaftData> GetFpgaRaftInstance(slotid_t id) {
     verify(id >= min_active_slot_);
     auto& sp_instance = raft_logs_[id];
     if(!sp_instance)
       sp_instance = std::make_shared<FpgaRaftData>();
     return sp_instance;
   }


  FpgaRaftServer(Frame *frame) ;
  ~FpgaRaftServer() ;

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
};
} // namespace janus
