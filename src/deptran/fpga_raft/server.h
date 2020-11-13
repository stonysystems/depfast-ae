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

struct KeyValue {
	int key;
	i32 value;
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

			struct KeyValue key_value_[2];
			auto de = IO::write("/db/data.txt", key_values, sizeof(struct KeyValue), kv_vector.size());
			
			struct timespec begin, end;
			clock_gettime(CLOCK_MONOTONIC, &begin);
      de->Wait();
			clock_gettime(CLOCK_MONOTONIC, &end);
			Log_info("Time of Write: %d", end.tv_nsec - begin.tv_nsec);
    } else {
			int value = -1;
			int value_;
			auto de = IO::write("/db/data.txt", &value, sizeof(int), 1);
			struct timespec begin, end;
			clock_gettime(CLOCK_MONOTONIC, &begin);
      de->Wait();
			clock_gettime(CLOCK_MONOTONIC, &end);
			Log_info("Time of Write: %d", end.tv_nsec - begin.tv_nsec);
    }
    *term = currentTerm ;

    save2file();    //TODO: just a sample
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


    template <class T>
    string val2string(const T& x){
        ostringstream obuff;
        obuff << x;
        return(obuff.str());
    };
    template <class T>
    T string2val(const string& x){
        T val;
        istringstream ibuff(x);
        ibuff >> val;
        return(val);
    };

    void save2file(){
        Log_info("Write yaml start");
        YAML::Node root;
        //assert(root.IsNull());

        root["currentTerm"]=val2string(currentTerm);
        root["vote_for_"]=val2string(vote_for_);
        YAML::Node _raft_logs_;
        int _raft_logs_cnt=0;

        for(map<slotid_t, shared_ptr<FpgaRaftData>>::iterator iter=raft_logs_.begin(); iter!=raft_logs_.end(); iter++){
            slotid_t key=iter->first;
            shared_ptr<FpgaRaftData> val=iter->second;
            YAML::Node _raft_logs_item;
            _raft_logs_item["term"]=val2string(val->term);

            //TODO: write key-val pairs
            shared_ptr<Marshallable> &cmd=val->log_;    // a command
            _raft_logs_item["logs_"]["kind_"]=val2string(val->log_->kind_);
            if(val->log_->kind_==MarshallDeputy::CMD_TPC_PREPARE){
                YAML::Node _kv_item;
                auto p_cmd = dynamic_pointer_cast<TpcPrepareCommand>(cmd);
                auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(p_cmd->cmd_)->sp_vec_piece_data_;
                int index = 0;
                for (auto it = sp_vec_piece->begin(); it != sp_vec_piece->end(); it++){
                    auto cmd_input = (*it)->input.values_;
                    for (auto it2 = cmd_input->begin(); it2 != cmd_input->end(); it2++) {
                        _kv_item[val2string(it2->first)]=val2string(it2->second.get_i32());
                    }
                }
                _raft_logs_item["logs_"]["kv"]=_kv_item;
            }

            root["raft_logs_"][val2string(key)]=_raft_logs_item;
            _raft_logs_cnt+=1;
        }

        ofstream ofs("/home/fl/FpgaRaftServerres.yaml");
        ofs<<root<<endl;
        ofs.close();
        Log_info("Write yaml end");
    }

    void read4file(){
        YAML::Node root = YAML::LoadFile("/home/fl/FpgaRaftServerres.yaml");

        currentTerm=string2val<uint64_t>(root["currentTerm"].as<string>());
        vote_for_=string2val<parid_t>(root["vote_for_"].as<string>());
        //for(auto i : root["raft_logs_"].getMemberNames()){
        for(YAML::const_iterator it= root["raft_logs_"].begin(); it != root["raft_logs_"].end();++it){
            slotid_t id=string2val<slotid_t>(it->first.as<string>());
            auto inst=GetFpgaRaftInstance(id);
            shared_ptr<Marshallable> cmd(new Marshallable(0));

            //TODO: read key-val pairs

            cmd->kind_=string2val<int32_t>(it->second["logs_"]["kind_"].as<string>());
            inst->log_= cmd;
            inst->term = string2val<ballot_t>(it->second["term"].as<string>());
        }
    }



    };
} // namespace janus
