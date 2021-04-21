

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"
#include "rdb/rocksdb_wrapper.h"

namespace janus {

bool FpgaRaftServer::looping = false;

struct hb_loop_args_type {
	FpgaRaftCommo* commo;
	FpgaRaftServer* sch;
};

FpgaRaftServer::FpgaRaftServer(Frame * frame) {
  frame_ = frame ;
  setIsFPGALeader(frame_->site_info_->locale_id == 0) ;
  setIsLeader(frame_->site_info_->locale_id == 0) ;
  stop_ = false ;
  timer_ = new Timer() ;
}

void FpgaRaftServer::Setup() {
	if (heartbeat_ && !FpgaRaftServer::looping && IsLeader()) {
		Log_info("starting loop at server");
		FpgaRaftServer::looping = true;
		memset(&loop_th_, 0, sizeof(loop_th_));
		hb_loop_args_type* hb_loop_args = new hb_loop_args_type();
		hb_loop_args->commo = (FpgaRaftCommo*) commo();
		hb_loop_args->sch = this;
		verify(hb_loop_args->commo && hb_loop_args->sch);
		Pthread_create(&loop_th_, nullptr, FpgaRaftServer::HeartbeatLoop, hb_loop_args);
	}
}

void* FpgaRaftServer::HeartbeatLoop(void* args) {
	hb_loop_args_type* hb_loop_args = (hb_loop_args_type*) args;

	FpgaRaftServer::looping = true;
	while(FpgaRaftServer::looping) {
		usleep(100*1000);
		uint64_t prevLogIndex = hb_loop_args->sch->lastLogIndex;	
		
		auto instance = hb_loop_args->sch->GetFpgaRaftInstance(prevLogIndex);
		auto term = instance->term;
		auto prevTerm = instance->prevTerm;
		auto ballot = instance->ballot;
		auto slot = instance->slot_id;
		shared_ptr<Marshallable> cmd = instance->log_;
		
		
		parid_t partition_id = hb_loop_args->sch->partition_id_;
		hb_loop_args->commo->BroadcastHeartbeat(partition_id, prevLogIndex);

		auto matcheds = hb_loop_args->commo->matchedIndex;
		for (auto it = matcheds.begin(); it != matcheds.end(); it++) {
			if (prevLogIndex > it->second + 10000 && cmd) {
				Log_info("leader_id: %d vs follower_id for %d: %d", prevLogIndex, it->first, it->second);
				//hb_loop_args->commo->SendHeartbeat(partition_id, it->first, prevLogIndex);
				hb_loop_args->commo->SendAppendEntriesAgain(it->first,
																				partition_id,
                                        slot,
                                        ballot,
                                        hb_loop_args->sch->IsLeader(),
                                        term,
                                        prevLogIndex,
                                        prevTerm,
                                        hb_loop_args->sch->commitIndex,
                                        cmd);
			}
		}
	}
	delete hb_loop_args;
	return nullptr;
}

FpgaRaftServer::~FpgaRaftServer() {
		if (heartbeat_ && FpgaRaftServer::looping) {
			FpgaRaftServer::looping = false;
			Pthread_join(loop_th_, nullptr);
		}
    
		stop_ = true ;
    Log_info("site par %d, loc %d: prepare %d, accept %d, commit %d", partition_id_, loc_id_, n_prepare_, n_accept_, 
    n_commit_);
}

void FpgaRaftServer::RequestVote2FPGA() {

  // currently don't request vote if no log
  if(this->commo_ == NULL || lastLogIndex == 0 ) return ;

  parid_t par_id = this->frame_->site_info_->partition_id_ ;
  parid_t loc_id = this->frame_->site_info_->locale_id ;

  if(paused_) {
      resetTimer() ;
      Log_debug("fpga raft server %d request vote to fpga rejected due to paused", loc_id );
      // req_voting_ = false ;
      return ;
  }

  Log_debug("fpga raft server %d in request vote to fpga", loc_id );

  uint32_t lstoff = 0  ;
  slotid_t lst_idx = 0 ;
  ballot_t lst_term = 0 ;

  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    // TODO set fpga isleader false, recheck 
    setIsFPGALeader(false) ;
    currentTerm++ ;
    lstoff = lastLogIndex - snapidx_ ;
    auto log = GetFpgaRaftInstance(lstoff) ;
    lst_idx = lstoff + snapidx_ ;
    lst_term = log->term ;
  }
  
  auto sp_quorum = ((FpgaRaftCommo *)(this->commo_))->BroadcastVote2FPGA(par_id,lst_idx,lst_term,loc_id, currentTerm );
  sp_quorum->Wait();
  std::lock_guard<std::recursive_mutex> lock1(mtx_);
  if (sp_quorum->Yes()) {
    // become a leader
    setIsFPGALeader(true) ;
    Log_debug("vote accepted %d curterm %d", loc_id, currentTerm);
  } else if (sp_quorum->No()) {
    // become a follower
    Log_debug("vote rejected %d", loc_id);
    setIsFPGALeader(false) ;
    //reset cur term if new term is higher
    ballot_t new_term = sp_quorum->Term() ;
    currentTerm = new_term > currentTerm? new_term : currentTerm ;
  } else {
    // TODO process timeout.
    Log_debug("vote timeout %d", loc_id);
  }
  req_voting_ = false ;
}

void FpgaRaftServer::OnVote2FPGA(const slotid_t& lst_log_idx,
                            const ballot_t& lst_log_term,
                            const parid_t& can_id,
                            const ballot_t& can_term,
                            ballot_t *reply_term,
                            bool_t *vote_granted,
                            const function<void()> &cb) {

  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("fpga raft receives vote from candidate: %llx", can_id);

  uint64_t cur_term = currentTerm ;
  if( can_term < cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  // has voted to a machine in the same term, vote no
  // TODO when to reset the vote_for_??
//  if( can_term == cur_term && vote_for_ != INVALID_PARID )
  if( can_term == cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  // lstoff starts from 1
  uint32_t lstoff = lastLogIndex - snapidx_ ;

  ballot_t curlstterm = snapterm_ ;
  slotid_t curlstidx = lastLogIndex ;

  if(lstoff > 0 )
  {
    auto log = GetFpgaRaftInstance(lstoff) ;
    curlstterm = log->term ;
  }

  Log_debug("vote for lstoff %d, curlstterm %d, curlstidx %d", lstoff, curlstterm, curlstidx  );


  // TODO del only for test 
  verify(lstoff == lastLogIndex ) ;

  if( lst_log_term > curlstterm || (lst_log_term == curlstterm && lst_log_idx >= curlstidx) )
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, true, cb) ;
    return ;
  }

  doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;

}


bool FpgaRaftServer::RequestVote() {
  for(int i = 0; i < 1000; i++) Log_info("not calling the wrong method");

  // currently don't request vote if no log
  if(this->commo_ == NULL || lastLogIndex == 0 ) return false;

  parid_t par_id = this->frame_->site_info_->partition_id_ ;
  parid_t loc_id = this->frame_->site_info_->locale_id ;


  if(paused_) {
      Log_debug("fpga raft server %d request vote rejected due to paused", loc_id );
      resetTimer() ;
      // req_voting_ = false ;
      return false;
  }

  Log_debug("fpga raft server %d in request vote", loc_id );

  uint32_t lstoff = 0  ;
  slotid_t lst_idx = 0 ;
  ballot_t lst_term = 0 ;

  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    // TODO set fpga isleader false, recheck 
    setIsFPGALeader(false) ;
    currentTerm++ ;
    lstoff = lastLogIndex - snapidx_ ;
    auto log = GetFpgaRaftInstance(lstoff) ;
    lst_idx = lstoff + snapidx_ ;
    lst_term = log->term ;
  }
  
  auto sp_quorum = ((FpgaRaftCommo *)(this->commo_))->BroadcastVote(par_id,lst_idx,lst_term,loc_id, currentTerm );
  sp_quorum->Wait();
  std::lock_guard<std::recursive_mutex> lock1(mtx_);
  if (sp_quorum->Yes()) {
    // become a leader
    setIsLeader(true) ;

    this->rep_frame_ = this->frame_ ;

    auto co = ((TxLogServer *)(this))->CreateRepCoord(0);
    auto empty_cmd = std::make_shared<TpcEmptyCommand>();
    verify(empty_cmd->kind_ == MarshallDeputy::CMD_TPC_EMPTY);
    auto sp_m = dynamic_pointer_cast<Marshallable>(empty_cmd);
    ((CoordinatorFpgaRaft*)co)->Submit(sp_m);
    
    //RequestVote2FPGA() ;
    if(IsLeader())
    {
	  	//for(int i = 0; i < 100; i++) Log_info("wait wait wait");
      Log_debug("vote accepted %d curterm %d", loc_id, currentTerm);
  		req_voting_ = false ;
			return true;
    }
    else
    {
      Log_debug("fpga vote rejected %d curterm %d, do rollback", loc_id, currentTerm);
      setIsLeader(false) ;
    	return false;
		}
  } else if (sp_quorum->No()) {
    // become a follower
    Log_debug("vote rejected %d", loc_id);
    setIsLeader(false) ;
    //reset cur term if new term is higher
    ballot_t new_term = sp_quorum->Term() ;
    currentTerm = new_term > currentTerm? new_term : currentTerm ;
  	req_voting_ = false ;
		return false;
  } else {
    // TODO process timeout.
    Log_debug("vote timeout %d", loc_id);
  	req_voting_ = false ;
		return false;
  }
}

void FpgaRaftServer::OnVote(const slotid_t& lst_log_idx,
                            const ballot_t& lst_log_term,
                            const parid_t& can_id,
                            const ballot_t& can_term,
                            ballot_t *reply_term,
                            bool_t *vote_granted,
                            const function<void()> &cb) {

  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("fpga raft receives vote from candidate: %llx", can_id);

  setIsFPGALeader(false) ;

  // TODO wait all the log pushed to fpga host

  uint64_t cur_term = currentTerm ;
  if( can_term < cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  // has voted to a machine in the same term, vote no
  // TODO when to reset the vote_for_??
//  if( can_term == cur_term && vote_for_ != INVALID_PARID )
  if( can_term == cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  // lstoff starts from 1
  uint32_t lstoff = lastLogIndex - snapidx_ ;

  ballot_t curlstterm = snapterm_ ;
  slotid_t curlstidx = lastLogIndex ;

  if(lstoff > 0 )
  {
    auto log = GetFpgaRaftInstance(lstoff) ;
    curlstterm = log->term ;
  }

  Log_debug("vote for lstoff %d, curlstterm %d, curlstidx %d", lstoff, curlstterm, curlstidx  );


  // TODO del only for test 
  verify(lstoff == lastLogIndex ) ;

  if( lst_log_term > curlstterm || (lst_log_term == curlstterm && lst_log_idx >= curlstidx) )
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, true, cb) ;
    return ;
  }

  doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;

}

void FpgaRaftServer::StartTimer()
{
    if(!init_ ){
        resetTimer() ;
        Coroutine::CreateRun([&]() {
            Log_debug("start timer for election") ;
            int32_t duration = randDuration() ;
            while(!stop_)
            {
                if ( !IsLeader() && timer_->elapsed() > duration) {
                    Log_info(" timer time out") ;
                    // ask to vote
                    // req_voting_ = true ;
                    RequestVote() ;
                    /*while(req_voting_)
                    {
                      auto sp_e1 = Reactor::CreateSpEvent<TimeoutEvent>(wait_int_);
                      sp_e1->Wait(wait_int_) ;
                      if(stop_) return ;
                    }*/
                    Log_debug("start a new timer") ;
                    resetTimer() ;
                    duration = randDuration() ;
                }
                auto sp_e2 = Reactor::CreateSpEvent<TimeoutEvent>(wait_int_);
                sp_e2->Wait(wait_int_) ;
            } 
        });
      init_ = true ;
  }
}

/* NOTE: same as ReceiveAppend */
/* NOTE: broadcast send to all of the host even to its own server 
 * should we exclude the execution of this function for leader? */
  void FpgaRaftServer::OnAppendEntries(const slotid_t slot_id,
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
                                     const function<void()> &cb) {

        std::lock_guard<std::recursive_mutex> lock(mtx_);
        //StartTimer() ;
        
        Log_debug("fpga-raft scheduler on append entries for "
                "slot_id: %llx, loc: %d, PrevLogIndex: %d",
                slot_id, this->loc_id_, leaderPrevLogIndex);
        if ((leaderCurrentTerm >= this->currentTerm) &&
                (leaderPrevLogIndex <= this->lastLogIndex)
                /* TODO: log[leaderPrevLogidex].term == leaderPrevLogTerm */) {
            //resetTimer() ;
            if (leaderCurrentTerm > this->currentTerm) {
                currentTerm = leaderCurrentTerm;
                Log_debug("server %d, set to be follower", loc_id_ ) ;
                setIsLeader(false) ;
            }

						//this means that this is a retry of a previous one for a simulation
						/*if (slot_id == 100000000 || leaderPrevLogIndex + 1 < lastLogIndex) {
							for (int i = 0; i < 1000000; i++) Log_info("Dropping this AE message: %d %d", leaderPrevLogIndex, lastLogIndex);
							//verify(0);
							*followerAppendOK = 0;
							cb();
							return;
						}*/

            this->lastLogIndex = leaderPrevLogIndex + 1 /* TODO:len(ents) */;
            uint64_t prevCommitIndex = this->commitIndex;
            this->commitIndex = std::max(leaderCommitIndex, this->commitIndex);
            /* TODO: Replace entries after s.log[prev] w/ ents */
            /* TODO: it should have for loop for multiple entries */
            auto instance = GetFpgaRaftInstance(lastLogIndex);
            instance->log_ = cmd;


            // Pass the content to a thread that is always running
            // Disk write event
            // Wait on the event
            instance->term = this->currentTerm;
            //app_next_(*instance->log_); 
            verify(lastLogIndex > commitIndex);

            *followerAppendOK = 1;
            *followerCurrentTerm = this->currentTerm;
            *followerLastLogIndex = this->lastLogIndex;
            
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

							//auto de = IO::write("/db/data.txt", key_values, sizeof(struct KeyValue), kv_vector.size());

							//de->Wait();

              //Changed here.
              std::string index_key = std::to_string(this->lastLogIndex);
	          auto val=rdb::RocksdbWrapper::MakeSlice(reinterpret_cast<char*>(key_values),sizeof(struct KeyValue)*kv_vector.size());
	          rdb::rocksdb_wrapper()->Put(index_key,val);
	      
            } else {
							int value = -1;
							//auto de = IO::write("/db/data.txt", &value, sizeof(int), 1);

              //de->Wait();

              //Changed here.
              std::string index_key = std::to_string(this->lastLogIndex);
              auto val=rdb::RocksdbWrapper::MakeSlice(reinterpret_cast<char*>(&value),sizeof(int));
	          rdb::rocksdb_wrapper()->Put(index_key,val);
            }
        }
        else {
            Log_debug("reject append loc: %d, leader term %d last idx %d, server term: %d last idx: %d",
                this->loc_id_, leaderCurrentTerm, leaderPrevLogIndex, currentTerm, lastLogIndex);          
            *followerAppendOK = 0;
        }

				/*if (rand() % 1000 == 0) {
					usleep(25*1000);
				}*/
        cb();
    }

    void FpgaRaftServer::OnForward(shared_ptr<Marshallable> &cmd, 
                                          uint64_t *cmt_idx,
                                          const function<void()> &cb) {
        this->rep_frame_ = this->frame_ ;
        auto co = ((TxLogServer *)(this))->CreateRepCoord(0);
        ((CoordinatorFpgaRaft*)co)->Submit(cmd);
        
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        *cmt_idx = ((CoordinatorFpgaRaft*)co)->cmt_idx_ ;
        if(IsLeader() || *cmt_idx == 0 )
        {
          Log_debug(" is leader");
          *cmt_idx = this->commitIndex ;
        }

        verify(*cmt_idx != 0) ;
        cb() ;        
    }

  void FpgaRaftServer::OnCommit(const slotid_t slot_id,
                              const ballot_t ballot,
                              shared_ptr<Marshallable> &cmd) {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
		struct timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);

    // This prevents the log entry from being applied twice
    if (in_applying_logs_) {
      return;
    }
    in_applying_logs_ = true;
    
    for (slotid_t id = executeIndex + 1; id <= commitIndex; id++) {
        auto next_instance = GetFpgaRaftInstance(id);
        if (next_instance->log_) {
            Log_debug("fpga-raft par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
            app_next_(*next_instance->log_);
            executeIndex++;
        } else {
            break;
        }
    }
    in_applying_logs_ = false;

		/*clock_gettime(CLOCK_MONOTONIC, &end);
		Log_info("time of decide on server: %d", (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec);*/
  }
  void FpgaRaftServer::SpCommit(const uint64_t cmt_idx) {
      verify(0) ; // TODO delete it
      std::lock_guard<std::recursive_mutex> lock(mtx_);
      Log_debug("fpga raft spcommit for index: %lx for server %d", cmt_idx, loc_id_);
      verify(cmt_idx != 0 ) ;
      if (cmt_idx < commitIndex) {
          return ;
      }

      commitIndex = cmt_idx;

      for (slotid_t id = executeIndex + 1; id <= commitIndex; id++) {
          auto next_instance = GetFpgaRaftInstance(id);
          if (next_instance->log_) {
              app_next_(*next_instance->log_);
              Log_debug("fpga-raft par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
              executeIndex++;
          } else {
              break;
          }
      }
  }

} // namespace janus
