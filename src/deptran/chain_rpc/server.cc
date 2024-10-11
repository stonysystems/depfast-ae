

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"
#include "utils.h"

namespace janus {

bool ChainRPCServer::looping = false;

struct hb_loop_args_type {
	ChainRPCCommo* commo;
	ChainRPCServer* sch;
};

ChainRPCServer::ChainRPCServer(Frame * frame) {
  frame_ = frame ;
  setIsFPGALeader(frame_->site_info_->locale_id == 0) ;
  setIsLeader(frame_->site_info_->locale_id == 0) ;
  stop_ = false ;
  timer_ = new Timer() ;
}

void ChainRPCServer::Setup() {
	if (heartbeat_ && !ChainRPCServer::looping && IsLeader()) {
		Log_info("starting loop at server");
		ChainRPCServer::looping = true;
		memset(&loop_th_, 0, sizeof(loop_th_));
		hb_loop_args_type* hb_loop_args = new hb_loop_args_type();
		hb_loop_args->commo = (ChainRPCCommo*) commo();
		hb_loop_args->sch = this;
		verify(hb_loop_args->commo && hb_loop_args->sch);
		Pthread_create(&loop_th_, nullptr, ChainRPCServer::HeartbeatLoop, hb_loop_args);
	}
}

void* ChainRPCServer::HeartbeatLoop(void* args) {
	hb_loop_args_type* hb_loop_args = (hb_loop_args_type*) args;

	ChainRPCServer::looping = true;
	while(ChainRPCServer::looping) {
		usleep(100*1000);
		uint64_t prevLogIndex = hb_loop_args->sch->lastLogIndex;	
		
		auto instance = hb_loop_args->sch->GetChainRPCInstance(prevLogIndex);
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

ChainRPCServer::~ChainRPCServer() {
		if (heartbeat_ && ChainRPCServer::looping) {
			ChainRPCServer::looping = false;
			Pthread_join(loop_th_, nullptr);
		}
    
		stop_ = true ;
    Log_info("site par %d, loc %d: prepare %d, accept %d, commit %d", partition_id_, loc_id_, n_prepare_, n_accept_, 
    n_commit_);
}

void ChainRPCServer::RequestVote2FPGA() {

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
    auto log = GetChainRPCInstance(lstoff) ;
    lst_idx = lstoff + snapidx_ ;
    lst_term = log->term ;
  }
  
  auto sp_quorum = ((ChainRPCCommo *)(this->commo_))->BroadcastVote2FPGA(par_id,lst_idx,lst_term,loc_id, currentTerm );
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

void ChainRPCServer::OnVote2FPGA(const slotid_t& lst_log_idx,
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
    auto log = GetChainRPCInstance(lstoff) ;
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


bool ChainRPCServer::RequestVote() {
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
    auto log = GetChainRPCInstance(lstoff) ;
    lst_idx = lstoff + snapidx_ ;
    lst_term = log->term ;
  }
  
  auto sp_quorum = ((ChainRPCCommo *)(this->commo_))->BroadcastVote(par_id,lst_idx,lst_term,loc_id, currentTerm );
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
    ((CoordinatorChainRPC*)co)->Submit(sp_m);
    
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

void ChainRPCServer::OnVote(const slotid_t& lst_log_idx,
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
    auto log = GetChainRPCInstance(lstoff) ;
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

void ChainRPCServer::StartTimer()
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
                sp_e2->Wait() ;
            } 
        });
      init_ = true ;
  }
}

/* NOTE: same as ReceiveAppend */
/* NOTE: broadcast send to all of the host even to its own server 
 * should we exclude the execution of this function for leader? */
  void ChainRPCServer::OnAppendEntries(const slotid_t slot_id,
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
        Log_track("fpga-raft scheduler on append entries for "
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
            verify(this->lastLogIndex == leaderPrevLogIndex);
            this->lastLogIndex = leaderPrevLogIndex + 1 /* TODO:len(ents) */;
            uint64_t prevCommitIndex = this->commitIndex;
            this->commitIndex = std::max(leaderCommitIndex, this->commitIndex);
            /* TODO: Replace entries after s.log[prev] w/ ents */
            /* TODO: it should have for loop for multiple entries */
            auto instance = GetChainRPCInstance(lastLogIndex);
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
            

            // Write to disk? why do we need this op?
						if (cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT){
              // auto p_cmd = dynamic_pointer_cast<TpcCommitCommand>(cmd);
              // auto sp_vec_piece = dynamic_pointer_cast<VecPieceData>(p_cmd->cmd_)->sp_vec_piece_data_;
              
							// vector<struct KeyValue> kv_vector;
							// int index = 0;
							// for (auto it = sp_vec_piece->begin(); it != sp_vec_piece->end(); it++){
							// 	auto cmd_input = (*it)->input.values_;
							// 	for (auto it2 = cmd_input->begin(); it2 != cmd_input->end(); it2++) {
							// 		struct KeyValue key_value = {it2->first, it2->second.get_i32()};
							// 		kv_vector.push_back(key_value);
							// 	}
							// }
              // fprintf(stderr, "kv_vector size: %d\n", kv_vector.size());

							// struct KeyValue key_values[kv_vector.size()];
							// std::copy(kv_vector.begin(), kv_vector.end(), key_values);

							// auto de = IO::write(filename, key_values, sizeof(struct KeyValue), kv_vector.size());
							// de->Wait();
            } else {
							// int value = -1;
							// auto de = IO::write(filename, &value, sizeof(int), 1);
              // de->Wait();
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
        // shared_ptr<Marshallable> sp;
        // OnCommit(0,0, sp); // We don't really use those parameters.
        // fprintf(stderr, "OnCommit done\n");
    }
    
    void ChainRPCServer::OnAppendEntriesAccBack2LeaderChain(const slotid_t slot_id,
                                     const ballot_t ballot,
                                     shared_ptr<Marshallable> &cu_cmd,
                                     const function<void()> &cb) {
        auto cu_cmd_ptr = dynamic_pointer_cast<ControlUnit>(cu_cmd);
        auto commo = (ChainRPCCommo *)(this->commo_);
        verify(commo->rpc_par_proxies_[partition_id_].size() == cu_cmd_ptr->total_partitions_);
        Log_track("Received AppendEntriesAccBack2LeaderChain controlUnit:%s", cu_cmd_ptr->toString().c_str());
        if (IsLeader()) {
          // uint64_t end_in_ns = cu_cmd_ptr->GetNowInns();
          // Log_track("Leader received back: %f ms, path_id: %d, uuid_:%s, ", (end_in_ns-cu_cmd_ptr->init_time)/1000.0/1000.0, cu_cmd_ptr->pathIdx_, cu_cmd_ptr->uuid_.c_str());

          // If the leader receives an accumulated results from the followers
          // We should feed a accumulated results back to the coordinator to make a final decision.
          
          auto data = commo->data_append_map_[cu_cmd_ptr->uniq_id_];
          auto e = std::get<8>(data);
          unordered_map<int, int> ackedReplicas;
          if (e) {
            for (int i=0; i<cu_cmd_ptr->appendOK_.size(); i++) {
              if (cu_cmd_ptr->appendOK_[i] == 1) {
                ackedReplicas[cu_cmd_ptr->local_ids_[i]] = 1;
                e->FeedResponse(true, cu_cmd_ptr->lastLogIndex_[i], "");
              }
            }
            
            Log_track("Without retry, uuid_:%s ready:%d", cu_cmd_ptr->uuid_.c_str(), e->IsReady());
            if (e->IsReady()) {
              //commo->received_quorum_ok_cnt += 1;
            } else {
              //commo->received_quorum_fail_cnt += 1; 
            }

            while (!e->IsReady()) { // A majority of acked replicas are ready.
              cu_cmd_ptr->isRetry = 1;
              vector<int> hops ;
              int nextHop = -1;
              for (int i=1; i<cu_cmd_ptr->total_partitions_; i++) {
                if (ackedReplicas.find(i) == ackedReplicas.end()) {
                  nextHop = i;
                  break;
                }
              }

              if (nextHop >= 0) {
                int uniq_id_ = cu_cmd_ptr->uniq_id_;
                auto proxy = (ChainRPCProxy*)commo->rpc_par_proxies_[partition_id_][nextHop].second;
                auto retry_e = Reactor::CreateSpEvent<QuorumEvent>(1, 1);
                std::string uuid_ = cu_cmd_ptr->uuid_;
                //commo->retry_rpc_cnt += 1;

                FutureAttr fuattr;
                fuattr.callback = [&e, nextHop, uuid_, uniq_id_, &retry_e,  &ackedReplicas] (Future* fu) {
                  retry_e->VoteYes();
                  uint64_t accept = 0;
                  uint64_t term = 0;
                  uint64_t index = 0;
                  
                  fu->get_reply() >> accept;
                  fu->get_reply() >> term;
                  fu->get_reply() >> index;

                  if (accept=1) {
                    e->FeedResponse(true, index, "");
                    Log_track("acked replica: %d in Retry, uniq_id_:%d, uuid: %s", nextHop, uniq_id_, uuid_.c_str());
                  }
                  ackedReplicas[nextHop] = 1;
                };

                Log_track("Retry a RPC to a hop: %d", nextHop);
                MarshallDeputy cu_md(cu_cmd);
                auto f = proxy->async_AppendEntriesChain(std::get<0>(data),
                                            std::get<1>(data),
                                            std::get<2>(data),
                                            std::get<3>(data),
                                            std::get<4>(data),
                                            std::get<5>(data),
                                            std::get<6>(data),
                                            std::get<7>(data),
                                            cu_md,
                                            fuattr);
                Future::safe_release(f);
                retry_e->Wait();
              }
            }
            commo->data_append_map_.erase(cu_cmd_ptr->uniq_id_);
          }else {
            Log_info("Fail to update the event mapping");
          }
        } else {
          void(0);
        }
        cb();
        return ;
    }

    void ChainRPCServer::OnAppendEntriesChain(const slotid_t slot_id,
                                     const ballot_t ballot,
                                     const uint64_t leaderCurrentTerm,
                                     const uint64_t leaderPrevLogIndex,
                                     const uint64_t leaderPrevLogTerm,
                                     const uint64_t leaderCommitIndex,
																		 const struct DepId dep_id,
                                     shared_ptr<Marshallable> &cmd,
                                     shared_ptr<Marshallable> &cu_cmd,
                                     uint64_t *followerAppendOK,
                                     uint64_t *followerCurrentTerm,
                                     uint64_t *followerLastLogIndex,
                                     const function<void()> &cb) {
        auto cu_cmd_ptr = dynamic_pointer_cast<ControlUnit>(cu_cmd);
        auto commo = (ChainRPCCommo *)(this->commo_);
        verify(commo->rpc_par_proxies_[partition_id_].size() == cu_cmd_ptr->total_partitions_);
        Log_track("Received controlUnit:%s", cu_cmd_ptr->toString().c_str());

        Log_track("ChainRPC scheduler on append entries for "
                 "loc: %d, slot_id: %llu, PrevLogIndex: %d, lastLogIndex: %d, isLeader: %d",
                 this->loc_id_, slot_id, leaderPrevLogIndex, this->lastLogIndex, IsLeader());

        { // minimize contention
          std::lock_guard<std::recursive_mutex> lock(mtx_);
          if ((leaderCurrentTerm >= this->currentTerm) &&
                  (leaderPrevLogIndex <= this->lastLogIndex) &&
                  this->lastLogIndex == leaderPrevLogIndex) {
              if (leaderCurrentTerm > this->currentTerm) {
                  currentTerm = leaderCurrentTerm;
                  Log_debug("server %d, set to be follower", loc_id_ ) ;
                  setIsLeader(false) ;
              }

              verify(this->lastLogIndex == leaderPrevLogIndex);
              this->lastLogIndex = leaderPrevLogIndex + 1;
              uint64_t prevCommitIndex = this->commitIndex;
              this->commitIndex = std::max(leaderCommitIndex, this->commitIndex);
              /* TODO: Replace entries after s.log[prev] w/ ents */
              /* TODO: it should have for loop for multiple entries */
              auto instance = GetChainRPCInstance(lastLogIndex);
              instance->log_ = cmd;

              // Pass the content to a thread that is always running
              // Disk write event
              // Wait on the event
              instance->term = this->currentTerm;
              //app_next_(*instance->log_); 
              //verify(lastLogIndex > commitIndex);

              *followerAppendOK = 1;
              *followerCurrentTerm = this->currentTerm;
              *followerLastLogIndex = this->lastLogIndex;
              
              cu_cmd_ptr->AppendResponseForAppendEntries(loc_id_, *followerAppendOK, *followerCurrentTerm, *followerLastLogIndex);
              cu_cmd_ptr->acc_ack_ += 1;
          }
          else {
              Log_debug("reject append loc: %d, leader term %d last idx %d, server term: %d last idx: %d",
                  this->loc_id_, leaderCurrentTerm, leaderPrevLogIndex, currentTerm, lastLogIndex);          
              *followerAppendOK = 0;
              
              cu_cmd_ptr->AppendResponseForAppendEntries(loc_id_, *followerAppendOK, *followerCurrentTerm, *followerLastLogIndex);
              cu_cmd_ptr->acc_rej_ += 1;
          }
        }

        // Skip retry entry's propogation
        if (cu_cmd_ptr->isRetry) {
          cb();
          return ;
        }

        // Forward this request and accumulated results to the next hop.
        // Note that the next hop can be the leader or next follower in the chain or both.
        // We have to propogate requests to all replicas in the chain.
        vector<int> hops ;
        int firstHop = cu_cmd_ptr->Increment2NextHop(); // Jump to the next hop.
        hops.push_back(firstHop);
        int secondHop = firstHop; // If jump to the leader early.
        
        if (!cu_cmd_ptr->return_leader_ && cu_cmd_ptr->RegisterEarlyTerminate()) {
          secondHop = 0;
        }

        if (secondHop != firstHop) {
          hops.push_back(secondHop);
        }

        std::sort(hops.begin(), hops.end()); // Always try to send to the leader first.
        for (int i=0; i<hops.size(); i++) {
          int nextHop = hops[i];
          
          if (nextHop == 0) {
            if (cu_cmd_ptr->return_leader_ == 1) { // If already returned, no further actions.
              continue; 
            }
            cu_cmd_ptr->return_leader_ = 1;
          }
          Log_track("Jump to next hop: %d, ControlUnit: %s", nextHop, cu_cmd_ptr->toString().c_str());

          auto proxy = (ChainRPCProxy*)commo->rpc_par_proxies_[partition_id_][nextHop].second;
          FutureAttr fuattr;
          fuattr.callback = [this] (Future* fu) { 
            // Do nothing...
          };
          MarshallDeputy md(cmd);
          MarshallDeputy cu_md(cu_cmd);
          if (nextHop == 0) { // Carray accumulated results back to the leader
            auto f = proxy->async_AppendEntriesAccBack2LeaderChain(slot_id,
                                      ballot,
                                      cu_md,
                                      fuattr);
            Future::safe_release(f);
          } else { // Continue propogating requests to all nodes
            auto f = proxy->async_AppendEntriesChain(slot_id,
                                      ballot,
                                      currentTerm,
                                      leaderPrevLogIndex,
                                      leaderPrevLogTerm,
                                      commitIndex,
                                      dep_id,
                                      md,
                                      cu_md,
                                      fuattr);
            Future::safe_release(f);
          }
        }
        cb();
    }

    void ChainRPCServer::OnForward(shared_ptr<Marshallable> &cmd, 
                                          uint64_t *cmt_idx,
                                          const function<void()> &cb) {
        this->rep_frame_ = this->frame_ ;
        auto co = ((TxLogServer *)(this))->CreateRepCoord(0);
        ((CoordinatorChainRPC*)co)->Submit(cmd);
        
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        *cmt_idx = ((CoordinatorChainRPC*)co)->cmt_idx_ ;
        if(IsLeader() || *cmt_idx == 0 )
        {
          Log_debug(" is leader");
          *cmt_idx = this->commitIndex ;
        }

        verify(*cmt_idx != 0) ;
        cb() ;        
    }

  void ChainRPCServer::OnCommit(const slotid_t slot_id,
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
        auto next_instance = GetChainRPCInstance(id); // Strong assumption that the log is not null from AppendEntries, but it is not always true.
        if (next_instance->log_) {
            Log_track("chainRPC par:%d loc:%d executed slot %lu", partition_id_, loc_id_, id);
            app_next_(*next_instance->log_);
            executeIndex++;
        } else {
            break;
        }
    }
    in_applying_logs_ = false;

    int i = min_active_slot_;
    while (i + 6000 < executeIndex) {
      removeCmd(i++);
    }
    min_active_slot_ = i;

		/*clock_gettime(CLOCK_MONOTONIC, &end);
		Log_info("time of decide on server: %d", (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec);*/
  }
  void ChainRPCServer::SpCommit(const uint64_t cmt_idx) {
      verify(0) ; // TODO delete it
      std::lock_guard<std::recursive_mutex> lock(mtx_);
      Log_debug("fpga raft spcommit for index: %lx for server %d", cmt_idx, loc_id_);
      verify(cmt_idx != 0 ) ;
      if (cmt_idx < commitIndex) {
          return ;
      }

      commitIndex = cmt_idx;

      for (slotid_t id = executeIndex + 1; id <= commitIndex; id++) {
          auto next_instance = GetChainRPCInstance(id);
          if (next_instance->log_) {
              app_next_(*next_instance->log_);
              Log_debug("fpga-raft par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
              executeIndex++;
          } else {
              break;
          }
      }
  }

  void ChainRPCServer::removeCmd(slotid_t slot) {
    auto cmd = dynamic_pointer_cast<TpcCommitCommand>(raft_logs_[slot]->log_);
    if (!cmd)
      return;
    tx_sched_->DestroyTx(cmd->tx_id_);
    raft_logs_.erase(slot);
  }

} // namespace janus
