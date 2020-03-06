

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"


namespace janus {

FpgaRaftServer::FpgaRaftServer(Frame * frame) {
  frame_ = frame ;
  is_leader_ = frame_->site_info_->locale_id == 1 ;
  stop_ = false ;
  timer_ = new Timer() ;
}

FpgaRaftServer::~FpgaRaftServer() {
    stop_ = true ;
    Log_info("site par %d, loc %d: prepare %d, accept %d, commit %d", partition_id_, loc_id_, n_prepare_, n_accept_, 
    n_commit_);
}

void FpgaRaftServer::RequestVote() {
  
  if(this->commo_ == NULL ) return ;

  parid_t par_id = this->frame_->site_info_->partition_id_ ;
  parid_t loc_id = this->frame_->site_info_->locale_id ;

  Log_debug("fpga raft server %d ask to vote", loc_id );

  uint32_t lstoff = 0  ;
  slotid_t lst_idx = 0 ;
  ballot_t lst_term = 0 ;

  {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    // TODO set fpga isleader false 
    currentTerm++ ;
    lstoff = lastLogIndex - snapidx_ ;
    auto log = GetFpgaRaftInstance(lstoff) ;
    lst_idx = log->idx;
    lst_term = log->term ;
  }
  
  auto sp_quorum = ((FpgaRaftCommo *)(this->commo_))->BroadcastVote(par_id,lst_idx,lst_term,loc_id, currentTerm );
  sp_quorum->Wait();
  std::lock_guard<std::recursive_mutex> lock1(mtx_);
  if (sp_quorum->Yes()) {
    // become a leader
    is_leader_ = true ;
    Log_debug("vote accepted %d curterm %d", loc_id, currentTerm);
  } else if (sp_quorum->No()) {
    // become a follower
    Log_debug("vote rejected %d", loc_id);
    is_leader_ = false ;
    //reset cur term if new term is higher
    ballot_t new_term = sp_quorum->Term() ;
    currentTerm = new_term > currentTerm? new_term : currentTerm ;
  } else {
    // TODO process timeout.
    Log_debug("vote timeout %d", loc_id);
  }
  end_ = true ;
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

  uint64_t cur_term = currentTerm ;
  if( can_term < cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  // has voted to a machine in the same term, vote no
  // TODO when to reset the vote_for_??
  if( can_term == cur_term && vote_for_ != INVALID_PARID )
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
    curlstidx = log->idx ;
  }

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
                    Log_debug(" timer time out") ;
                    // ask to vote
                    RequestVote() ;
                    auto sp_e1 = Reactor::CreateSpEvent<TimeoutEvent>(wait_int_);
                    while(!end_)
                    {
                      sp_e1->Wait(wait_int_) ;
                      if(stop_) return ;
                    }
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
                                     shared_ptr<Marshallable> &cmd,
                                     uint64_t *followerAppendOK,
                                     uint64_t *followerCurrentTerm,
                                     uint64_t *followerLastLogIndex,
                                     const function<void()> &cb) {

        std::lock_guard<std::recursive_mutex> lock(mtx_);
        StartTimer() ;
        resetTimer() ;

        Log_debug("fpga-raft scheduler on append entries for "
                "slot_id: %llx, loc: %d, PrevLogIndex: %d",
                slot_id, this->loc_id_, leaderPrevLogIndex);
        if ((leaderCurrentTerm >= this->currentTerm) &&
                (leaderPrevLogIndex <= this->lastLogIndex)
                /* TODO: log[leaderPrevLogidex].term == leaderPrevLogTerm */) {
            if (leaderCurrentTerm > this->currentTerm) {
                currentTerm = leaderCurrentTerm;
                Log_debug("server %d, set to be follower", loc_id_ ) ;
                is_leader_ = false ;
            }
            this->lastLogIndex = leaderPrevLogIndex + 1 /* TODO:len(ents) */;
            uint64_t prevCommitIndex = this->commitIndex;
            this->commitIndex = std::max(leaderCommitIndex, this->commitIndex);
            /* TODO: Replace entries after s.log[prev] w/ ents */
            /* TODO: it should have for loop for multiple entries */
            auto instance = GetFpgaRaftInstance(lastLogIndex);
            instance->log_ = cmd; 
            instance->term = this->currentTerm;
            //app_next_(*instance->log_); 
            verify(lastLogIndex > commitIndex);

            *followerAppendOK = 1;
            *followerCurrentTerm = this->currentTerm;
            *followerLastLogIndex = this->lastLogIndex;
        }
        else {
            *followerAppendOK = 0;
        }
        cb();
    }

    void FpgaRaftServer::OnForward(shared_ptr<Marshallable> &cmd, 
                                          uint64_t *cmt_idx,
                                          const function<void()> &cb) {
        this->rep_frame_ = this->frame_ ;

        auto co = ((TxLogServer *)(this))->CreateRepCoord();
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
    /* verify(slot_id > max_executed_slot_); */
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
  void FpgaRaftServer::SpCommit(const uint64_t cmt_idx) {
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
