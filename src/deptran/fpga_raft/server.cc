

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"
#include "frame.h"
#include "coordinator.h"


namespace janus {

FpgaRaftServer::FpgaRaftServer(Frame * frame) {
  frame_ = frame ;
  state_ = frame_->site_info_->locale_id == 2? State::LEADER : State::FOLLOWER ;
  stop_ = false ;
  timer_ = new Timer() ;
}

FpgaRaftServer::~FpgaRaftServer() {
    stop_ = true ;
    Log_info("site par %d, loc %d: prepare %d, accept %d, commit %d", partition_id_, loc_id_, n_prepare_, n_accept_, 
    n_commit_);
}

void FpgaRaftServer::AskToVote() {
  // TODO need a lock yidawu
  //std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler ask to vote");
  state_ = State::CANDIDATE ;
  currentTerm++ ;
//  parid_t par_id = site_info_->partition_id_ ;
  parid_t par_id = 0 ;
  parid_t loc_id = 0 ;
  slotid_t lst_idx = 0;
  ballot_t lst_term = 0 ; // broadcast here

  if(this->commo_ == NULL ) return ;
  
  par_id = this->frame_->site_info_->partition_id_;
  loc_id = this->frame_->site_info_->locale_id;

  auto sp_quorum = ((FpgaRaftCommo *)(this->commo_))->BroadcastVote(par_id,lst_idx,lst_term,loc_id, currentTerm );
  sp_quorum->Wait();
  if (sp_quorum->Yes()) {
    // become a leader
    state_ = State::LEADER ;
    Log_debug("vote accepted %d curterm %d", loc_id, currentTerm);
  } else if (sp_quorum->No()) {
    // become a follower
    Log_debug("vote rejected %d", loc_id);
    state_ = State::FOLLOWER ;
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
  Log_debug("multi-paxos scheduler receives vote for candidate: %llx", can_id);
  if ( frame_->site_info_->locale_id == can_id)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;  
  } 

  uint64_t cur_term = currentTerm ;

  if( can_term <= cur_term)
  {
    doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, false, cb) ;
    return ;
  }

  doVote(lst_log_idx, lst_log_term, can_id, can_term, reply_term, vote_granted, true, cb) ;

}


void FpgaRaftServer::OnPrepare(slotid_t slot_id,
                            ballot_t ballot,
                            ballot_t *max_ballot,
                            const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler receives prepare for slot_id: %llx",
            slot_id);
  auto instance = GetInstance(slot_id);
  verify(ballot != instance->max_ballot_seen_);
  if (instance->max_ballot_seen_ < ballot) {
    instance->max_ballot_seen_ = ballot;
  } else {
    // TODO if accepted anything, return;
    verify(0);
  }
  *max_ballot = instance->max_ballot_seen_;
  n_prepare_++;
  cb();
}

void FpgaRaftServer::OnAccept(const slotid_t slot_id,
                           const ballot_t ballot,
                           shared_ptr<Marshallable> &cmd,
                           ballot_t *max_ballot,
                           const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler accept for slot_id: %llx", slot_id);
  auto instance = GetInstance(slot_id);
  verify(instance->max_ballot_accepted_ < ballot);
  if (instance->max_ballot_seen_ <= ballot) {
    instance->max_ballot_seen_ = ballot;
    instance->max_ballot_accepted_ = ballot;
  } else {
    // TODO
    verify(0);
  }
  *max_ballot = instance->max_ballot_seen_;
  n_accept_++;
  cb();
}

void FpgaRaftServer::StartTimer()
{
    if(!init_ ){
        resetTimer() ;

        Coroutine::CreateRun([&]() {
            //int32_t duration = 3 ;
            //Reactor::GetReactor()->looping_ = true ;
            int32_t duration = 2 + RandomGenerator::rand(0, 4) ;
            auto timeout = 1*1000*1000;
            while(!stop_)
            {
                if ( !IsLeader() && timer_->elapsed() > duration) {
                    Log_debug(" timer time out") ;
                    // ask to vote
                    AskToVote() ;
                    auto sp_e1 = Reactor::CreateSpEvent<TimeoutEvent>(timeout);
                    while(!end_ && !stop_)
                    {
                    sp_e1->Wait() ;
                    }

                    Log_debug("start a new timer") ;
                    timer_->start() ;
                    duration = 2 + RandomGenerator::rand(0, 4) ;
                }
                auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(timeout);
                sp_e->Wait() ;
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
                this->state_ = State::FOLLOWER ;
                this->currentTerm = leaderCurrentTerm;
            }
            this->lastLogIndex = leaderPrevLogIndex + 1 /* TODO:len(ents) */;
            uint64_t prevCommitIndex = this->commitIndex;
            this->commitIndex = std::max(leaderCommitIndex, this->commitIndex);
            /* TODO: Replace entries after s.log[prev] w/ ents */
            /* TODO: it should have for loop for multiple entries */
            auto instance = GetFpgaRaftInstance(lastLogIndex);
            instance->log_ = cmd;
            instance->term = this->currentTerm;
            /* TODO: execute logs */
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

    void FpgaRaftServer::OnForward(shared_ptr<Marshallable> &cmd)
    {
        if(IsLeader())
        {
            Log_debug(" is leader");
        }
        else{
        Log_debug(" not leader");}

        this->rep_frame_ = this->frame_ ;

        auto co = ((TxLogServer *)(this))->CreateRepCoord();
        ((CoordinatorFpgaRaft*)co)->Submit(cmd);
    }


    void FpgaRaftServer::OnCommit(const slotid_t slot_id,
            const ballot_t ballot,
            shared_ptr<Marshallable> &cmd) {
        std::lock_guard<std::recursive_mutex> lock(mtx_);
        Log_debug("multi-paxos scheduler decide for slot: %lx", slot_id);
        auto instance = GetInstance(slot_id);
        instance->committed_cmd_ = cmd;
        if (slot_id > max_committed_slot_) {
            max_committed_slot_ = slot_id;
        }
        verify(slot_id > max_executed_slot_);
        for (slotid_t id = max_executed_slot_ + 1; id <= max_committed_slot_; id++) {
            auto next_instance = GetInstance(id);
            if (next_instance->committed_cmd_) {
                app_next_(*next_instance->committed_cmd_);
                Log_debug("multi-paxos par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
                max_executed_slot_++;
                n_commit_++;
            } else {
                break;
            }
        }

        // TODO should support snapshot for freeing memory.
        // for now just free anything 1000 slots before.
        int i = min_active_slot_;
        while (i + 1000 < max_executed_slot_) {
            logs_.erase(i);
            i++;
        }
        min_active_slot_ = i;
    }

} // namespace janus
