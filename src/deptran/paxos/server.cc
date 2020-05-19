

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"

namespace janus {


void PaxosServer::OnForward(shared_ptr<Marshallable> &cmd,
                            uint64_t dep_id,
                            uint64_t* coro_id,
                            const function<void()> &cb){
  Log_info("This paxos server is: %d", frame_->site_info_->id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);

  auto config = Config::GetConfig();
  rep_frame_ = Frame::GetFrame(config->replica_proto_);
  rep_frame_->site_info_ = frame_->site_info_;
  
  int n_io_threads = 1;
  auto svr_poll_mgr_ = new rrr::PollMgr(n_io_threads);
  auto rep_commo_ = rep_frame_->CreateCommo(svr_poll_mgr_);
  if(rep_commo_){
    rep_commo_->loc_id_ = frame_->site_info_->locale_id;
  }
  //rep_sched_->loc_id_ = site_info_->locale_id;;
  //rep_sched_->partition_id_ = site_info_->partition_id_;

  CreateRepCoord(dep_id)->Submit(cmd);
  *coro_id = Coroutine::CurrentCoroutine()->id;
  cb();
}

void PaxosServer::OnPrepare(slotid_t slot_id,
                            ballot_t ballot,
                            ballot_t *max_ballot,
                            uint64_t* coro_id,
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
  *coro_id = Coroutine::CurrentCoroutine()->id;
  *max_ballot = instance->max_ballot_seen_;
  n_prepare_++;
  cb();
}


void PaxosServer::OnAccept(const slotid_t slot_id,
		           const uint64_t time,
                           const ballot_t ballot,
                           shared_ptr<Marshallable> &cmd,
                           ballot_t *max_ballot,
                           uint64_t* coro_id,
                           const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("multi-paxos scheduler accept for slot_id: %llx", slot_id);

  auto instance = GetInstance(slot_id);
  
  //TODO: might need to optimize this. we can vote yes on duplicates at least for now
  //verify(instance->max_ballot_accepted_ < ballot);
  
  if (instance->max_ballot_seen_ <= ballot) {
    instance->max_ballot_seen_ = ballot;
    instance->max_ballot_accepted_ = ballot;
  } else {
    // TODO
    verify(0);
  }

  *coro_id = Coroutine::CurrentCoroutine()->id;
  *max_ballot = instance->max_ballot_seen_;
  n_accept_++;
  cb();
}

void PaxosServer::OnCommit(const slotid_t slot_id,
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
  // This prevents the log entry from being applied twice
  if (in_applying_logs_) {
    return;
  }
  in_applying_logs_ = true;
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
  FreeSlots();
}

void PaxosServer::OnBulkAccept(shared_ptr<Marshallable> &cmd,
                               i32* valid,
                               const function<void()> &cb) {

  auto bcmd = dynamic_pointer_cast<BulkPaxosCmd>(cmd);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *valid = 1;
  for(int i = 0; i < bcmd->slots.size(); i++){
      slotid_t slot_id = bcmd->slots[i];
      ballot_t ballot_id = bcmd->ballots[i];
      auto instance = GetInstance(slot_id);
      verify(instance->max_ballot_accepted_ < ballot_id);
      if (instance->max_ballot_seen_ <= ballot_id) {
          instance->max_ballot_seen_ = ballot_id;
          instance->max_ballot_accepted_ = ballot_id;
	        n_accept_++;
          *valid &= 1;
      } else {
          *valid &= 0;
          // TODO
          verify(0);
      }

  }
  cb();
}

void PaxosServer::OnBulkCommit(shared_ptr<Marshallable> &cmd,
                               i32* valid,
                               const function<void()> &cb) {
  auto bcmd = dynamic_pointer_cast<BulkPaxosCmd>(cmd);
  vector<shared_ptr<PaxosData>> commit_exec;
  mtx_.lock();
  //Log_debug("multi-paxos scheduler decide for slot: %lx", slot_id);
  for(int i = 0; i < bcmd->slots.size(); i++){
      slotid_t slot_id = bcmd->slots[i];
      ballot_t ballot_id = bcmd->ballots[i];
      auto instance = GetInstance(slot_id);
      instance->committed_cmd_ = bcmd->cmds[i]->sp_data_;
      if (slot_id > max_committed_slot_) {
          max_committed_slot_ = slot_id;
      }
  }
  for (slotid_t id = max_executed_slot_ + 1; id <= max_committed_slot_; id++) {
      auto next_instance = GetInstance(id);
      if (next_instance->committed_cmd_) {
          //app_next_(*next_instance->committed_cmd_);
	  commit_exec.push_back(next_instance);
	  //Log_debug("multi-paxos par:%d loc:%d executed slot %lx now", partition_id_, loc_id_, id);
          max_executed_slot_++;
          n_commit_++;
      } else {
          break;
      }
  }
  //FreeSlots();
  mtx_.unlock();
  for(int i = 0; i < commit_exec.size(); i++){
      app_next_(*commit_exec[i]->committed_cmd_);
  }
  min_active_slot_ = i;
  in_applying_logs_ = false;
}

} // namespace janus
