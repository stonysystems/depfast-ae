

#include "server.h"
// #include "paxos_worker.h"
#include "exec.h"

namespace janus {


void PaxosServer::OnForward(const uint64_t& tx_id,
                            const int& ret,
                            const int& prepare_or_commit,
                            const uint64_t& dep_id,
                            uint64_t* coro_id,
                            const function<void()> &cb){
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  shared_ptr<Marshallable> sp_m;
  if(prepare_or_commit == 1){
    auto sp_cmd = std::make_shared<TpcPrepareCommand>();
    sp_cmd->tx_id_ = tx_id;
    sp_cmd->cmd_ = sp_tx->cmd_;
    sp_m = dynamic_pointer_cast<Marshallable>(sp_cmd);
  }
  else{
    auto sp_cmd = std::make_shared<TpcCommitCommand>();
    sp_cmd->tx_id_ = tx_id;
    sp_cmd->ret_ = ret;
    sp_m = dynamic_pointer_cast<Marshallable>(sp_cmd);
  }
  
  CreateRepCoord(dep_id)->Submit(sp_m);
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
                           const ballot_t ballot,
                           shared_ptr<Marshallable> &cmd,
                           ballot_t *max_ballot,
                           uint64_t* coro_id,
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
