#include "server.h"
#include "frame.h"
#include "coordinator.h"

// #define DEBUG
#define WAIT_AT_UNCOMMIT
#define N_CMD_KEEP (15000)
// #define USE_TARJAN
const uint64_t PINGPONG_TIMEOUT_US = 500;

namespace janus {

const char* CopilotServer::toString(uint8_t is_pilot) {
  if (is_pilot)
    return "PILOT  ";
  else
    return "COPILOT";
}

CopilotServer::CopilotServer(Frame* frame) : log_infos_(2) {
  frame_ = frame;
  id_ = frame->site_info_->id;
  setIsPilot(frame_->site_info_->locale_id == 0);
  setIsCopilot(frame_->site_info_->locale_id == 1);

  Setup();
}

shared_ptr<CopilotData> CopilotServer::GetInstance(slotid_t slot, uint8_t is_pilot) {
  if (slot < log_infos_[is_pilot].min_active_slot && slot != 0) {
    Log_debug("server %d get freed ins %s %lu", id_, toString(is_pilot), slot);
    return nullptr;  // never re-create freed slot
  }
  auto& sp_instance = log_infos_[is_pilot].logs[slot];
  if (!sp_instance)
    sp_instance = std::make_shared<CopilotData>(
      CopilotData{nullptr,
                  0,
                  is_pilot, slot,
                  0,
                  Status::NOT_ACCEPTED,
                  0, 0});
  return sp_instance;
}

std::pair<slotid_t, uint64_t> CopilotServer::PickInitSlotAndDep() {
  
  uint64_t init_dep;
  slotid_t assigned_slot;
  /**
   * It also assigns the initial dependency for this entry,
   * which is the most recent entry (latest accepted entry)
   * from the other pilot it has seen.
   * 
   * initial slot id is 1, slot 0 is always empty
   */
  if (isPilot_) {
    init_dep = log_infos_[NO].current_slot;
    assigned_slot = ++log_infos_[YES].current_slot;
  } else if (isCopilot_) {
    init_dep = log_infos_[YES].current_slot;
    assigned_slot = ++log_infos_[NO].current_slot;
  } else {
    init_dep = 0;
    assigned_slot = 0;
  }

  Log_debug("server %d assigned %s : %lu -> %lu", id_, toString(isPilot_),
            assigned_slot, init_dep);

  return { assigned_slot, init_dep };
}

slotid_t CopilotServer::GetMaxCommittedSlot(uint8_t is_copilot) {
  return log_infos_[is_copilot].max_committed_slot;
}

bool CopilotServer::WaitMaxCommittedGT(uint8_t is_pilot, slotid_t slot, int timeout) {
  auto &event = log_infos_[is_pilot].max_cmit_evt;
  event.WaitUntilGreaterOrEqualThan(slot, timeout);
  return (event.value_ >= slot);
}

bool CopilotServer::allCmdComitted(shared_ptr<Marshallable> batch_cmd) {
  auto cmds = dynamic_pointer_cast<TpcBatchCommand>(batch_cmd);
  for (auto& c : cmds->cmds_) {
    if (!tx_sched_->CheckCommitted(*c))
      return false;
  }
  return true;
}

bool CopilotServer::EliminateNullDep(shared_ptr<CopilotData> &ins) {
  verify(ins);
  auto& cmd = ins->cmd;
  if (GET_STATUS(ins->status) < Status::FAST_ACCEPTED)
    return false;
  if (GET_STATUS(ins->status) == Status::EXECUTED) {
    Log_debug("server %d: eliminate %s entry %ld status %x", id_, toString(ins->is_pilot), ins->slot_id, ins->status);
    return true;
  }
  if (likely(cmd->kind_ == MarshallDeputy::CMD_TPC_BATCH)) {
    // check if cmd committed in tx scheduler, which virtually means cmd is executed
    if (allCmdComitted(cmd)) {
      Log_debug("server %d: eliminate %s entry %ld status %x", id_, toString(ins->is_pilot), ins->slot_id, ins->status);
       ins->status = Status::EXECUTED;
       if (ins->cmit_evt.value_ < 1)
         ins->cmit_evt.Set(1);
       updateMaxCmtdSlot(log_infos_[ins->is_pilot], ins->slot_id);
       updateMaxExecSlot(ins);
      return true;
    } else {
      return false;
    }
  } else if (cmd->kind_ == MarshallDeputy::CMD_NOOP) {
    // I don't think this case is possible
    Log_debug("server %d: eliminate %s entry %ld status %x", id_, toString(ins->is_pilot), ins->slot_id, ins->status);
     ins->status = Status::EXECUTED;
     if (ins->cmit_evt.value_ < 1)
         ins->cmit_evt.Set(1);
     updateMaxCmtdSlot(log_infos_[ins->is_pilot], ins->slot_id);
   	updateMaxExecSlot(ins);
    return true;
  } else {
    verify(0);
    return false;
  }
}

bool CopilotServer::AllDepsEliminated(uint8_t is_pilot, slotid_t dep_id) {
  for (auto i = log_infos_[is_pilot].max_executed_slot + 1; i <= dep_id; i++) {
    auto d = GetInstance(i, is_pilot);
    if (!EliminateNullDep(d))
      return false;
  }
  return true;
}

void CopilotServer::Setup() {

  log_infos_[NO] = {};
  log_infos_[YES] = {};

  GetInstance(0, YES)->status = EXECUTED;
  GetInstance(0, NO)->status = EXECUTED;
}

void CopilotServer::WaitForPingPong() {
  /**
   * There could be multiple coroutine waiting for the Ping-Pong signal,
   * they can be ready together, but only one can proceed to FastAccept.
   * The first one to wake up will grab the `pingpong_ok_` and set it to false
   * to prevent multiple coroutine from proceeding to FastAccept.
   * 
   * After other coroutines resume, they will see the `pingpong_ok_` as false
   * and re-enter the waiting, unless they have timeout, in which case they will
   * proceed to FastAccept and there will be multiple on-going FastAceept.
   */
  int time_to_wait = PINGPONG_TIMEOUT_US;
  while (WillWait(time_to_wait)) {
    // Log_info("server %d blocked", id_);
    // Log_info("%dus to wait", time_to_wait);
    if (pingpong_event_.WaitUntilGreaterOrEqualThan(1, time_to_wait)) {
      n_timeout++;
      // Log_info("server %d ping pong timeout %lld", id_, n_timeout);
      break;
    }
  }
  last_ready_time_ = Time::now(true);
  pingpong_ok_ = false;
  pingpong_event_.Set(0);  // must set it to 0 to make event into unready state, otherwise WaitUntil.. won't wait.
}

bool CopilotServer::WillWait(int &time_to_wait) const {
  /**
   * Lemma: if pingpong_ok_ is false right before WaitForPingPong(), then it must wait
   * Proof: if pingpong_ok_ is false, pingpong_event must be 0 (the inverse doesn't always hold),
   * since they are always set together. Thus, WaitForPingPong must enter the while loop,
   * after which pingpong_event_ must wait and yield.
   */
  auto now = Time::now(true);
  // Log_info("last %lld, now %lld", last_ready_time_, now);
  if (now >= last_ready_time_ + PINGPONG_TIMEOUT_US) {
    return false;
  } else {
    time_to_wait = last_ready_time_ + PINGPONG_TIMEOUT_US - now;
    return !pingpong_ok_;
  }
}

void CopilotServer::OnForward(shared_ptr<Marshallable>& cmd,
                              const function<void()>& cb) {
  verify(isPilot_ || isCopilot_);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_info("This Copilot server is: %d", id_);
  rep_frame_ = frame_;

  if (!isPilot_ && !isCopilot_) {
    verify(0);
    // TODO: forward to pilot server
  }
  auto coord = (CoordinatorCopilot *)CreateRepCoord();
  coord->Submit(cmd);

  cb();
}

void CopilotServer::OnPrepare(const uint8_t& is_pilot,
                              const uint64_t& slot,
                              const ballot_t& ballot,
                              const struct DepId& dep_id,
                              MarshallDeputy* ret_cmd,
                              ballot_t* max_ballot,
                              uint64_t* dep,
                              status_t* status,
                              const function<void()>& cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto ins = GetInstance(slot, is_pilot);
  log_infos_[is_pilot].current_slot = std::max(slot, log_infos_[is_pilot].current_slot);
  if (!ins) {
    // this entry is too old that it's already freed
    ret_cmd->SetMarshallable(make_shared<TpcNoopCommand>());
    *dep = 0;
    *status = Status::EXECUTED;
    *max_ballot = ballot;
    goto finish;
  }
  verify(ins);

  /**
   * The fast pilot does this by sending Prepare messages with a
   * higher ballot number b' for the entry to all replicas. If b'
   * is higher than the set ballot number for that entry, the
   * replicas reply with PrepareOk messages and update their
   * prepared ballot number for that entry.
   */
  if (ins->ballot < ballot) {
    ins->ballot = ballot;
  }
  /**
   * The PrepareOk messages include the highest ballot number
   * for which a replica has fast or regular accepted an entry,
   * the command and dependency associated with that entry, and
   * an id of the dependency's proposing pilot.
   */
  *max_ballot = ins->ballot;
  if (ins->cmd)
    ret_cmd->SetMarshallable(ins->cmd);
  else
    ret_cmd->SetMarshallable(make_shared<TpcNoopCommand>());
  *dep = ins->dep_id;
  *status = ins->status;

finish:
  Log_debug(
      "server %d [PREPARE    ] %s : %lu -> %lu status %x ballot %ld",
      id_, toString(is_pilot), slot, *dep, *status, *max_ballot);

  cb();
}

void CopilotServer::OnFastAccept(const uint8_t& is_pilot,
                                 const uint64_t& slot,
                                 const ballot_t& ballot,
                                 const uint64_t& dep,
                                 shared_ptr<Marshallable>& cmd,
                                 const struct DepId& dep_id,
                                 ballot_t* max_ballot,
                                 uint64_t* ret_dep,
                                 const function<void()> &cb) {
  // TODO: deal with ballot
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("server %d [FAST_ACCEPT] %s : %lu -> %lu", id_,
            toString(is_pilot), slot, dep);

  auto ins = GetInstance(slot, is_pilot);
  verify(ins);
  log_infos_[is_pilot].current_slot = std::max(slot, log_infos_[is_pilot].current_slot);
  auto& log_info = log_infos_[REVERSE(is_pilot)];
  auto& logs = log_info.logs;
  uint64_t suggest_dep = dep;

  /**
   * Thus, the check only needs to look at later entries in the other
   * pilot’s log. The compatibility check passes unless the replica
   * has already accepted a later entry P'.k (k > j) from the other
   * pilot P0 with a dependency earlier than P.i, i.e., P'.k’s dependency
   * is < P.i.
   * 
   * 
   */
  if (likely(dep != 0)) {
    for (slotid_t j = dep + 1; j <= log_info.max_accepted_slot; j++) {
      // if (!logs[j])
      //   Log_fatal("slot %lu max acpt %lu", j, log_info.max_accepted_slot);
      // auto dep_id = logs[j]->dep_id;
      auto it = logs.find(j);
      if (unlikely(it == logs.end()))
        continue;
      auto dep_id = it->second->dep_id;
      if (dep_id != 0 && dep_id < slot) {
        // if (GetInstance(dep_id, is_pilot)->status == Status::EXECUTED &&
        //     logs[dep]->status == Status::EXECUTED)
        //   continue;
        /**
         * Otherwise, it sends a FastAcceptReply message to the pilot
         * with its latest entry for the other pilot, P'.k,
         * as its suggested dependency.
         * //TODO: definition on "latest"
         */
        suggest_dep = log_info.max_accepted_slot;
        Log_debug(
            "copilot server %d find imcompatiable dependence for %s : "
            "%lu -> %lu. suggest dep: %lu",
            id_, toString(is_pilot), slot, dep, suggest_dep);
        break;
      }
    }
  }

  if (ins->ballot <= ballot) {
    ins->ballot = ballot;
    ins->dep_id = dep;
    ins->cmd = cmd;
    // still set the cmd here, to prevent PREPARE from getting an empty cmd
    ins->status = Status::FAST_ACCEPTED;
    if (suggest_dep == dep) {
      ins->status = Status::FAST_ACCEPTED_EQ;
      updateMaxAcptSlot(log_infos_[is_pilot], slot);
    }
  } else {
    // TODO
  }
  *max_ballot = ballot;
  *ret_dep = suggest_dep;

  if (cb) {
    pingpong_event_.Set(1);
    pingpong_ok_ = true;
    cb();
  }
}

void CopilotServer::OnAccept(const uint8_t& is_pilot,
                             const uint64_t& slot,
                             const ballot_t& ballot,
                             const uint64_t& dep,
                             shared_ptr<Marshallable>& cmd,
                             const struct DepId& dep_id,
                             ballot_t* max_ballot,
                             const function<void()> &cb) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("server %d [ACCEPT     ] %s : %lu -> %lu", id_, toString(is_pilot), slot, dep);

  auto ins = GetInstance(slot, is_pilot);
  verify(ins);
  auto& log_info = log_infos_[is_pilot];
  log_info.current_slot = std::max(slot, log_info.current_slot);

  if (ins->ballot <= ballot) {
    ins->ballot = ballot;
    ins->dep_id = dep;
    ins->cmd = cmd;
    ins->status = Status::ACCEPTED;
    updateMaxAcptSlot(log_info, slot);
  } else {  // ins->ballot > ballot
    /**
     * This can happen when a fast-takeover ACCEPT reaches the replica before a regular
     * ACCEPT and set a higher ballot number for the instance, thus block the regular
     * ACCEPT.
     */
  }

  *max_ballot = ballot;
  if (cb)
    cb();
}

void CopilotServer::OnCommit(const uint8_t& is_pilot,
                             const uint64_t& slot,
                             const uint64_t& dep,
                             shared_ptr<Marshallable>& cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("server %d [COMMIT     ] %s : %ld -> %ld", id_, toString(is_pilot), slot, dep);
  auto ins = GetInstance(slot, is_pilot);
  log_infos_[is_pilot].current_slot = std::max(slot, log_infos_[is_pilot].current_slot);
  if (!ins)
    return;
  verify(ins);
  if (GET_STATUS(ins->status) >= Status::COMMITED) {
    /**
     * This case only happens when: this instance is fast-takovered on
     * another server and that server sent a COMMIT for that instance
     * to all replicas
     * 
     * We can return even if it's committed but yet executed, since a committed
     * command is bound to get executed.
     */
    return;
  }
  
  ins->cmd = cmd;
  ins->status = Status::COMMITED;
#ifdef WAIT_AT_UNCOMMIT
  ins->cmit_evt.Set(1);
#endif

  auto& log_info = log_infos_[is_pilot];
  auto& another_log_info = log_infos_[REVERSE(is_pilot)];
  updateMaxCmtdSlot(log_info, slot);
  // verify(slot > log_info.max_executed_slot);

#ifndef WAIT_AT_UNCOMMIT
  /**
   * Q: should we execute commands here?
   * A: We may use another threads to execute the commands,
   * but for better programbility and understandability,
   * we should execute cmds here
   */
  /**
   * @Xuhao's note:
   * We should not only execute the committed cmd here, but also try to execute
   * cmds in both logs up-to max committed index. Cases are that later cmds have
   * committed before this cmd, and fail to execute due to this cmd had not been
   * committed at that time. The commit of this cmd makes executing later cmd possible.
   * 
   * If a cmd is committed but fail to execute due to uncommitted dependency, one way
   * for it to execute eventually is by executing a later cmd, which will execute all
   * cmds before it according to the order requirement. But a later cmd may not be
   * proposed if client is blocked and stop issuing new cmds. Thus the only way to
   * execute it in this case is to execute it when its dependent cmd commits, which is
   * bound to happen.
   */
  for (slotid_t i = slot; i <= log_info.max_committed_slot; i++) {
    auto is = GetInstance(i, is_pilot);
    executeCmds(is);
  }
  for (slotid_t i = std::max(dep, another_log_info.max_executed_slot + 1);
       i <= another_log_info.max_committed_slot; i++) {
    auto is = GetInstance(i, REVERSE(is_pilot));
    executeCmds(is);
  }
#else
#ifdef DEBUG
    executeCmd(ins);
#else
    executeCmds(ins); // log entry must be executed when return, otherwise deadlock may happen
#endif
#endif

  // TODO: should support snapshot for freeing memory.
  // for now just free anything 1000 slots before.
  int i = log_info.min_active_slot;
  while (i + N_CMD_KEEP < log_info.max_executed_slot) {
    // remove unused cmds. a removed cmd must have been executed.
    removeCmd(log_info, i++);
  }
  log_info.min_active_slot = i;
  // Log_info("server %d [COMMIT     ] %s : %ld -> %ld finish", id_, toString(is_pilot), slot, dep);
}

void CopilotServer::setIsPilot(bool isPilot) {
  verify(!isPilot || !isCopilot_);
  isPilot_ = isPilot;
  if (isPilot) {
    pingpong_ok_ = true;  // hand the ball to Pilot first
    last_ready_time_ = Time::now(true);
  }
}

void CopilotServer::setIsCopilot(bool isCopilot) {
  verify(!isCopilot || !isPilot_);
  isCopilot_ = isCopilot;
  if (isCopilot) {
    pingpong_ok_ = false;  // hand the ball to Pilot first
    last_ready_time_ = Time::now(true) + 2 * PINGPONG_TIMEOUT_US;
  }
}

inline void CopilotServer::updateMaxExecSlot(shared_ptr<CopilotData>& ins) {
  Log_debug("server %d [EXECUTE    ] %s : %lu -> %lu", id_, toString(ins->is_pilot), ins->slot_id, ins->dep_id);
  auto& log_info = log_infos_[ins->is_pilot];
  slotid_t i;
  for (i = log_info.max_executed_slot + 1; i <= ins->slot_id; i++) {
    auto& log_entry = log_info.logs[i];
    if (log_entry && GET_STATUS(log_entry->status) < Status::EXECUTED)
      break;
  }
  log_info.max_executed_slot = i - 1;
  Log_debug("server %d [max EXECUTE] %s : + %lu", id_, toString(ins->is_pilot), i - 1);
}

void CopilotServer::updateMaxAcptSlot(CopilotLogInfo& log_info, slotid_t slot) {
  slotid_t i;
  for (i = log_info.max_accepted_slot + 1; i <= slot; i++) {
    auto& log_entry = log_info.logs[i];
    if (log_entry && GET_STATUS(log_entry->status) < Status::FAST_ACCEPTED_EQ)
      break;
  }
  log_info.max_accepted_slot = i - 1;
}

void CopilotServer::updateMaxCmtdSlot(CopilotLogInfo& log_info, slotid_t slot) {
  slotid_t i;
  for (i = log_info.max_committed_slot + 1; i <= slot; i++) {
    auto& log_entry = log_info.logs[i];
    if (log_entry && GET_STATUS(log_entry->status) < Status::COMMITED)
      break;
  }
  log_info.max_committed_slot = i - 1;
  log_info.max_cmit_evt.Set(log_info.max_committed_slot);
}

void CopilotServer::removeCmd(CopilotLogInfo& log_info, slotid_t slot) {
  auto cmd = log_info.logs[slot]->cmd;
  if (cmd->kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto tpc_cmd = dynamic_pointer_cast<TpcCommitCommand>(cmd);
    tx_sched_->DestroyTx(tpc_cmd->tx_id_);
  } else if (cmd->kind_ == MarshallDeputy::CMD_TPC_BATCH) {
    auto batch_cmd = dynamic_pointer_cast<TpcBatchCommand>(cmd);
    for (auto& c : batch_cmd->cmds_)
      tx_sched_->DestroyTx(c->tx_id_);
  }
  log_info.logs.erase(slot);
}

bool CopilotServer::executeCmd(shared_ptr<CopilotData>& ins) {
  if (likely((bool)(ins->cmd))) {
    if (likely(ins->cmd->kind_ != MarshallDeputy::CMD_NOOP))
      app_next_(*ins->cmd);
    ins->status = Status::EXECUTED;
    updateMaxExecSlot(ins);
    return true;
  } else {
    Log_warn("server %d execute %s empty cmd %ld, status %x",
      id_, toString(ins->is_pilot), ins->slot_id, ins->status);
    verify(0);
    return false;
  }
}

#ifdef USE_TARJAN
bool CopilotServer::executeCmds(shared_ptr<CopilotData>& ins) {
  if (ins->status == Status::EXECUTED)
    return true;
  
  if (unlikely(ins->dep_id == 0 || ins->cmd->kind_ == MarshallDeputy::CMD_NOOP)) {
    return executeCmd(ins);
  } else {
#ifdef DEBUG
    return executeCmd(ins);
#else
    return findSCC(ins);
#endif
  }
}

#else

bool CopilotServer::executeCmds(shared_ptr<CopilotData>& ins) {
  if (ins->status == Status::EXECUTED)
    return true;
  
  auto p = ins->is_pilot;
  Log_debug("server %d execute %s : %ld from %ld", id_, toString(ins->is_pilot), ins->slot_id, log_infos_[p].max_executed_slot + 1);
  for (auto i = log_infos_[p].max_executed_slot + 1; i <= ins->slot_id; i++) {
    auto w = GetInstance(i, p);
    auto dep = GetInstance(w->dep_id, REVERSE(p));

    if (!dep)
      continue;

    if (GET_STATUS(w->status) == Status::EXECUTED) {
      // updateMaxExecSlot(w);
      continue;
    }

    if (GET_STATUS(w->status) < Status::COMMITED)
      w->cmit_evt.WaitUntilGreaterOrEqualThan(1);
    
    // if (w->status >= Status::COMMITED)
    //   log_infos_[p].max_dep = std::max(log_infos_[p].max_dep, w->dep_id);

    // case 1: no dependency, no-op, or dependency has been executed
    if (unlikely(w->dep_id == 0 ||
        w->cmd->kind_ == MarshallDeputy::CMD_NOOP) ||
        GET_STATUS(dep->status) == Status::EXECUTED) {
      executeCmd(w);
      continue;
    }

    // case 2: A cycle exists between entry and depEntry and currPilot has higher priority
    // (if it has lower priority, the cycle will be handled while processing the other pilot’s log)
    // bool has_cycle = log_infos_[REVERSE(p)].max_dep >= i;
    // if ((has_cycle && p == YES) ||
    //     (dep->dep_id >= i && p == YES && log_infos_[REVERSE(p)].max_executed_slot >= w->dep_id - 1)) {
    //   executeCmd(w);
    //   continue;
    // }

    // case 3: A cycle doesn't exist, must execute its dependency
    for (auto j = log_infos_[REVERSE(p)].max_executed_slot + 1; j <= w->dep_id; j++) {
      auto d = GetInstance(j, REVERSE(p));
      // case 1: A cycle exists and d is on thr lower priority. thus w is executable
      if ((d->dep_id >= i) && (p == YES))
        break;

      if (EliminateNullDep(d))
         continue;
      
      // case 2: cycle doesn't exist or d is on the higher priority, must wait after d commits
      d->cmit_evt.WaitUntilGreaterOrEqualThan(1);
      if (GET_STATUS(d->status) >= Status::EXECUTED)
        // case 2.1: d already executed else where
        continue;
      else if (d->dep_id == 0 || d->cmd->kind_ == MarshallDeputy::CMD_NOOP) {
        // case 2.2: d has no dep or d is no-op
        executeCmd(d);
      } else if (d->dep_id >= i) {
        // case 2.3: A cycle exists
        if (p == YES)
          // case 2.3.1: d is on lower priority. w is executable
          break;
        else
          // case 2.3.2: d is on higher priority. d is executable
          executeCmd(d);
      } else {
        // case 2.4: cycle doesn't exist, d depends on entry before w, which must already be executed
        executeCmd(d);
      }
    }

    // dependency executed, execute w
    executeCmd(w);
    // at the end of this iteration, w must be executed, otherwise we can't proceed to next entry
  }

  return true;
}

#endif

bool CopilotServer::checkAllDepExecuted(uint8_t is_pilot, slotid_t start, slotid_t end) {
  for (slotid_t i = start; i <= end; i++) {
    auto ins = GetInstance(i, is_pilot);
    if (!EliminateNullDep(ins))
      return false;
  }

  return true;
}

bool CopilotServer::isExecuted(shared_ptr<CopilotData>& ins) {
  uint8_t p = ins->is_pilot;
  return log_infos_[p].max_executed_slot >= ins->slot_id;
}

/**************************************************************************

Traditional way to do command execution. Abort execution at unCOMMITed cmd

adopted from:
https://github.com/efficient/epaxos/blob/master/src/epaxos/epaxos-exec.go
https://github.com/PlatformLab/epaxos/blob/master/src/epaxos/epaxos-exec.go

**************************************************************************/
#ifndef WAIT_AT_UNCOMMIT

bool CopilotServer::findSCC(shared_ptr<CopilotData>& root) {
  int index = 1;
  while (!stack_.empty()) {
    // auto v = stack_.top();
    // v->dfn = 0;
    stack_.pop();
  }
  
  return strongConnect(root, &index);
}

bool CopilotServer::strongConnect(shared_ptr<CopilotData>& ins, int* index) {
  ins->dfn = *index;
  ins->low = *index;
  *index = *index + 1;
  stack_.push(ins);
  Log_debug("SCC %s : %lu -> %lu (%d, %d)", toString(ins->is_pilot), ins->slot_id, ins->dep_id, ins->dfn, ins->low);

  std::vector<uint8_t> order = ins->is_pilot ? std::vector<uint8_t>{YES, NO}
                                             : std::vector<uint8_t>{NO, YES};

  for (auto& p : order) {
    // first handle own side, then handle another side
    auto end = (p == ins->is_pilot) ? ins->slot_id - 1 : ins->dep_id;
    for (auto i = log_infos_[p].max_executed_slot + 1; i <= end; i++) {
      auto w = GetInstance(i, p);

      if (w->status == Status::EXECUTED) continue;

      if (w->status < Status::COMMITED) {
        // TODO: this cmd has not been committed, wait or return?
        Log_debug("%d, unCOMMITTED cmd %s : %lu -> %lu", id_, toString(w->is_pilot), w->slot_id, w->dep_id);
        ins->dfn = 0;
        return false;
      }

      if (w->dfn == 0) {
        if (!strongConnect(w, index)) {
          // find uncommitted cmd, abort execution and pop out all stacked cmds
          shared_ptr<CopilotData> v;
          do {
            v = stack_.top();
            v->dfn = 0;
            stack_.pop();
          } while (v != ins);
          return false;
        }
        ins->low = std::min(ins->low, w->low);
      } else {
        ins->low = std::min(w->dfn, ins->low);
      }
    }
  }

  if (ins->low == ins->dfn) {
    std::vector<shared_ptr<CopilotData> > list;
    shared_ptr<CopilotData> w;
    
    do {
      w = stack_.top();
      stack_.pop();
      list.push_back(w);
    } while (w != ins);

    std::sort(list.begin(), list.end(),
              [](const shared_ptr<CopilotData>& i1,
                 const shared_ptr<CopilotData>& i2) -> bool {
                if (i1->is_pilot == i2->is_pilot)
                  return i1->slot_id < i2->slot_id;
                else
                  return i1->is_pilot;
              });
    
    for (auto& c : list) {
      executeCmd(c);
    }
  }

  return true;
}

#else

/**************************************************************************

     New way to do command execution. Wait and yield at unCOMMITed cmd

**************************************************************************/

/**
 * DFS traverse the dependence graph (iteratively), if encountered an uncommitted cmd, wait and yield,
 * stop at EXECUTED cmd. There can be multiple DFS instance going in parallel, so each
 * DFS has an independent visited map.
 */
void CopilotServer::waitAllPredCommit(shared_ptr<CopilotData>& ins) {
  visited_map_t visited;
  copilot_stack_t stack;

  stack.push(ins);

  while (!stack.empty()) {
    auto w = stack.top();
    stack.pop();

    if (visited[w])
      continue;
    
    visited[w] = true;
    auto pre_ins = GetInstance(w->slot_id - 1, w->is_pilot);
    auto dep_ins = GetInstance(w->dep_id, REVERSE(w->is_pilot));

    if (pre_ins && !isExecuted(pre_ins) && !visited[pre_ins]) {
      if (pre_ins->status < COMMITED)
        pre_ins->cmit_evt.WaitUntilGreaterOrEqualThan(1);
      stack.push(pre_ins);
    }

    if (dep_ins && !isExecuted(dep_ins) && !visited[dep_ins]) {
      if (dep_ins->status < COMMITED)
        dep_ins->cmit_evt.WaitUntilGreaterOrEqualThan(1);
      stack.push(dep_ins);
    }
  }

  // waitPredCmds(ins, visited);
}

/**
 * DFS traverse the dependence graph (recursively), if encountered an uncommitted cmd, wait and yield,
 * stop at EXECUTED cmd. There can be multiple DFS instance going in parallel, so each
 * DFS has an independent visited map.
 */
void CopilotServer::waitPredCmds(shared_ptr<CopilotData>& w, shared_ptr<visited_map_t> m) {
  (*m)[w] = true;
  auto pre_ins = GetInstance(w->slot_id - 1, w->is_pilot);
  auto dep_ins = GetInstance(w->dep_id, REVERSE(w->is_pilot));

  if (pre_ins && pre_ins->status < EXECUTED && (m->find(pre_ins) == m->end())) {
    if (pre_ins->status < COMMITED)
      pre_ins->cmit_evt.WaitUntilGreaterOrEqualThan(1);
    waitPredCmds(pre_ins, m);
  }

  if (dep_ins && dep_ins->status < EXECUTED && (m->find(dep_ins) == m->end())) {
    if (dep_ins->status < COMMITED)
      dep_ins->cmit_evt.WaitUntilGreaterOrEqualThan(1);
    waitPredCmds(dep_ins, m);
  }
}

bool CopilotServer::findSCC(shared_ptr<CopilotData>& root) {
  int index = 1;
  copilot_stack_t stack;
  waitAllPredCommit(root);
  return strongConnect(root, &index);
}

bool CopilotServer::strongConnect(shared_ptr<CopilotData>& ins, int* index) {
  // at this time all predecessor cmds must have committed so this function must complete successfully
  ins->dfn = *index;
  ins->low = *index;
  *index = *index + 1;
  stack_.push(ins);
  verify(ins);
  // Log_debug("SCC %s : %lu -> %lu (%d, %d)", toString(ins->is_pilot), ins->slot_id, ins->dep_id, ins->dfn, ins->low);


  std::vector<uint8_t> order = ins->is_pilot ? std::vector<uint8_t>{YES, NO}
                                             : std::vector<uint8_t>{NO, YES};
  
  for (auto& p : order) {
    auto end = (p == ins->is_pilot) ? ins->slot_id - 1 : ins->dep_id;
    for (auto i = log_infos_[p].max_executed_slot + 1; i <= end; i++) {
      auto w = GetInstance(i, p);

      if (w->status == Status::EXECUTED) continue;

      if (w->status < Status::COMMITED) {
        verify(0); // should not happen
      }

      if (w->dfn == 0) {
        strongConnect(w, index);
        ins->low = std::min(ins->low, w->low);
      } else {
        ins->low = std::min(w->dfn, ins->low);
      }
    }
  }

  if (ins->low == ins->dfn) {
    std::vector<shared_ptr<CopilotData> > list;
    shared_ptr<CopilotData> w;
    
    do {
      w = stack_.top();
      stack_.pop();
      list.push_back(w);
    } while (w != ins);

    std::sort(list.begin(), list.end(),
              [](const shared_ptr<CopilotData>& i1,
                 const shared_ptr<CopilotData>& i2) -> bool {
                if (i1->is_pilot == i2->is_pilot)
                  return i1->slot_id < i2->slot_id;
                else
                  return i1->is_pilot;
              });
    
    for (auto& c : list) {
      executeCmd(c);
    }
  }

  return true;
}

#endif

} // namespace janus
