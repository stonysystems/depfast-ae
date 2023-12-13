#include "server.h"
#include "frame.h"
#if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
#include "../classic/tpc_command.h"
#endif


namespace janus {

EpaxosServer::EpaxosServer(Frame * frame) {
  frame_ = frame;
}

EpaxosServer::~EpaxosServer() {}

/***********************************
   Main event processing loop      *
************************************/

void EpaxosServer::Setup() {
  // Future Work: Increment epoch and give unique replica id on restarts (store old replica_id persistently and new_replica_id = old_replica_id + N)
  curr_replica_id = site_id_;

  #ifdef EPAXOS_TEST_CORO
  // Process requests from queue
  Coroutine::CreateRun([this](){
    while(true) {
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (reqs.empty()) {
        lock.unlock();
        Coroutine::Sleep(1000);
        continue;
      }
      list<EpaxosRequest> pending_reqs = std::move(reqs);
      lock.unlock();
      for (EpaxosRequest req : pending_reqs) {
        Coroutine::CreateRun([this, req]() mutable {
          HandleRequest(req.cmd, req.dkey, [this, req]() {
            app_next_(*(req.cmd));
          });
        });
      }
      Coroutine::Sleep(10);
    }
  });

  // Prepare requests from prepare queue
  Coroutine::CreateRun([this](){
    while(true) {
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (prepare_reqs.empty()) {
        lock.unlock();
        Coroutine::Sleep(10000);
        continue;
      }
      auto req = prepare_reqs.front();
      prepare_reqs.pop_front();
      lock.unlock();
      Coroutine::CreateRun([this, req]() mutable {
        PrepareTillCommitted(req.first, req.second);
        StartExecution(req.first, req.second);
      });
    }
  });
  #endif

  // Execute committed commands and explicitly prepare if required
  Coroutine::CreateRun([this](){
    while(true) {
      #ifdef EPAXOS_TEST_CORO
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (pause_execution) { 
        lock.unlock();
        Coroutine::Sleep(10000);
        continue;
      }
      lock.unlock();
      #endif

      for (auto itr : this->received_till) {
        uint64_t replica_id = itr.first;
        uint64_t received_till_instance_no = itr.second;
        uint64_t instance_no = prepared_till.count(replica_id) ? prepared_till[replica_id] + 1 : 0;
        while (instance_no <= received_till_instance_no) {
          Coroutine::CreateRun([this, replica_id, instance_no]() mutable {
            shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
            ecmd->committed_ev.WaitUntilGreaterOrEqualThan(1, commit_timeout);
            PrepareTillCommitted(replica_id, instance_no);
            StartExecution(replica_id, instance_no);
          });
          instance_no++;
        }
      }
      prepared_till = this->received_till;
      Coroutine::Sleep(10);
    }
  });
}

#if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_SERVER_METRICS_COLLECTION)

/***********************************
      Test Request Handlers      *
************************************/

void EpaxosServer::Start(shared_ptr<Marshallable>& cmd, string dkey) {
  Log_debug("Received request in server: %d for dkey: %s", site_id_, dkey.c_str());
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  reqs.push_back(EpaxosRequest(cmd, dkey));
}

void EpaxosServer::SetInstance(shared_ptr<Marshallable>& cmd, uint64_t& replica_id, uint64_t& instance_no) {
  auto& command = dynamic_cast<TpcCommitCommand&>(*cmd);
  instance[command.tx_id_] = make_pair(replica_id, instance_no);
}

pair<int64_t, int64_t> EpaxosServer::GetInstance(int& cmd) {
  if (instance.count(cmd) == 0) {
    return make_pair(-1, -1);
  }
  return instance[cmd];
}

void EpaxosServer::GetState(uint64_t& replica_id, 
                            uint64_t& instance_no, 
                            shared_ptr<Marshallable> *cmd, 
                            string *dkey,
                            uint64_t *seq, 
                            map<uint64_t, uint64_t> *deps, 
                            status_t *state) {
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  *cmd = ecmd->cmd;
  *dkey = ecmd->dkey;
  *deps = ecmd->deps;
  *seq = ecmd->seq;
  *state = ecmd->state;
}

void EpaxosServer::Prepare(uint64_t& replica_id, uint64_t& instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("Received prepare request in server: %d for replica: %d instance: %d", site_id_, replica_id, instance_no);
  prepare_reqs.push_back(make_pair(replica_id, instance_no));
}

void EpaxosServer::PauseExecution(bool pause) {
  pause_execution = pause;
}

pair<int, int> EpaxosServer::GetFastAndSlowPathCount() {
  return make_pair(fast, slow);
}

pair<vector<float>, vector<float>> EpaxosServer::GetLatencies() {
  return make_pair(commit_times, exec_times);
}
#endif

/***********************************************
            Client Request Handlers            *
************************************************/

void EpaxosServer::Start(shared_ptr<Marshallable>& cmd, string dkey, const function<void()> &cb) {
  Log_debug("Received request in server: %d for dkey: %s", site_id_, dkey.c_str());
  #ifdef EPAXOS_SERVER_METRICS_COLLECTION
  auto& command = dynamic_cast<TpcCommitCommand&>(*cmd);
  start_times[command.tx_id_].start();
  #endif
  HandleRequest(cmd, dkey, cb);
}

void EpaxosServer::HandleRequest(shared_ptr<Marshallable>& cmd, string& dkey, const function<void()> &cb) {
  int64_t leader_prev_dep_instance_no = -1;
  uint64_t curr_instance_no = next_instance_no++;
  ballot_t ballot = GetInitialBallot();
  if (dkey_deps[dkey].count(curr_replica_id)) {
    leader_prev_dep_instance_no = dkey_deps[dkey][curr_replica_id];
  }
  dkey_deps[dkey][curr_replica_id] = curr_instance_no; // Important - otherwise next command may not have dependency on this command
  #if defined(EPAXOS_TEST_CORO)
  SetInstance(cmd, curr_replica_id, curr_instance_no);
  #endif
  GetCommand(curr_replica_id, curr_instance_no)->callback = cb;
  StartPreAccept(cmd, dkey, ballot, curr_replica_id, curr_instance_no, leader_prev_dep_instance_no, false);
}

/***********************************
       Phase 1: Pre-accept         *
************************************/

bool EpaxosServer::StartPreAccept(shared_ptr<Marshallable>& cmd, 
                                  string& dkey, 
                                  ballot_t& ballot, 
                                  uint64_t& replica_id,
                                  uint64_t& instance_no,
                                  int64_t leader_prev_dep_instance_no,
                                  bool recovery) {
  Log_debug("Started pre-accept for request for replica: %d instance: %d dkey: %s with leader_prev_dep_instance_no: %d ballot: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), leader_prev_dep_instance_no, ballot, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject old message - we have moved on
  if (ecmd->state >= EpaxosCommandState::ACCEPTED || ballot < ecmd->highest_seen) {
    return false;
  }
  // Get latest attributes
  uint64_t seq;
  map<uint64_t, uint64_t> deps;
  GetLatestAttributes(dkey, replica_id, leader_prev_dep_instance_no, &seq, &deps);
  // Pre-accept command
  ecmd->replica_id = replica_id;
  ecmd->instance_no = instance_no;
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = seq;
  ecmd->deps = deps;
  ecmd->highest_seen = ballot;
  ecmd->highest_accepted = ballot;
  ecmd->state = EpaxosCommandState::PRE_ACCEPTED;
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, seq);
  // Send pre-accept requests
  auto ev = commo()->SendPreAccept(site_id_, 
                                   partition_id_, 
                                   recovery,
                                   ballot, 
                                   replica_id,
                                   instance_no, 
                                   cmd, 
                                   dkey, 
                                   seq, 
                                   deps);
  ev->Wait(rpc_timeout);
  // Process pre-accept replies
  Log_debug("Started pre-accept reply processing for replica: %d instance: %d dkey: %s with leader_prev_dep_instance_no: %d ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), leader_prev_dep_instance_no, ballot, replica_id, curr_replica_id);
  // Fail if timeout/no-majority
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Pre-accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success via fast path quorum
  if (ev->FastPath() && IsInitialBallot(ballot) && AreAllDependenciesCommitted(ev->replies, ev->eq_reply.deps)) {
    #ifdef EPAXOS_SERVER_METRICS_COLLECTION
    fast++;
    #endif
    Log_debug("Fastpath for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return StartCommit(cmd, dkey, ev->eq_reply.seq, ev->eq_reply.deps, replica_id, instance_no);
  }
  // Success via slow path quorum
  #ifdef EPAXOS_SERVER_METRICS_COLLECTION
  slow++;
  #endif
  Log_debug("Slowpath for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  // Merge attributes
  uint64_t merged_seq;
  map<uint64_t, uint64_t> merged_deps;
  MergeAttributes(ev->replies, &merged_seq, &merged_deps);
  // Start accept phase
  return StartAccept(cmd, dkey, ballot, merged_seq, merged_deps, replica_id, instance_no);
}

EpaxosPreAcceptReply EpaxosServer::OnPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                      string dkey, 
                                                      ballot_t ballot, 
                                                      uint64_t seq, 
                                                      map<uint64_t, uint64_t> deps, 
                                                      uint64_t replica_id, 
                                                      uint64_t instance_no) {
  Log_debug("Received pre-accept request for replica: %d instance: %d dkey: %s with ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), ballot, replica_id, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject older ballots
  if (ballot < ecmd->highest_seen) {
    return EpaxosPreAcceptReply(EpaxosPreAcceptStatus::FAILED, ecmd->highest_seen);
  }
  ecmd->highest_seen = ballot;
  /* - If it was pre-accepted by majority identically then it will never come to pre-accept phase again and instead go to accept phase.
     - If it was accepted by majority of replica then it will never come to pre-accept phase again and instead go to accept phase.
     - If it was committed in some other replica then it will never come to pre-accept phase again and instead go to accept phase.
     - If already committed or executed in this replica then it is just an delayed message so fail it (same as ignore). 
     - If already accepted in this replica then it means it was pre-accepted by some majority non-identically and accepted by the 
       leader. So prepare should have tried to accept this message again. So reject it so that prepare will try again.
       If new cmd is NOOP, then it will overwrite the accept when commit request comes. */
  if (ecmd->state >= EpaxosCommandState::ACCEPTED) {
    return EpaxosPreAcceptReply(EpaxosPreAcceptStatus::FAILED, ballot);
  }
  EpaxosPreAcceptStatus status = EpaxosPreAcceptStatus::IDENTICAL;
  // Initialise attributes
  uint64_t merged_seq = seq;
  auto merged_deps = deps;
  if (dkey != NOOP_DKEY) {
    merged_seq = max(merged_seq, dkey_seq[dkey] + 1);
    if (merged_seq > seq) {
      status = EpaxosPreAcceptStatus::NON_IDENTICAL;
    }
    for (auto itr : dkey_deps[dkey]) {
      uint64_t dreplica_id = itr.first;
      uint64_t dinstance_no = itr.second;
      if (dreplica_id == replica_id) continue;
      if (merged_deps.count(dreplica_id) == 0 || dinstance_no > merged_deps[dreplica_id]) {
        merged_deps[dreplica_id] = dinstance_no;
        status = EpaxosPreAcceptStatus::NON_IDENTICAL;
      }
    }
  }
  // Get list of replicas with committed deps
  unordered_set<uint64_t> committed_deps = GetReplicasWithAllDependenciesCommitted(merged_deps);
  // Eq state not set if ballot is not default ballot
  if (!IsInitialBallot(ballot)) {
    status = EpaxosPreAcceptStatus::NON_IDENTICAL;
  }
  // Pre-accept command
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = merged_seq;
  ecmd->deps = merged_deps;
  ecmd->highest_accepted = ballot;
  ecmd->state = status == EpaxosPreAcceptStatus::IDENTICAL ? 
                                        EpaxosCommandState::PRE_ACCEPTED_EQ : 
                                        EpaxosCommandState::PRE_ACCEPTED;
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, merged_seq);
  // Reply
  return EpaxosPreAcceptReply(status, ballot, merged_seq, merged_deps, committed_deps);
}

/***********************************
         Phase 2: Accept           *
************************************/

bool EpaxosServer::StartAccept(shared_ptr<Marshallable>& cmd, 
                               string& dkey, 
                               ballot_t& ballot, 
                               uint64_t& seq,
                               map<uint64_t, uint64_t>& deps, 
                               uint64_t& replica_id, 
                               uint64_t& instance_no) {
  Log_debug("Started accept request for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject old message - we have moved on
  if (ecmd->state >= EpaxosCommandState::COMMITTED || ballot < ecmd->highest_seen) {
    return false;
  }
  // Accept command
  ecmd->replica_id = replica_id;
  ecmd->instance_no = instance_no;
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = seq;
  ecmd->deps = deps;
  ecmd->highest_seen = ballot;
  ecmd->highest_accepted = ballot;
  ecmd->state = EpaxosCommandState::ACCEPTED;
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, seq);
  // Send accept requests
  auto ev = commo()->SendAccept(site_id_, 
                                partition_id_, 
                                ballot, 
                                replica_id,
                                instance_no, 
                                cmd, 
                                dkey, 
                                seq, 
                                deps);
  ev->Wait(rpc_timeout);
  // Process accept replies
  Log_debug("Started accept reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  // Fail if timeout/no-majority
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success
  return StartCommit(cmd, dkey, seq, deps, replica_id, instance_no);
}

EpaxosAcceptReply EpaxosServer::OnAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                string dkey, 
                                                ballot_t ballot, 
                                                uint64_t seq,
                                                map<uint64_t, uint64_t> deps, 
                                                uint64_t replica_id, 
                                                uint64_t instance_no) {
  Log_debug("Received accept request for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject older ballots
  if (ballot < ecmd->highest_seen) {
    return EpaxosAcceptReply(false, ecmd->highest_seen);
  }
  ecmd->highest_seen = ballot;
  // Accept command
  /* - If already committed or executed in this replica then it is just an delayed message so fail it (same as ignore). 
     - If already accepted in this replica then it can be still overwritten because majority haven't agreed to it identically. */
  if (ecmd->state >= EpaxosCommandState::COMMITTED) {
    return EpaxosAcceptReply(false, ballot);
  }
  ecmd->replica_id = replica_id;
  ecmd->instance_no = instance_no;
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = seq;
  ecmd->deps = deps;
  ecmd->highest_accepted = ballot;
  ecmd->state = EpaxosCommandState::ACCEPTED;
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, seq);
  // Reply
  return EpaxosAcceptReply(true, ballot);
}

/***********************************
          Commit Phase             *
************************************/

bool EpaxosServer::StartCommit(shared_ptr<Marshallable>& cmd, 
                               string& dkey, 
                               uint64_t& seq,
                               map<uint64_t, uint64_t>& deps, 
                               uint64_t& replica_id, 
                               uint64_t& instance_no) {
  Log_debug("Started commit request for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Commit command if not committed
  if (ecmd->state < EpaxosCommandState::COMMITTED) {
    ecmd->replica_id = replica_id;
    ecmd->instance_no = instance_no;
    ecmd->cmd = cmd;
    ecmd->dkey = dkey;
    ecmd->seq = seq;
    ecmd->deps = deps;
    ecmd->state = EpaxosCommandState::COMMITTED;
    #ifdef EPAXOS_SERVER_METRICS_COLLECTION
    if (replica_id == curr_replica_id) {
      auto& command = dynamic_cast<TpcCommitCommand&>(*cmd);
      commit_times.push_back(start_times[command.tx_id_].elapsed());
    }
    #endif
    ecmd->committed_ev.Set(1);
    // Update attributes
    UpdateAttributes(dkey, replica_id, instance_no, seq);
    // Update committed till
    UpdateCommittedTill(replica_id, instance_no);
  }
  Log_debug("Committed replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  // Send async commit request to all
  commo()->SendCommit(site_id_, 
                      partition_id_, 
                      replica_id,
                      instance_no, 
                      cmd, 
                      dkey, 
                      seq, 
                      deps);
  return true;
}

void EpaxosServer::OnCommitRequest(shared_ptr<Marshallable>& cmd, 
                                   string dkey, 
                                   uint64_t seq,
                                   map<uint64_t, uint64_t> deps, 
                                   uint64_t replica_id, 
                                   uint64_t instance_no) {
  Log_debug("Received commit request for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Commit command if not committed
  if (ecmd->state >= EpaxosCommandState::COMMITTED) {
    return;
  }
  ecmd->replica_id = replica_id;
  ecmd->instance_no = instance_no;
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = seq;
  ecmd->deps = deps;
  ecmd->state = EpaxosCommandState::COMMITTED;
  ecmd->committed_ev.Set(1);
  #ifdef EPAXOS_SERVER_METRICS_COLLECTION
  if (replica_id == curr_replica_id) {
    auto& command = dynamic_cast<TpcCommitCommand&>(*cmd);
    commit_times.push_back(start_times[command.tx_id_].elapsed());
  }
  #endif
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, seq);
  // Update committed till
  UpdateCommittedTill(replica_id, instance_no);
}

/***********************************
       TryPreAccept Phase          *
************************************/
bool EpaxosServer::StartTryPreAccept(shared_ptr<Marshallable>& cmd, 
                                     string& dkey, 
                                     ballot_t& ballot, 
                                     uint64_t& seq,
                                     map<uint64_t, uint64_t>& deps, 
                                     uint64_t& replica_id, 
                                     uint64_t& instance_no,
                                     int64_t leader_prev_dep_instance_no,
                                     unordered_set<siteid_t>& preaccepted_sites) {
  Log_debug("Started try-pre-accept for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject old message - we have moved on
  if (ecmd->state >= EpaxosCommandState::ACCEPTED || ballot < ecmd->highest_seen) {
    return false;
  }
  // Add self reply
  EpaxosTryPreAcceptReply self_reply(EpaxosTryPreAcceptStatus::NO_CONFLICT, ballot, 0, 0);
  if (preaccepted_sites.count(site_id_) != 0) {
    preaccepted_sites.erase(site_id_);
  } else {
    EpaxosTryPreAcceptStatus conflict_state;
    uint64_t conflict_replica_id;
    uint64_t conflict_instance_no;
    FindTryPreAcceptConflict(cmd, dkey, seq, deps, replica_id, instance_no, &conflict_state, &conflict_replica_id, &conflict_instance_no);
    // committed conflict
    if (conflict_state == EpaxosTryPreAcceptStatus::COMMITTED_CONFLICT) {
      return StartPreAccept(cmd, dkey, ballot, replica_id, instance_no, leader_prev_dep_instance_no, true);
    } 
    // Pre-accept command
    if (conflict_state == EpaxosTryPreAcceptStatus::NO_CONFLICT) {
      ecmd->replica_id = replica_id;
      ecmd->instance_no = instance_no;
      ecmd->cmd = cmd;
      ecmd->dkey = dkey;
      ecmd->seq = seq;
      ecmd->deps = deps;
      ecmd->highest_seen = ballot;
      ecmd->highest_accepted = ballot;
      ecmd->state = EpaxosCommandState::PRE_ACCEPTED;
    } else {
      self_reply.status = EpaxosTryPreAcceptStatus::UNCOMMITTED_CONFLICT;
      self_reply.conflict_replica_id = conflict_replica_id;
      self_reply.conflict_instance_no = conflict_instance_no;
      Log_debug("New leader has conflict in try-pre-accept for replica: %d instance: %d by replica: %d conflicted by replica: %d instance: %d", replica_id, instance_no, curr_replica_id, conflict_replica_id, conflict_instance_no);
    }
  }
  // Send try-pre-accept requests
  auto ev = commo()->SendTryPreAccept(site_id_, 
                                      partition_id_,
                                      preaccepted_sites,
                                      ballot, 
                                      replica_id,
                                      instance_no, 
                                      cmd, 
                                      dkey, 
                                      seq, 
                                      deps);
  ev->VoteYes(self_reply);
  Log_debug("Added try-pre-accept self-reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  ev->Wait(rpc_timeout);
  // Process try-pre-accept replies
  Log_debug("Started try-pre-accept reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  // Fail if timeout
  if (ev->status_ == Event::TIMEOUT) {
    Log_debug("Try-pre-accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success
  if (ev->NoConflict()) {
    Log_debug("Try-pre-accept success for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return StartAccept(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  }
  // Old message - moved on
  if (ev->MovedOn()) {
    Log_debug("Try-pre-accept moved on for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return false;
  }
  // Committed conflict
  if (ev->CommittedConflict()) {
    Log_debug("Try-pre-accept committed conflict for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return StartPreAccept(cmd, dkey, ballot, replica_id, instance_no, leader_prev_dep_instance_no, true);
  }
  // Fail if no-majority
  if (ev->No()) {
    Log_debug("Try-pre-accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Defer
  Log_debug("Try-pre-accept conflict for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  bool is_deferred = deferred[replica_id].count(instance_no) != 0;
  pair<uint64_t, uint64_t> deferred_inst, conflict_inst;
  if (is_deferred) {
    deferred_inst = deferred[replica_id][instance_no];
  }
  for (auto reply : ev->replies) {
    if (reply.status != EpaxosTryPreAcceptStatus::UNCOMMITTED_CONFLICT) {
      continue;
    }
    if (is_deferred && deferred_inst.first == reply.conflict_replica_id) {
      Log_debug("Try-pre-accept loop for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
      return StartPreAccept(cmd, dkey, ballot, replica_id, instance_no, leader_prev_dep_instance_no, true);
    }
    conflict_inst = make_pair(reply.conflict_replica_id, reply.conflict_instance_no);
  }
  deferred[conflict_inst.first][conflict_inst.second] = make_pair(replica_id, instance_no);
  Log_debug("Try-pre-accept deferred for replica: %d instance: %d by replica: %d conflicted by replica: %d instance: %d", replica_id, instance_no, curr_replica_id, conflict_inst.first, conflict_inst.second);
  return false;
}

EpaxosTryPreAcceptReply EpaxosServer::OnTryPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                            string dkey, 
                                                            ballot_t ballot, 
                                                            uint64_t seq, 
                                                            map<uint64_t, uint64_t> deps, 
                                                            uint64_t replica_id, 
                                                            uint64_t instance_no) {
  Log_debug("Received try-pre-accept request for replica: %d instance: %d dkey: %s with ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), ballot, replica_id, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject older ballots
  if (ballot < ecmd->highest_seen) {
    return EpaxosTryPreAcceptReply(EpaxosTryPreAcceptStatus::REJECTED, ecmd->highest_seen, 0, 0);
  }
  ecmd->highest_seen = ballot;
  // Reject old messages - we have moved on
  if (ecmd->state >= EpaxosCommandState::ACCEPTED) {
    return EpaxosTryPreAcceptReply(EpaxosTryPreAcceptStatus::MOVED_ON, ballot, 0, 0);
  }
  // Find conflict (if any)
  EpaxosTryPreAcceptStatus conflict_state;
  uint64_t conflict_replica_id;
  uint64_t conflict_instance_no;
  FindTryPreAcceptConflict(cmd, dkey, seq, deps, replica_id, instance_no, &conflict_state, &conflict_replica_id, &conflict_instance_no);
  // Reply conflict
  if (conflict_state == EpaxosTryPreAcceptStatus::COMMITTED_CONFLICT || conflict_state == EpaxosTryPreAcceptStatus::UNCOMMITTED_CONFLICT) {
    return EpaxosTryPreAcceptReply(conflict_state, ballot, conflict_replica_id, conflict_instance_no);
  }
  // Pre-accept command
  ecmd->replica_id = replica_id;
  ecmd->instance_no = instance_no;
  ecmd->cmd = cmd;
  ecmd->dkey = dkey;
  ecmd->seq = seq;
  ecmd->deps = deps;
  ecmd->highest_accepted = ballot;
  ecmd->state = EpaxosCommandState::PRE_ACCEPTED;
  // Update attributes
  UpdateAttributes(dkey, replica_id, instance_no, seq);
  // Reply
  return EpaxosTryPreAcceptReply(EpaxosTryPreAcceptStatus::NO_CONFLICT, ballot, 0, 0);
}

/***********************************
         Prepare Phase             *
************************************/

bool EpaxosServer::StartPrepare(uint64_t& replica_id, uint64_t& instance_no) {
  // Create prepare reply from self
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  EpaxosPrepareReply self_reply = EpaxosPrepareReply(true, 
                                                     ecmd->cmd, 
                                                     ecmd->dkey, 
                                                     ecmd->seq,
                                                     ecmd->deps,
                                                     ecmd->state,
                                                     curr_replica_id,
                                                     ecmd->highest_accepted);
  // Increment ballot
  ballot_t ballot = GetInitialBallot();
  if (ecmd->highest_seen > ballot) {
    ballot = ecmd->highest_seen;
  }
  ballot = GetNextBallot(ballot);
  ecmd->highest_seen = ballot;
  Log_info("Started prepare for replica: %d instance: %d by replica: %d for ballot: %d", replica_id, instance_no, curr_replica_id, ballot);
  // Send prepare requests
  auto ev = commo()->SendPrepare(site_id_, 
                                partition_id_, 
                                ballot, 
                                replica_id,
                                instance_no);
  ev->Wait(rpc_timeout);
  // Process prepare replies
  Log_debug("Started prepare reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Prepare failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Add self reply
  ev->replies.push_back(self_reply);
  // Recovery conditions
  ballot_t highest_accepted_ballot = -1;
  EpaxosPrepareReply rec_command;
  unordered_set<siteid_t> identical_preaccepted_sites;
  bool leader_replied = false;
  for (auto reply : ev->replies) {
    if (!reply.status || reply.cmd_state == EpaxosCommandState::NOT_STARTED) continue;
    // Atleast one commited reply
    if (reply.cmd_state >= EpaxosCommandState::COMMITTED) {
      Log_debug("Prepare - committed cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, curr_replica_id, reply.acceptor_replica_id);
      return StartCommit(reply.cmd, reply.dkey, reply.seq, reply.deps, replica_id, instance_no);
    }
    // Atleast one accept reply
    if (reply.cmd_state == EpaxosCommandState::ACCEPTED && reply.ballot > highest_accepted_ballot) {
      Log_debug("Prepare - accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, curr_replica_id, reply.acceptor_replica_id);
      rec_command = reply;
      highest_accepted_ballot = reply.ballot;
    }
    // Identical/non-identical replies of default ballot
    else if ((reply.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ || reply.cmd_state == EpaxosCommandState::PRE_ACCEPTED)
              && rec_command.cmd_state != EpaxosCommandState::ACCEPTED) {
      if (rec_command.cmd_state == EpaxosCommandState::NOT_STARTED) {
        Log_debug("Prepare - one pre-accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, curr_replica_id, reply.acceptor_replica_id);
        rec_command = reply;
        identical_preaccepted_sites.insert(ev->replicaid_siteid_map[reply.acceptor_replica_id]);
      } else if (reply.seq == rec_command.seq && reply.deps == rec_command.deps) {
        Log_debug("Prepare - similar pre-accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, curr_replica_id, reply.acceptor_replica_id);
        identical_preaccepted_sites.insert(ev->replicaid_siteid_map[reply.acceptor_replica_id]);
      } else if (reply.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ) {
        Log_debug("Prepare - identical pre-accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, curr_replica_id, reply.acceptor_replica_id);
        rec_command = reply;
        identical_preaccepted_sites = unordered_set<siteid_t>();
      }
      if (reply.acceptor_replica_id == replica_id) {
        leader_replied = true;
      }
    }
  }
  // Atleast one accepted reply - start phase accept
  if (rec_command.cmd_state == EpaxosCommandState::ACCEPTED) {
    Log_debug("Prepare - Atleast one accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return StartAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no);
  }
  // N/2 identical pre-accepted replies for default ballot
  if (!leader_replied && identical_preaccepted_sites.size() >= NSERVERS/2 && (commo()->thrifty || rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ)) {
    Log_debug("Prepare - Majority identical pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    return StartAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no);
  }
  // (F+1)/2 identical pre-accepted replies for default ballot
  if (!leader_replied && rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ && identical_preaccepted_sites.size() >= (NSERVERS/2 + 1)/2) {
    Log_debug("Prepare - (F+1)/2 identical pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    uint64_t leader_deps_instance = rec_command.deps.count(replica_id) ? rec_command.deps[replica_id] : -1;
    return StartTryPreAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no, leader_deps_instance, identical_preaccepted_sites);
  }
  // Atleast one pre-accepted reply - start phase pre-accept
  if (rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED || rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ) {
    Log_debug("Prepare - Atleast one pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
    uint64_t leader_deps_instance = rec_command.deps.count(replica_id) ? rec_command.deps[replica_id] : -1;
    return StartPreAccept(rec_command.cmd, rec_command.dkey, ballot, replica_id, instance_no, leader_deps_instance, true);
  }
  // No pre-accepted replies - start phase pre-accept with NO_OP
  Log_debug("Prepare - No pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  uint64_t seq = 0;
  map<uint64_t, uint64_t> deps;
  string noop_dkey = NOOP_DKEY;
  shared_ptr<Marshallable> NOOP_CMD = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
  return StartAccept(NOOP_CMD, noop_dkey, ballot, seq, deps, replica_id, instance_no);
}

EpaxosPrepareReply EpaxosServer::OnPrepareRequest(ballot_t ballot, uint64_t replica_id, uint64_t instance_no) {
  Log_debug("Received prepare request for replica: %d instance: %d in replica: %d for ballot: %d from new leader", replica_id, instance_no, curr_replica_id, ballot);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Reject older ballots
  if (ballot < ecmd->highest_seen) {
    return EpaxosPrepareReply(false, 
                              ecmd->cmd, 
                              ecmd->dkey, 
                              ecmd->seq,
                              ecmd->deps,
                              ecmd->state,
                              curr_replica_id,
                              ecmd->highest_seen);
  }
  ecmd->highest_seen = ballot;
  return EpaxosPrepareReply(true, 
                            ecmd->cmd, 
                            ecmd->dkey, 
                            ecmd->seq,
                            ecmd->deps,
                            ecmd->state,
                            curr_replica_id,
                            ecmd->highest_accepted);
}

void EpaxosServer::PrepareTillCommitted(uint64_t& replica_id, uint64_t& instance_no) {
  // Repeat till prepare succeeds
  bool committed = false;
  while (!committed) {
    auto state = GetCommand(replica_id, instance_no)->state;
    if (state >= EpaxosCommandState::COMMITTED) return;
    committed = StartPrepare(replica_id, instance_no);
    Coroutine::Sleep(200000 + (rand() % 50000));
  }
}

/***********************************
        Execution Phase            *
************************************/

vector<shared_ptr<EpaxosCommand>> EpaxosServer::GetDependencies(shared_ptr<EpaxosCommand>& ecmd) {
  vector<shared_ptr<EpaxosCommand>> deps;
  for (auto itr : ecmd->deps) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    GetCommand(dreplica_id, dinstance_no)->committed_ev.WaitUntilGreaterOrEqualThan(1);
    int64_t prev_instance_no = dinstance_no;
    while (prev_instance_no >= 0) {
      shared_ptr<EpaxosCommand> prev_cmd = GetCommand(dreplica_id, prev_instance_no);
      if (prev_cmd->dkey != NOOP_DKEY && prev_cmd->dkey != ecmd->dkey) {
        prev_instance_no--;
        continue;
      }
      prev_cmd->committed_ev.WaitUntilGreaterOrEqualThan(1);
      if (prev_cmd->dkey == ecmd->dkey) break;
      prev_instance_no--;
    }
    if (prev_instance_no < 0) continue;
    if (GetCommand(dreplica_id, prev_instance_no)->state == EpaxosCommandState::EXECUTED) continue;
    deps.push_back(GetCommand(dreplica_id, prev_instance_no));
  }
  return deps;
}

void EpaxosServer::Execute(shared_ptr<EpaxosCommand>& ecmd) {
  ecmd->state = EpaxosCommandState::EXECUTED;
  Log_debug("Executed replica: %d instance: %d in replica: %d", ecmd->replica_id, ecmd->instance_no, curr_replica_id);
  if (ecmd->replica_id == curr_replica_id) {
    #ifdef EPAXOS_SERVER_METRICS_COLLECTION
    auto& command = dynamic_cast<TpcCommitCommand&>(*(ecmd->cmd));
    exec_times.push_back(start_times[command.tx_id_].elapsed());
    #endif
    ecmd->callback();
  }
  #ifdef EPAXOS_TEST_CORO
  else {
    app_next_(*(ecmd->cmd));
  }
  #endif
  // Update executed_till
  executed_till[ecmd->dkey][ecmd->replica_id] = max(executed_till[ecmd->dkey][ecmd->replica_id], ecmd->instance_no);
}

// Should be called only after command at that instance is committed
void EpaxosServer::StartExecution(uint64_t& replica_id, uint64_t& instance_no) {
  // Stop execution of next command of same dkey
  #ifdef EPAXOS_TEST_CORO
  if (pause_execution) return;
  #endif
  Log_debug("Received execution request for replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  // Stop execution of next command of same dkey
  if (ecmd->dkey == NOOP_DKEY) return;
  string &dkey = ecmd->dkey;
  in_exec_dkeys[dkey].Wait(1);
  if (ecmd->state == EpaxosCommandState::EXECUTED) {
    in_exec_dkeys[dkey].NotifyOne();
    return;
  }
  // Execute
  unique_ptr<EpaxosGraph> graph = make_unique<EpaxosGraph>();
  graph->Execute(ecmd,
                 [this](shared_ptr<EpaxosCommand> &ecmd) {
                   Execute(ecmd);
                   return;
                 }, 
                 [this](shared_ptr<EpaxosCommand> &ecmd) {
                   return GetDependencies(ecmd);
                 });
  Log_debug("Completed replica: %d instance: %d by replica: %d", replica_id, instance_no, curr_replica_id);
  // Free lock to execute next command of same dkey
  in_exec_dkeys[dkey].NotifyOne();
}

/***********************************
         Helper Methods            *
************************************/


shared_ptr<EpaxosCommand> EpaxosServer::GetCommand(uint64_t replica_id, uint64_t instance_no) {
  if (cmds[replica_id].count(instance_no) == 0) {
    cmds[replica_id][instance_no] = make_shared<EpaxosCommand>();
  }
  return cmds[replica_id][instance_no];
}

template<class ClassT>
void EpaxosServer::UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t& replica_id, uint64_t& instance_no) {
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  ballot_t highest_seen_ballot = ecmd->highest_seen;
  for (auto reply : replies) {
    if (reply.ballot > highest_seen_ballot) {
      highest_seen_ballot = reply.ballot;
    }
  }
  ecmd->highest_seen = highest_seen_ballot;
}

void EpaxosServer::UpdateCommittedTill(uint64_t& replica_id, uint64_t& instance_no) {
  if ((instance_no == 0 && committed_till.count(replica_id) == 0) || instance_no == committed_till[replica_id] + 1) {
    uint64_t committed_till_instance_no = instance_no;
    while (committed_till_instance_no <= received_till[replica_id] && GetCommand(replica_id, committed_till_instance_no)->state >= EpaxosCommandState::COMMITTED) {
      committed_till_instance_no++;
    }
    committed_till[replica_id] = committed_till_instance_no - 1;
  }
}

bool EpaxosServer::AreAllDependenciesCommitted(vector<EpaxosPreAcceptReply>& replies, map<uint64_t, uint64_t>& merged_deps) {
  map<uint64_t, uint64_t> merged_committed_till = committed_till;
  for (int i = 0; i < replies.size(); i++) {
    for (uint64_t dreplica_id : replies[i].committed_deps) {
      uint64_t dinstance_no = replies[i].deps[dreplica_id];
      merged_committed_till[dreplica_id] = max(merged_committed_till[dreplica_id], dinstance_no);
    }
  }
  bool all_committed = true;
  for (auto itr : merged_deps) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    if (merged_committed_till.count(dreplica_id) == 0 || merged_committed_till[dreplica_id] < dinstance_no) {
      all_committed = false;
      break;
    }
  }
  Log_debug("All committed %d", all_committed);
  return all_committed;
}

unordered_set<uint64_t> EpaxosServer::GetReplicasWithAllDependenciesCommitted(map<uint64_t, uint64_t>& merged_deps) {
  unordered_set<uint64_t> committed_deps;
  for (auto itr : merged_deps) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    if (committed_till.count(dreplica_id) != 0 && committed_till[dreplica_id] >= dinstance_no) {
      committed_deps.insert(dreplica_id);
    }
  }
  return committed_deps;
}

void EpaxosServer::GetLatestAttributes(string& dkey, uint64_t& leader_replica_id, int64_t& leader_prev_dep_instance_no, uint64_t *seq, map<uint64_t, uint64_t> *deps) {
  if (dkey == NOOP_DKEY) {
    *seq = 0;
    return;
  }
  *seq = dkey_seq[dkey] + 1;
  for (auto itr : dkey_deps[dkey]) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    if (dreplica_id == leader_replica_id) continue;
    (*deps)[dreplica_id] = dinstance_no;
  }
  if(leader_prev_dep_instance_no >= 0) {
    (*deps)[leader_replica_id] = leader_prev_dep_instance_no;
  }
}

void EpaxosServer::UpdateAttributes(string& dkey, 
                                    uint64_t& replica_id, 
                                    uint64_t& instance_no, 
                                    uint64_t& seq) {
  received_till[replica_id] = max(received_till[replica_id], instance_no);
  if (dkey == NOOP_DKEY) return;
  dkey_seq[dkey] = max(dkey_seq[dkey], seq);
  dkey_deps[dkey][replica_id] = max(dkey_deps[dkey][replica_id], instance_no);
}

void EpaxosServer::MergeAttributes(vector<EpaxosPreAcceptReply>& replies, uint64_t *seq, map<uint64_t, uint64_t> *deps) {
  *seq = 0;
  for (auto reply : replies) {
    *seq = max(*seq, reply.seq);
    for (auto itr : reply.deps) {
      uint64_t dreplica_id = itr.first;
      uint64_t dinstance_no = itr.second;
      (*deps)[dreplica_id] = max((*deps)[dreplica_id], dinstance_no);
      received_till[dreplica_id] = max(received_till[dreplica_id], dinstance_no);
    }
  }
}

bool EpaxosServer::IsInitialBallot(ballot_t& ballot) {
  return ((ballot & 0xFFFFFFFF) >> 8) == 0;
}

ballot_t EpaxosServer::GetInitialBallot() {
  return curr_epoch << 32 | curr_replica_id;
}

ballot_t EpaxosServer::GetNextBallot(ballot_t &ballot) {
  ballot_t ballot_no = (ballot & 0xFFFFFFFF) >> 8;
  ballot_no += 1;
  return curr_epoch << 32 | ballot_no << 8 | curr_replica_id;
}

void EpaxosServer::FindTryPreAcceptConflict(shared_ptr<Marshallable>& cmd, 
                                            string& dkey, 
                                            uint64_t& seq,
                                            map<uint64_t, uint64_t>& deps, 
                                            uint64_t& replica_id, 
                                            uint64_t& instance_no,
                                            EpaxosTryPreAcceptStatus *conflict_state,
                                            uint64_t *conflict_replica_id, 
                                            uint64_t *conflict_instance_no) {
  *conflict_replica_id = 0;
  *conflict_instance_no = 0;
  *conflict_state = EpaxosTryPreAcceptStatus::NO_CONFLICT;
  shared_ptr<EpaxosCommand> ecmd = GetCommand(replica_id, instance_no);
  if ((ecmd->state == EpaxosCommandState::PRE_ACCEPTED || ecmd->state == EpaxosCommandState::PRE_ACCEPTED_EQ)
      && (ecmd->seq == seq && ecmd->deps == deps)) {
    return;
  }
  for (auto itr : dkey_deps[dkey]) {
    uint64_t dreplica_id = itr.first;
    for(uint64_t dinstance_no = executed_till[dkey][dreplica_id]; dinstance_no <= itr.second; dinstance_no++) {
      // no point checking past instance in replica's row
      if (dreplica_id == replica_id && dinstance_no == instance_no) break;
      // the instance cannot be a dependency for itself
      if (deps.count(dreplica_id) && deps[dreplica_id] == dinstance_no) continue;
      // command not seen by the server yet
      shared_ptr<EpaxosCommand> dep_ecmd = GetCommand(dreplica_id, dinstance_no);
      if (dep_ecmd->state == EpaxosCommandState::NOT_STARTED) continue;
      // command not interefering (case 6.i in proof)
      if (dep_ecmd->dkey != dkey) continue;
      // command depends on currently try-pre-accepting command, so not a conflict (case 6.ii in proof)
      if (dep_ecmd->deps[replica_id] >= instance_no) continue;
      if (dinstance_no > deps[dreplica_id]  // (case 6.iii.a in proof)
          || (dep_ecmd->seq >= seq // (case 6.iii.b)
              && !(dreplica_id == replica_id && (dep_ecmd->state == EpaxosCommandState::PRE_ACCEPTED // (case 6.iii.b exception)
                                                 || dep_ecmd->state == EpaxosCommandState::PRE_ACCEPTED_EQ)))) {
        *conflict_replica_id = dreplica_id;
        *conflict_instance_no = dinstance_no;
        if (dep_ecmd->state >= EpaxosCommandState::COMMITTED) {
          *conflict_state = EpaxosTryPreAcceptStatus::COMMITTED_CONFLICT;
        } else {
          *conflict_state = EpaxosTryPreAcceptStatus::UNCOMMITTED_CONFLICT;
        }
        return;
      }
    }
  }
}

/* Do not modify any code below here */

void EpaxosServer::Disconnect(const bool disconnect) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(disconnected_ != disconnect);
  // global map of rpc_par_proxies_ values accessed by partition then by site
  static map<parid_t, map<siteid_t, map<siteid_t, vector<SiteProxyPair>>>> _proxies{};
  if (_proxies.find(partition_id_) == _proxies.end()) {
    _proxies[partition_id_] = {};
  }
  EpaxosCommo *c = (EpaxosCommo*) commo();
  if (disconnect) {
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() > 0);
    auto sz = c->rpc_par_proxies_.size();
    _proxies[partition_id_][loc_id_].insert(c->rpc_par_proxies_.begin(), c->rpc_par_proxies_.end());
    c->rpc_par_proxies_ = {};
    verify(_proxies[partition_id_][loc_id_].size() == sz);
    verify(c->rpc_par_proxies_.size() == 0);
  } else {
    verify(_proxies[partition_id_][loc_id_].size() > 0);
    auto sz = _proxies[partition_id_][loc_id_].size();
    c->rpc_par_proxies_ = {};
    c->rpc_par_proxies_.insert(_proxies[partition_id_][loc_id_].begin(), _proxies[partition_id_][loc_id_].end());
    _proxies[partition_id_][loc_id_] = {};
    verify(_proxies[partition_id_][loc_id_].size() == 0);
    verify(c->rpc_par_proxies_.size() == sz);
  }
  disconnected_ = disconnect;
}

bool EpaxosServer::IsDisconnected() {
  return disconnected_;
}

} // namespace janus
