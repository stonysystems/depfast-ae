#include "server.h"
#include "frame.h"


namespace janus {

EpaxosServer::EpaxosServer(Frame * frame) {
  frame_ = frame;
  Log::set_level(Log::DEBUG); // <REMOVE_AFTER_TESTING>
}

EpaxosServer::~EpaxosServer() {}

/***********************************
   Main event processing loop      *
************************************/

void EpaxosServer::Setup() {
  // Future Work: Give unique replica id on restarts (by storing old replica_id persistently and new_replica_id = old_replica_id + N)
  replica_id_ = site_id_;

  // Process requests
  Coroutine::CreateRun([this](){
    while(true) {
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (reqs.size() == 0) {
        lock.unlock();
        Coroutine::Sleep(1000);
      } else {
        EpaxosRequest req = reqs.front();
        reqs.pop_front();
        lock.unlock();
        Coroutine::CreateRun([this, &req](){
          StartPreAccept(req.cmd, req.dkey, req.ballot, req.replica_id, req.instance_no, req.leader_dep_instance, false);
        });
      }
    }
  });

  // Explicity prepare uncommitted commands
  Coroutine::CreateRun([this](){
    while(true) {
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (pause_execution) {
        lock.unlock();
        Coroutine::Sleep(10000);
        continue;
      }
      auto received_till_ = this->received_till;
      auto prepared_till_ = this->prepared_till;
      lock.unlock();

      for (auto itr : received_till_) {
        uint64_t replica_id = itr.first;
        uint64_t received_till_instance_no = itr.second;
        uint64_t instance_no = prepared_till_.count(replica_id) ? prepared_till_[replica_id] + 1 : 0;
        while (instance_no <= received_till_instance_no) {
          Coroutine::CreateRun([this, replica_id, instance_no]() {
            std::unique_lock<std::recursive_mutex> lock(mtx_);
            while(!cmds[replica_id][instance_no].isCreatedBefore(2000)) {
              lock.unlock();
              Coroutine::Sleep(10000);
              lock.lock();
            }
            lock.unlock();
            PrepareTillCommitted(replica_id, instance_no);
            StartExecution(replica_id, instance_no); // <REMOVE_AFTER_TESTING>
          });
          Coroutine::Sleep(10);
          instance_no++;
        }
      }
      lock.lock();
      this->prepared_till = received_till_;
      lock.unlock();
      Coroutine::Sleep(10);
    }
  });

  // Process prepare requests <REMOVE_AFTER_TESTING>
   Coroutine::CreateRun([this](){
     while(true) {
      std::unique_lock<std::recursive_mutex> lock(mtx_);
      if (prepare_reqs.size() == 0) {
        lock.unlock();
        Coroutine::Sleep(10000);
      } else {
        auto req = prepare_reqs.front();
        prepare_reqs.pop_front();
        lock.unlock();
        Coroutine::CreateRun([this, &req](){
          PrepareTillCommitted(req.first, req.second);
          StartExecution(req.first, req.second);
        });
       }
     }
   });
}

/***********************************
      Client Request Handlers      *
************************************/

void EpaxosServer::Start(shared_ptr<Marshallable>& cmd, string dkey, uint64_t *replica_id, uint64_t *instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  EpaxosRequest req = CreateEpaxosRequest(cmd, dkey);
  *replica_id = req.replica_id;
  *instance_no = req.instance_no;
  Log_debug("Received request in server: %d for dep_key: %s replica: %d instance: %d", site_id_, dkey.c_str(), req.replica_id, req.instance_no);
  reqs.push_back(req);
}

void EpaxosServer::GetState(uint64_t replica_id, 
                uint64_t instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *dkey,
                uint64_t *seq, 
                unordered_map<uint64_t, uint64_t> *deps, 
                status_t *state) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  *cmd = cmds[replica_id][instance_no].cmd;
  *dkey = cmds[replica_id][instance_no].dkey;
  *deps = cmds[replica_id][instance_no].deps;
  *seq = cmds[replica_id][instance_no].seq;
  *state = cmds[replica_id][instance_no].state;
}

void EpaxosServer::Prepare(uint64_t replica_id, uint64_t instance_no) { // <REMOVE_AFTER_TESTING>
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("Received prepare request in server: %d for replica: %d instance: %d", site_id_, replica_id, instance_no);
  prepare_reqs.push_back(make_pair(replica_id, instance_no));
}

void EpaxosServer::PauseExecution(bool pause) { // <REMOVE_AFTER_TESTING>
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  pause_execution = pause;
}

EpaxosRequest EpaxosServer::CreateEpaxosRequest(shared_ptr<Marshallable>& cmd, string dkey) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  uint64_t instance_no = next_instance_no;
  next_instance_no = next_instance_no + 1;
  int64_t leader_dep_instance = -1;
  if (dkey_deps[dkey].count(replica_id_)) {
    leader_dep_instance = dkey_deps[dkey][replica_id_];
  }
  dkey_deps[dkey][replica_id_] = instance_no; // Important - otherwise next command may not have dependency on this command
  EpaxosBallot ballot = EpaxosBallot(curr_epoch, 0, replica_id_);
  return EpaxosRequest(cmd, dkey, ballot, replica_id_, instance_no, leader_dep_instance);
}

/***********************************
       Phase 1: Pre-accept         *
************************************/

bool EpaxosServer::StartPreAccept(shared_ptr<Marshallable>& cmd, 
                                  string dkey, 
                                  EpaxosBallot ballot, 
                                  uint64_t replica_id,
                                  uint64_t instance_no,
                                  int64_t leader_dep_instance,
                                  bool recovery) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Started pre-accept for request for replica: %d instance: %d dep_key: %s with leader_dep_instance: %d ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), leader_dep_instance, ballot.ballot_no, ballot.replica_id, replica_id_);
  // Reject old message - we have moved on
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::ACCEPTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    return false;
  }
  // Initialise attributes
  uint64_t seq = dkey_seq[dkey] + 1;
  unordered_map<uint64_t, uint64_t> deps = dkey_deps[dkey];
  if(leader_dep_instance >= 0) {
    deps[replica_id] = leader_dep_instance;
  } else {
    deps.erase(replica_id);
  }
  // Pre-accept command
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = seq;
  cmds[replica_id][instance_no].deps = deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = EpaxosCommandState::PRE_ACCEPTED;
  // Update internal atributes
  if (cmd->kind_ != MarshallDeputy::CMD_NOOP) {
    dkey_seq[dkey] = seq;
    dkey_deps[dkey][replica_id] = max(dkey_deps[dkey][replica_id], instance_no);
  }
  received_till[replica_id] = max(received_till[replica_id], instance_no);
  lock.unlock();
  // Send pre-accept requests
  auto ev = commo()->SendPreAccept(site_id_, 
                                   partition_id_, 
                                   recovery,
                                   ballot.epoch, 
                                   ballot.ballot_no, 
                                   ballot.replica_id, 
                                   replica_id,
                                   instance_no, 
                                   cmd, 
                                   dkey, 
                                   seq, 
                                   deps);
  ev->Wait(10000000);
  // Process pre-accept replies
  Log_debug("Started pre-accept reply processing for replica: %d instance: %d dep_key: %s with leader_dep_instance: %d ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), leader_dep_instance, ballot.ballot_no, ballot.replica_id, replica_id_);
  // Fail if timeout/no-majority
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Pre-accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success via fast path quorum
  if (ev->FastPath()) {
    Log_debug("Fastpath for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    return StartCommit(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  }
  // Success via slow path quorum
  if (ev->SlowPath()) {
    Log_debug("Slowpath for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    auto merged_res = MergeAttributes(ev->replies);
    UpdateInternalAttributes(cmd, dkey, replica_id, instance_no, merged_res.first, merged_res.second);
    return StartAccept(cmd, dkey, ballot, merged_res.first, merged_res.second, replica_id, instance_no);
  }
  verify(0);
  return false;
}

EpaxosPreAcceptReply EpaxosServer::OnPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                      string dkey, 
                                                      EpaxosBallot ballot, 
                                                      uint64_t seq, 
                                                      unordered_map<uint64_t, uint64_t> deps, 
                                                      uint64_t replica_id, 
                                                      uint64_t instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("Received pre-accept request for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), ballot.ballot_no, ballot.replica_id, replica_id_);
  EpaxosPreAcceptStatus status = EpaxosPreAcceptStatus::IDENTICAL;
  // Reject older ballots
  if (!ballot.isGreater(cmds[replica_id][instance_no].highest_seen)) {
    status = EpaxosPreAcceptStatus::FAILED;
    EpaxosBallot &highest_seen = cmds[replica_id][instance_no].highest_seen;
    EpaxosPreAcceptReply reply(status, highest_seen.epoch, highest_seen.ballot_no, highest_seen.replica_id);
    return reply;
  }
  /* - If it was pre-accepted by majority identically then it will never come to pre-accept phase again and instead go to accept phase.
     - If it was accepted by majority of replica then it will never come to pre-accept phase again and instead go to accept phase.
     - If it was committed in some other replica then it will never come to pre-accept phase again and instead go to accept phase.
     - If already committed or executed in this replica then it is just an delayed message so fail it (same as ignore). 
     - If already accepted in this replica then it means it was pre-accepted by some majority non-identically and accepted by the 
       leader. So prepare should have tried to accept this message again. So reject it so that prepare will try again.
       If new cmd is NOOP, then it will overwrite the accept when commit request comes. */
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::ACCEPTED 
      || cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    status = EpaxosPreAcceptStatus::FAILED;
    cmds[replica_id][instance_no].highest_seen = ballot;
    EpaxosPreAcceptReply reply(status, ballot.epoch, ballot.ballot_no, ballot.replica_id);
    return reply;
  }
  // Initialise attributes
  uint64_t merged_seq = dkey_seq[dkey] + 1;
  merged_seq = max(merged_seq, seq);
  if (merged_seq != seq) {
    status = EpaxosPreAcceptStatus::NON_IDENTICAL;
  }
  auto merged_deps = deps;
  for (auto itr : dkey_deps[dkey]) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    if (dreplica_id == replica_id) continue;
    if (merged_deps.count(dreplica_id) == 0 || dinstance_no > merged_deps[dreplica_id]) {
      merged_deps[dreplica_id] = dinstance_no;
      status = EpaxosPreAcceptStatus::NON_IDENTICAL;
    }
  }
  // Eq state not set if ballot is not default ballot
  if (!ballot.isDefault()) {
    status = EpaxosPreAcceptStatus::NON_IDENTICAL;
  }
  // Pre-accept command
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = merged_seq;
  cmds[replica_id][instance_no].deps = merged_deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = status ==  EpaxosPreAcceptStatus::IDENTICAL ? 
                                        EpaxosCommandState::PRE_ACCEPTED_EQ : 
                                        EpaxosCommandState::PRE_ACCEPTED;
  // Update internal attributes
  if (cmd->kind_ != MarshallDeputy::CMD_NOOP) {
    dkey_seq[dkey] = cmds[replica_id][instance_no].seq;
    int64_t leader_dep_instance = max(dkey_deps[dkey][replica_id], instance_no);
    dkey_deps[dkey] = cmds[replica_id][instance_no].deps;
    dkey_deps[dkey][replica_id] = leader_dep_instance;
    received_till[replica_id] = max(received_till[replica_id], dkey_deps[dkey][replica_id]);
  }
  received_till[replica_id] = max(received_till[replica_id], instance_no);
  // Reply
  EpaxosPreAcceptReply reply(status, ballot.epoch, ballot.ballot_no, ballot.replica_id, merged_seq, merged_deps);
  return reply;
}

/***********************************
         Phase 2: Accept           *
************************************/

bool EpaxosServer::StartAccept(shared_ptr<Marshallable>& cmd, 
                               string dkey, 
                               EpaxosBallot ballot, 
                               uint64_t seq,
                               unordered_map<uint64_t, uint64_t> deps, 
                               uint64_t replica_id, 
                               uint64_t instance_no) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Started accept request for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Reject old message - we have moved on
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED
      || !ballot.isGreaterOrEqual(cmds[replica_id][instance_no].highest_seen)) {
    return false;
  }
  // Accept command
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = seq;
  cmds[replica_id][instance_no].deps = deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = EpaxosCommandState::ACCEPTED;
  lock.unlock();
  // Send accept requests
  auto ev = commo()->SendAccept(site_id_, 
                                partition_id_, 
                                ballot.epoch, 
                                ballot.ballot_no, 
                                ballot.replica_id, 
                                replica_id,
                                instance_no, 
                                cmd, 
                                dkey, 
                                seq, 
                                deps);
  ev->Wait(10000000);
  // Process accept replies
  Log_debug("Started accept reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Fail if timeout/no-majority
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success
  if (ev->Yes()) {
    return StartCommit(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  }
  verify(0);
  return false;
}

EpaxosAcceptReply EpaxosServer::OnAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                string dkey, 
                                                EpaxosBallot ballot, 
                                                uint64_t seq,
                                                unordered_map<uint64_t, uint64_t> deps, 
                                                uint64_t replica_id, 
                                                uint64_t instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("Received accept request for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Reject older ballots
  if (!ballot.isGreaterOrEqual(cmds[replica_id][instance_no].highest_seen)) {
    EpaxosBallot &highest_seen = cmds[replica_id][instance_no].highest_seen;
    EpaxosAcceptReply reply(false, highest_seen.epoch, highest_seen.ballot_no, highest_seen.replica_id);
    return reply;
  }
  // Accept command
  /* - If already committed or executed in this replica then it is just an delayed message so fail it (same as ignore). 
     - If already accepted in this replica then it can be still overwritten because majority haven't agreed to it identically. */
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED 
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    cmds[replica_id][instance_no].highest_seen = ballot;
    EpaxosAcceptReply reply(false, ballot.epoch, ballot.ballot_no, ballot.replica_id);
    return reply;
  }
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = seq;
  cmds[replica_id][instance_no].deps = deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = EpaxosCommandState::ACCEPTED;
  // Update internal attributes
  UpdateInternalAttributes(cmd, dkey, replica_id, instance_no, seq, deps);
  // Reply
  EpaxosAcceptReply reply(true, ballot.epoch, ballot.ballot_no, ballot.replica_id);
  return reply;
}

/***********************************
          Commit Phase             *
************************************/

bool EpaxosServer::StartCommit(shared_ptr<Marshallable>& cmd, 
                               string dkey, 
                               EpaxosBallot ballot, 
                               uint64_t seq,
                               unordered_map<uint64_t, uint64_t> deps, 
                               uint64_t replica_id, 
                               uint64_t instance_no) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Started commit request for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);// Use the latest ballot
  if (!ballot.isGreaterOrEqual(cmds[replica_id][instance_no].highest_seen)) {
    ballot = cmds[replica_id][instance_no].highest_seen;
  }
  // Commit command if not committed
  if (cmds[replica_id][instance_no].state != EpaxosCommandState::EXECUTED) {
    cmds[replica_id][instance_no].state = EpaxosCommandState::COMMITTED;
  }
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = seq;
  cmds[replica_id][instance_no].deps = deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  lock.unlock();
  // Execute
  StartExecutionAsync(replica_id, instance_no);
  // Send async commit request to all
  auto ev = commo()->SendCommit(site_id_, 
                                partition_id_, 
                                ballot.epoch, 
                                ballot.ballot_no, 
                                ballot.replica_id,
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
                                   EpaxosBallot ballot, 
                                   uint64_t seq,
                                   unordered_map<uint64_t, uint64_t> deps, 
                                   uint64_t replica_id, 
                                   uint64_t instance_no) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Received commit request for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Use the latest ballot
  if (!ballot.isGreaterOrEqual(cmds[replica_id][instance_no].highest_seen)) {
    ballot = cmds[replica_id][instance_no].highest_seen;
  }
  // Commit command if not committed
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    return;
  }
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = seq;
  cmds[replica_id][instance_no].deps = deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = EpaxosCommandState::COMMITTED;
  // Update internal attributes
  UpdateInternalAttributes(cmd, dkey, replica_id, instance_no, seq, deps);
  lock.unlock();
  // Execute
  StartExecutionAsync(replica_id, instance_no);
}

/***********************************
       TryPreAccept Phase          *
************************************/
bool EpaxosServer::StartTryPreAccept(shared_ptr<Marshallable>& cmd, 
                                     string dkey, 
                                     EpaxosBallot ballot, 
                                     uint64_t seq,
                                     unordered_map<uint64_t, uint64_t> deps, 
                                     uint64_t replica_id, 
                                     uint64_t instance_no,
                                     unordered_set<siteid_t> preaccepted_sites) {
  if (preaccepted_sites.count(site_id_) == 0) {
    auto reply = OnTryPreAcceptRequest(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
    if (reply.status) {
      preaccepted_sites.insert(site_id_);
    }
  } else {
    preaccepted_sites.insert(site_id_);
  }
  // Send try-pre-accept requests
  auto ev = commo()->SendTryPreAccept(site_id_, 
                                      partition_id_,
                                      preaccepted_sites,
                                      ballot.epoch, 
                                      ballot.ballot_no, 
                                      ballot.replica_id, 
                                      replica_id,
                                      instance_no, 
                                      cmd, 
                                      dkey, 
                                      seq, 
                                      deps);
  ev->Wait(10000000);
  // Process try-pre-accept replies
  Log_debug("Started try-pre-accept reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Fail if timeout/no-majority
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Try-pre-accept failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Success
  if (ev->Yes()) {
    return StartAccept(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  }
  verify(0);
  return false;
}

EpaxosTryPreAcceptReply EpaxosServer::OnTryPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                            string dkey, 
                                                            EpaxosBallot ballot, 
                                                            uint64_t seq, 
                                                            unordered_map<uint64_t, uint64_t> deps, 
                                                            uint64_t replica_id, 
                                                            uint64_t instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("Received try-pre-accept request for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d by replica: %d", 
            replica_id, instance_no, dkey.c_str(), ballot.ballot_no, ballot.replica_id, replica_id_);
  // Reject older ballots
  if (!ballot.isGreaterOrEqual(cmds[replica_id][instance_no].highest_seen)) {
    EpaxosBallot &highest_seen = cmds[replica_id][instance_no].highest_seen;
    EpaxosTryPreAcceptReply reply(false, highest_seen.epoch, highest_seen.ballot_no, highest_seen.replica_id);
    return reply;
  }
  // Reject old messages - we have moved on
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::PRE_ACCEPTED 
      || cmds[replica_id][instance_no].state == EpaxosCommandState::ACCEPTED 
      || cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    cmds[replica_id][instance_no].highest_seen = ballot;
    EpaxosTryPreAcceptReply reply(false, ballot.epoch, ballot.ballot_no, ballot.replica_id);
    return reply;
  }
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::PRE_ACCEPTED_EQ) {
    cmds[replica_id][instance_no].highest_seen = ballot;
    EpaxosTryPreAcceptReply reply(true, ballot.epoch, ballot.ballot_no, ballot.replica_id);
    return reply;
  }
  // Initialise attributes
  uint64_t merged_seq = dkey_seq[dkey] + 1;
  merged_seq = max(merged_seq, seq);
  if (merged_seq != seq) {
    EpaxosTryPreAcceptReply reply(false, ballot.epoch, ballot.ballot_no, ballot.replica_id);
    return reply;
  }
  auto merged_deps = deps;
  for (auto itr : dkey_deps[dkey]) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    if (dreplica_id == replica_id) continue;
    if (merged_deps.count(dreplica_id) == 0 || dinstance_no > merged_deps[dreplica_id]) {
      merged_deps[dreplica_id] = dinstance_no;
      EpaxosTryPreAcceptReply reply(false, ballot.epoch, ballot.ballot_no, ballot.replica_id);
      return reply;
    }
  }
  // Pre-accept command
  cmds[replica_id][instance_no].cmd = cmd;
  cmds[replica_id][instance_no].dkey = dkey;
  cmds[replica_id][instance_no].seq = merged_seq;
  cmds[replica_id][instance_no].deps = merged_deps;
  cmds[replica_id][instance_no].highest_seen = ballot;
  cmds[replica_id][instance_no].highest_accepted = ballot;
  cmds[replica_id][instance_no].state = EpaxosCommandState::PRE_ACCEPTED;
  // Update internal attributes
  if (cmd->kind_ != MarshallDeputy::CMD_NOOP) {
    dkey_seq[dkey] = cmds[replica_id][instance_no].seq;
    int64_t leader_dep_instance = max(dkey_deps[dkey][replica_id], instance_no);
    dkey_deps[dkey] = cmds[replica_id][instance_no].deps;
    dkey_deps[dkey][replica_id] = leader_dep_instance;
    received_till[replica_id] = max(received_till[replica_id], dkey_deps[dkey][replica_id]);
  }
  received_till[replica_id] = max(received_till[replica_id], instance_no);
  // Reply
  EpaxosTryPreAcceptReply reply(true, ballot.epoch, ballot.ballot_no, ballot.replica_id);
  return reply;
}

/***********************************
         Prepare Phase             *
************************************/

bool EpaxosServer::StartPrepare(uint64_t replica_id, uint64_t instance_no) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    return true;
  }
  // Get ballot = highest seen ballot
  EpaxosBallot ballot = EpaxosBallot(curr_epoch, 0, replica_id_);
  // Create prepare reply from self
  shared_ptr<Marshallable> NOOP_CMD = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
  EpaxosPrepareReply self_reply(true, NOOP_CMD, NOOP_DKEY, 0, unordered_map<uint64_t, uint64_t>(), EpaxosCommandState::NOT_STARTED, replica_id_, 0, -1, 0);
  if (cmds[replica_id][instance_no].state != EpaxosCommandState::NOT_STARTED) {
    self_reply = EpaxosPrepareReply(true, 
                                    cmds[replica_id][instance_no].cmd, 
                                    cmds[replica_id][instance_no].dkey, 
                                    cmds[replica_id][instance_no].seq,
                                    cmds[replica_id][instance_no].deps,
                                    cmds[replica_id][instance_no].state,
                                    replica_id_,
                                    cmds[replica_id][instance_no].highest_accepted.epoch,
                                    cmds[replica_id][instance_no].highest_accepted.ballot_no,
                                    cmds[replica_id][instance_no].highest_accepted.replica_id);
  }
  ballot.ballot_no = max(ballot.ballot_no, cmds[replica_id][instance_no].highest_seen.ballot_no) + 1;
  cmds[replica_id][instance_no].highest_seen = ballot;
  Log_debug("Started prepare for replica: %d instance: %d by replica: %d for ballot: %d", replica_id, instance_no, replica_id_, ballot.ballot_no);
  lock.unlock();
  // Send prepare requests
  auto ev = commo()->SendPrepare(site_id_, 
                                partition_id_, 
                                ballot.epoch, 
                                ballot.ballot_no, 
                                ballot.replica_id, 
                                replica_id,
                                instance_no);
  ev->Wait(10000000);
  // Process prepare replies
  Log_debug("Started prepare reply processing for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  if (ev->status_ == Event::TIMEOUT || ev->No()) {
    Log_debug("Prepare failed for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateHighestSeenBallot(ev->replies, replica_id, instance_no);
    return false;
  }
  // Add self reply
  ev->replies.push_back(self_reply);
  EpaxosBallot highest_accepted_ballot = EpaxosBallot();
  EpaxosPrepareReply rec_command;
  unordered_set<siteid_t> identical_preaccepted_sites;
  for (auto reply : ev->replies) {
    if (!reply.status || reply.cmd_state == EpaxosCommandState::NOT_STARTED) continue;
    EpaxosBallot reply_ballot = EpaxosBallot(reply.epoch, reply.ballot_no, reply.replica_id);
    if (reply_ballot.isGreater(highest_accepted_ballot)) {
      highest_accepted_ballot = reply_ballot;
      rec_command = EpaxosPrepareReply();
    }
    if (reply_ballot.isGreaterOrEqual(highest_accepted_ballot)) {
      highest_accepted_ballot = reply_ballot;
      // Atleast one commited reply
      if (reply.cmd_state == EpaxosCommandState::COMMITTED || reply.cmd_state == EpaxosCommandState::EXECUTED) {
        Log_debug("Prepare - committed cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, replica_id_, reply.acceptor_replica_id);
        UpdateInternalAttributes(reply.cmd, reply.dkey, replica_id, instance_no, reply.seq, reply.deps);
        return StartCommit(reply.cmd, reply.dkey, ballot, reply.seq, reply.deps, replica_id, instance_no);
      }
      // Atleast one accept reply
      if (reply.cmd_state == EpaxosCommandState::ACCEPTED) {
        Log_debug("Prepare - accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, replica_id_, reply.acceptor_replica_id);
        rec_command = reply;
      } 
      // Identical replies of default ballot
      else if (reply.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ 
                && rec_command.cmd_state != EpaxosCommandState::ACCEPTED) {
        Log_debug("Prepare - identical pre-accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, replica_id_, reply.acceptor_replica_id);
        rec_command = reply;
        identical_preaccepted_sites.insert(ev->replicaid_siteid_map[reply.acceptor_replica_id]);
      } 
      // Non-identical replies of default ballot
      else if (reply.cmd_state == EpaxosCommandState::PRE_ACCEPTED 
                && rec_command.cmd_state != EpaxosCommandState::ACCEPTED
                && rec_command.cmd_state != EpaxosCommandState::PRE_ACCEPTED_EQ) {
        Log_debug("Prepare - pre-accepted cmd found for replica: %d instance: %d by replica: %d from acceptor: %d", replica_id, instance_no, replica_id_, reply.acceptor_replica_id);
        rec_command = reply;
      }
    }
  }
  // Atleast one accepted reply - start phase accept
  if (rec_command.cmd_state == EpaxosCommandState::ACCEPTED) {
    Log_debug("Prepare - Atleast one accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateInternalAttributes(rec_command.cmd, rec_command.dkey, replica_id, instance_no, rec_command.seq, rec_command.deps);
    return StartAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no);
  }
  // N/2 identical pre-accepted replies for default ballot
  if (rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ && identical_preaccepted_sites.size() >= NSERVERS/2) {
    Log_debug("Prepare - Majority identical pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    UpdateInternalAttributes(rec_command.cmd, rec_command.dkey, replica_id, instance_no, rec_command.seq, rec_command.deps);
    return StartAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no);
  }
  int F = NSERVERS/2;
  // (F+1)/2 identical pre-accepted replies for default ballot
  if (rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ && identical_preaccepted_sites.size() >= (F+1)/2) {
    // Log_debug("Prepare - (F+1)/2 identical pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    // UpdateInternalAttributes(rec_command.cmd, rec_command.dkey, replica_id, instance_no, rec_command.seq, rec_command.deps);
    // bool status = StartTryPreAccept(rec_command.cmd, rec_command.dkey, ballot, rec_command.seq, rec_command.deps, replica_id, instance_no, identical_preaccepted_sites);
    // if (status) return status;
  }
  // Atleast one pre-accepted reply - start phase pre-accept
  if (rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED || rec_command.cmd_state == EpaxosCommandState::PRE_ACCEPTED_EQ) {
    Log_debug("Prepare - Atleast one pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
    uint64_t leader_deps_instance = rec_command.deps.count(replica_id) ? rec_command.deps[replica_id] : -1;
    UpdateInternalAttributes(rec_command.cmd, rec_command.dkey, replica_id, instance_no, rec_command.seq, rec_command.deps);
    return StartPreAccept(rec_command.cmd, rec_command.dkey, ballot, replica_id, instance_no, leader_deps_instance, true);
  }
  // No pre-accepted replies - start phase pre-accept with NO_OP
  Log_debug("Prepare - No pre-accepted cmd found for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  return StartPreAccept(NOOP_CMD, NOOP_DKEY, ballot, replica_id, instance_no, -1, true);
}

EpaxosPrepareReply EpaxosServer::OnPrepareRequest(EpaxosBallot ballot, uint64_t replica_id, uint64_t instance_no) {
  Log_debug("Received prepare request for replica: %d instance: %d in replica: %d for ballot: %d from new leader: %d", replica_id, instance_no, replica_id_, ballot.ballot_no, ballot.replica_id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  shared_ptr<Marshallable> NOOP_CMD = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
  EpaxosPrepareReply reply(true, NOOP_CMD, NOOP_DKEY, 0, unordered_map<uint64_t, uint64_t>(), EpaxosCommandState::NOT_STARTED, replica_id_, 0, -1, 0);
  // Reject older ballots
  if (!ballot.isGreater(cmds[replica_id][instance_no].highest_seen)) {
    reply.status = false;
    reply.epoch = cmds[replica_id][instance_no].highest_seen.epoch;
    reply.ballot_no = cmds[replica_id][instance_no].highest_seen.ballot_no;
    reply.replica_id = cmds[replica_id][instance_no].highest_seen.replica_id;
    return reply;
  }
  if (cmds[replica_id][instance_no].state != EpaxosCommandState::NOT_STARTED) {
    reply = EpaxosPrepareReply(true, 
                               cmds[replica_id][instance_no].cmd, 
                               cmds[replica_id][instance_no].dkey, 
                               cmds[replica_id][instance_no].seq,
                               cmds[replica_id][instance_no].deps,
                               cmds[replica_id][instance_no].state,
                               replica_id_,
                               cmds[replica_id][instance_no].highest_accepted.epoch,
                               cmds[replica_id][instance_no].highest_accepted.ballot_no,
                               cmds[replica_id][instance_no].highest_accepted.replica_id);
  }
  return reply;
}

void EpaxosServer::PrepareTillCommitted(uint64_t replica_id, uint64_t instance_no) {
  // Wait till concurrent prepare succeeds
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Prepare till committed replica: %d instance: %d in replica: %d", replica_id, instance_no, replica_id_);
  while (cmds[replica_id][instance_no].preparing) {
    lock.unlock();
    Log_debug("Waiting for replica: %d instance: %d in replica: %d", replica_id, instance_no, replica_id_);
    Coroutine::Sleep(100000);
    lock.lock();
  }
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::COMMITTED
      || cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    return;
  }
  cmds[replica_id][instance_no].preparing = true;
  lock.unlock();
  // Wait till prepare succeeds
  bool committed = StartPrepare(replica_id, instance_no);
  while (!committed) {
    Coroutine::Sleep(100000 + (rand() % 50000));
    committed = StartPrepare(replica_id, instance_no);
  }
  // Mark preparing
  lock.lock();
  cmds[replica_id][instance_no].preparing = false;
}

/***********************************
        Execution Phase            *
************************************/

int EpaxosServer::CreateEpaxosGraph(uint64_t replica_id, uint64_t instance_no, EpaxosGraph *graph) {
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  Log_debug("Adding to graph replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  if (cmds[replica_id][instance_no].state != EpaxosCommandState::COMMITTED
      && cmds[replica_id][instance_no].state != EpaxosCommandState::EXECUTED) {
    lock.unlock();
    PrepareTillCommitted(replica_id, instance_no);
    lock.lock();
  }
  if (cmds[replica_id][instance_no].state == EpaxosCommandState::EXECUTED) {
    return 1;
  }
  if (cmds[replica_id][instance_no].cmd->kind_ == MarshallDeputy::CMD_NOOP) {
    return 0;
  }
  shared_ptr<EpaxosVertex> child = make_shared<EpaxosVertex>(&cmds[replica_id][instance_no], replica_id, instance_no);
  bool exists = graph->FindOrCreateVertex(child);
  if (exists) {
    return 2;
  }
  auto deps = child->cmd->deps;
  string dkey = child->cmd->dkey;
  lock.unlock();

  for (auto itr : deps) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    int status = CreateEpaxosGraph(dreplica_id, dinstance_no, graph);
    if (status == 0) {
      int64_t prev_instance_no = dinstance_no - 1;
      while (prev_instance_no >= 0) {
        lock.lock();
        string dep_dkey = cmds[dreplica_id][prev_instance_no].dkey;
        lock.unlock();
        if (dep_dkey != NOOP_DKEY && dep_dkey != dkey) {
          prev_instance_no--;
          continue;
        }
        PrepareTillCommitted(dreplica_id, prev_instance_no);
        lock.lock();
        dep_dkey = cmds[dreplica_id][prev_instance_no].dkey;
        lock.unlock();
        if (dep_dkey == dkey) {
          break;
        }
        prev_instance_no--;
      }
      if (prev_instance_no < 0) continue;
      status = CreateEpaxosGraph(dreplica_id, prev_instance_no, graph);
      dinstance_no = prev_instance_no;
    }
    if (status == 1) continue;
    lock.lock();
    shared_ptr<EpaxosVertex> parent = make_shared<EpaxosVertex>(&cmds[dreplica_id][dinstance_no], dreplica_id, dinstance_no);
    graph->FindOrCreateParentEdge(child, parent);
    lock.unlock();
  }
  return 2;
}

// Should be called only after command at that instance is committed
void EpaxosServer::StartExecution(uint64_t replica_id, uint64_t instance_no) {
  Log_debug("Received execution request for replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Stop execution of next command of same dkey
  std::unique_lock<std::recursive_mutex> lock(mtx_);
  if (pause_execution) { // <REMOVE_AFTER_TESTING>
    return;
  }
  if (cmds[replica_id][instance_no].cmd->kind_ == MarshallDeputy::CMD_NOOP) {
    return;
  }
  // Stop execution of next command of same dkey
  while (in_process_dkeys.count(cmds[replica_id][instance_no].dkey) > 0) {
    lock.unlock();
    Coroutine::Sleep(10000);
    lock.lock();
  }
  in_process_dkeys.insert(cmds[replica_id][instance_no].dkey);
  lock.unlock();
  // Execute
  EpaxosGraph graph = EpaxosGraph();
  CreateEpaxosGraph(replica_id, instance_no, &graph);
  lock.lock();
  auto sorted_vertices = graph.GetSortedVertices();
  for (auto vertex : sorted_vertices) {
    if (vertex->cmd->state != EpaxosCommandState::EXECUTED) {
      vertex->cmd->state = EpaxosCommandState::EXECUTED;
      Log_debug("Executed replica: %d instance: %d in replica: %d kind: %d", vertex->replica_id, vertex->instance_no, replica_id_, vertex->cmd->cmd->kind_);
      app_next_(*(vertex->cmd->cmd));
    }
  }
  Log_debug("Completed replica: %d instance: %d by replica: %d", replica_id, instance_no, replica_id_);
  // Free lock to execute next command of same dkey
  if (cmds[replica_id][instance_no].cmd->kind_ != MarshallDeputy::CMD_NOOP) {
    in_process_dkeys.erase(cmds[replica_id][instance_no].dkey);
  }
}

void EpaxosServer::StartExecutionAsync(uint64_t replica_id, uint64_t instance_no) {
  Coroutine::CreateRun([this, replica_id, instance_no](){
    StartExecution(replica_id, instance_no);
  });
}

/***********************************
         Helper Methods            *
************************************/

template<class ClassT>
void EpaxosServer::UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t replica_id, uint64_t instance_no) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  EpaxosBallot highest_seen_ballot = cmds[replica_id][instance_no].highest_seen;
  for (auto reply : replies) {
    EpaxosBallot ballot(reply.epoch, reply.ballot_no, reply.replica_id);
    if (ballot.isGreater(highest_seen_ballot)) {
      highest_seen_ballot = ballot;
    }
  }
  cmds[replica_id][instance_no].highest_seen = highest_seen_ballot;
}

pair<uint64_t, unordered_map<uint64_t, uint64_t>> 
EpaxosServer::MergeAttributes(vector<EpaxosPreAcceptReply>& replies) {
  uint64_t seq = 0;
  unordered_map<uint64_t, uint64_t> deps;
  for (auto reply : replies) {
    seq = max(seq, reply.seq);
    for (auto itr : reply.deps) {
      uint64_t dreplica_id = itr.first;
      uint64_t dinstance_no = itr.second;
      deps[dreplica_id] = max(deps[dreplica_id], dinstance_no);
    }
  }
  return make_pair(seq, deps);
}

void EpaxosServer::UpdateInternalAttributes(shared_ptr<Marshallable> &cmd,
                                            string dkey, 
                                            uint64_t replica_id, 
                                            uint64_t instance_no, 
                                            uint64_t seq, 
                                            unordered_map<uint64_t, uint64_t> deps) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  received_till[replica_id] = max(received_till[replica_id], instance_no);
  if (cmd->kind_ == MarshallDeputy::CMD_NOOP) {
    return;
  }                                           
  dkey_seq[dkey] = max(dkey_seq[dkey], seq);
  for (auto itr : deps) {
    uint64_t dreplica_id = itr.first;
    uint64_t dinstance_no = itr.second;
    dkey_deps[dkey][dreplica_id] = max(dkey_deps[dkey][dreplica_id], dinstance_no);
    received_till[dreplica_id] = max(received_till[dreplica_id], dinstance_no);
  }
  dkey_deps[dkey][replica_id] = max(dkey_deps[dkey][replica_id], instance_no);
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
