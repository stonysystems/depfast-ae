#include "testconf.h"
#include "marshallable.h"
#include "../classic/tpc_command.h"

namespace janus {

#if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)

int _test_id_g = 0;
std::chrono::_V2::system_clock::time_point _test_starttime_g;

string map_to_string(unordered_map<uint64_t, uint64_t> m) {
  string result = "";
  for (auto itr : m) {
    result += to_string(itr.first) + ":" + to_string(itr.second) + ", ";
  }
  result = result.substr(0, result.size() - 2);
  return result;
}

string vector_to_string(vector<uint64_t> exec_orders) {
  string s = "";
  for(int i = 0; i < exec_orders.size(); i++) {
    s += to_string(exec_orders[i]) + ", ";
  }
  return s;
}

EpaxosFrame **EpaxosTestConfig::replicas = nullptr;
std::vector<int> EpaxosTestConfig::committed_cmds[NSERVERS];
uint64_t EpaxosTestConfig::rpc_count_last[NSERVERS];

EpaxosTestConfig::EpaxosTestConfig(EpaxosFrame **replicas_) {
  verify(replicas == nullptr);
  replicas = replicas_;
  for (int i = 0; i < NSERVERS; i++) {
    replicas[i]->svr_->rep_frame_ = replicas[i]->svr_->frame_;
    rpc_count_last[i] = 0;
    disconnected_[i] = false;
    slow_[i] = false;
  }
  th_ = std::thread([this](){ netctlLoop(); });
}

void EpaxosTestConfig::SetLearnerAction(void) {
  for (int i = 0; i < NSERVERS; i++) {
    replicas[i]->svr_->RegLearnerAction([i](Marshallable& cmd) {
      verify(cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT);
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      Log_debug("server %d committed value %d", i, command.tx_id_);
      committed_cmds[i].push_back(command.tx_id_);
    });
  }
}

void EpaxosTestConfig::SetRepeatedLearnerAction(function<function<void(Marshallable &)>(int)> callback) {
  for (int i = 0; i < NSERVERS; i++) {
    replicas[i]->svr_->RegLearnerAction(callback(i));
  }
}

void EpaxosTestConfig::Start(int svr, int cmd, string dkey, uint64_t *replica_id, uint64_t *instance_no) {
  // Construct an empty TpcCommitCommand containing cmd as its tx_id_
  auto cmdptr = std::make_shared<TpcCommitCommand>();
  auto vpd_p = std::make_shared<VecPieceData>();
  vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
  cmdptr->tx_id_ = cmd;
  cmdptr->cmd_ = vpd_p;
  auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
  // call Start()
  Log_debug("Starting agreement on svr %d for cmd id %d", svr, cmdptr->tx_id_);
  replicas[svr]->svr_->Start(cmdptr_m, dkey, replica_id, instance_no);
}

void EpaxosTestConfig::GetState(int svr, 
                                uint64_t replica_id, 
                                uint64_t instance_no, 
                                shared_ptr<Marshallable> *cmd, 
                                string *dkey,
                                uint64_t *seq, 
                                unordered_map<uint64_t, uint64_t> *deps, 
                                status_t *state) {
  replicas[svr]->svr_->GetState(replica_id, instance_no, cmd, dkey, seq, deps, state);
}

void EpaxosTestConfig::Prepare(int svr, uint64_t replica_id, uint64_t instance_no) {
  replicas[svr]->svr_->Prepare(replica_id, instance_no);
}

void EpaxosTestConfig::PauseExecution(bool pause) {
  for (int i = 0; i < NSERVERS; i++) {
    replicas[i]->svr_->PauseExecution(pause);
  }
}

vector<int> EpaxosTestConfig::GetExecutedCommands(int svr) {
  return committed_cmds[svr];
}

int EpaxosTestConfig::GetRequestCount(int svr) {
  return replicas[svr]->svr_->GetRequestCount();
}

int EpaxosTestConfig::NExecuted(uint64_t tx_id, int n) {
  auto start = chrono::steady_clock::now();
  int ne = 0;
  while ((chrono::steady_clock::now() - start) < chrono::seconds{2}) {
    ne = 0;
    for (int i = 0; i < NSERVERS; i++) {
      auto cmd = std::find(committed_cmds[i].begin(), committed_cmds[i].end(), tx_id);
      if (cmd != committed_cmds[i].end()) {
        ne++;
      }
    } 
    if (ne >= n) {
      return ne;
    }
    Coroutine::Sleep(100000);
    Log_debug("%d executed server for cmd: %d", ne, tx_id);
  }
  return ne;
}

bool EpaxosTestConfig::ExecutedPairsInOrder(vector<pair<uint64_t, uint64_t>> expected_pairs) {
  for (int svr = 0; svr < NSERVERS; svr++) {
    for (int i = 0; i < expected_pairs.size(); i++) {
      auto occ = find(committed_cmds[svr].begin(), committed_cmds[svr].end(), expected_pairs[i].first);
      if (occ == committed_cmds[svr].end()) continue;
      auto occ_next = find(committed_cmds[svr].begin(), committed_cmds[svr].end(), expected_pairs[i].second);
      if (occ_next == committed_cmds[svr].end()) continue;
      if (occ - committed_cmds[svr].begin() > occ_next - committed_cmds[svr].begin()) {
        Log_debug("cmd: %d executed before cmd: %d", expected_pairs[i].second, expected_pairs[i].first);
        return false;
      }
    }
  }
  return true;
}

bool EpaxosTestConfig::ExecutedInSameOrder(unordered_set<uint64_t> dependent_cmds) {
  vector<vector<uint64_t>> exec_orders;
  uint64_t longest_svr = 0;
  for (int svr = 0; svr < NSERVERS; svr++) {
    exec_orders.push_back(vector<uint64_t>());
    for (auto itr = committed_cmds[svr].begin(); itr != committed_cmds[svr].end(); itr++) {
      if (dependent_cmds.count(*itr) > 0) {
        exec_orders[svr].push_back(*itr);
      }
    }
    if (exec_orders[svr].size() > exec_orders[longest_svr].size()) {
      longest_svr = svr;
    }
    Log_debug("Executed %d/%d cmds in server: %d", exec_orders[svr].size(), dependent_cmds.size(), svr);
    verify(exec_orders[svr].size() > 0); // atleast some commands are executed
  }
  for (int svr = 0; svr < NSERVERS; svr++) {
    for (int i = 0; i < exec_orders[svr].size(); i++) {
      if (exec_orders[svr][i] != exec_orders[longest_svr][i]) {
        Log_debug("Execution order for cmd: %d is different in server: %d from longest %d", exec_orders[svr][i], svr, longest_svr);
        string cmds = vector_to_string(exec_orders[longest_svr]);
        int i = 0;
        while (i + 100 <= cmds.size()){
          Log_debug("Longest: %s", cmds.substr(i, 100).c_str());
          i += 100;
        }
        Log_debug("Longest: %s", cmds.substr(i, cmds.size()).c_str());
        cmds = vector_to_string(exec_orders[svr]);
        i = 0;
        while (i + 100 <= cmds.size()){
          Log_debug("Got: %s", cmds.substr(i, 100).c_str());
          i += 100;
        }
        Log_debug("Got: %s", cmds.substr(i, cmds.size()).c_str());
        return false;
      }
    }
  }
  return true;
}


int EpaxosTestConfig::NCommitted(uint64_t replica_id, uint64_t instance_no, int n) {
  bool cnoop;
  string cdkey;
  uint64_t cseq;
  unordered_map<uint64_t, uint64_t> cdeps;
  return NCommitted(replica_id, instance_no, n, &cnoop, &cdkey, &cseq, &cdeps);
}

int EpaxosTestConfig::NCommitted(uint64_t replica_id, 
                                  uint64_t instance_no, 
                                  int n, 
                                  bool *cnoop, 
                                  string *cdkey, 
                                  uint64_t *cseq, 
                                  unordered_map<uint64_t, uint64_t> *cdeps) {
  auto start = chrono::steady_clock::now();
  int nc = 0;
  while ((chrono::steady_clock::now() - start) < chrono::seconds{2}) {
    bool init = true;
    int committed_cmd_kind;
    string committed_dkey;
    uint64_t committed_seq;
    unordered_map<uint64_t, uint64_t> committed_deps;
    nc = 0;
    for (int j=0; j< NSERVERS; j++) {
      shared_ptr<Marshallable> cmd_;
      string dkey_;
      uint64_t seq_;
      unordered_map<uint64_t, uint64_t> deps_;
      status_t state_;
      GetState(j, replica_id, instance_no, &cmd_, &dkey_, &seq_, &deps_, &state_);
      if (state_ == EpaxosCommandState::COMMITTED || state_ == EpaxosCommandState::EXECUTED) {
        nc++;
        if(init) {
          init = false;
          committed_cmd_kind = cmd_->kind_;
          committed_dkey = dkey_;
          committed_seq = seq_;
          committed_deps = deps_;
          continue;
        }
        if (committed_cmd_kind != cmd_->kind_) {
          Log_debug("committed different commands, (expected %d got %d)", committed_cmd_kind, cmd_->kind_);
          return -1;
        }
        if (committed_dkey != dkey_) {
          Log_debug("committed different dependency keys, (expected %s got %s)", committed_dkey.c_str(), dkey_.c_str());
          return -2;
        }
        if (committed_seq != seq_ ) {
          Log_debug("committed different sequence numbers, (expected %d got %d)", committed_seq, seq_);
          return -3;
        }
        if (committed_deps != deps_) {
          Log_debug("committed different dependencies, (expected %s got %s)", map_to_string(committed_deps).c_str(), map_to_string(deps_).c_str());
          return -4;
        }
      }
    }
    if (nc >= n) {
      *cnoop = committed_cmd_kind == MarshallDeputy::CMD_NOOP;
      *cdkey = committed_dkey;
      *cseq = committed_seq;
      *cdeps = committed_deps;
      return nc;
    }
    Coroutine::Sleep(100000);
    Log_debug("%d committed server for replica: %d instance: %d", nc, replica_id, instance_no);
  }
  Log_debug("%d committed server for replica: %d instance: %d", nc, replica_id, instance_no);
  return nc;
}

int EpaxosTestConfig::NAccepted(uint64_t replica_id, uint64_t instance_no, int n) {
  auto start = chrono::steady_clock::now();
  int na = 0;
  while ((chrono::steady_clock::now() - start) < chrono::seconds{2}) {
    na = 0;
    for (int j=0; j< NSERVERS; j++) {
      shared_ptr<Marshallable> cmd_;
      string dkey_;
      uint64_t seq_;
      unordered_map<uint64_t, uint64_t> deps_;
      status_t state_;
      GetState(j, replica_id, instance_no, &cmd_, &dkey_, &seq_, &deps_, &state_);
      if (state_ == EpaxosCommandState::ACCEPTED ) {
        na++;
      }
    }
    if (na >= n) {
      return na;
    }
    Coroutine::Sleep(100000);
    Log_debug("%d accepted servers for replica: %d instance: %d", na, replica_id, instance_no);
  }
  Log_debug("%d accepted servers for replica: %d instance: %d", na, replica_id, instance_no);
  return na;
}

int EpaxosTestConfig::NPreAccepted(uint64_t replica_id, uint64_t instance_no, int n) {
  auto start = chrono::steady_clock::now();
  int na = 0;
  while ((chrono::steady_clock::now() - start) < chrono::seconds{2}) {
    na = 0;
    for (int j=0; j< NSERVERS; j++) {
      shared_ptr<Marshallable> cmd_;
      string dkey_;
      uint64_t seq_;
      unordered_map<uint64_t, uint64_t> deps_;
      status_t state_;
      GetState(j, replica_id, instance_no, &cmd_, &dkey_, &seq_, &deps_, &state_);
      if (state_ == EpaxosCommandState::PRE_ACCEPTED || state_ == EpaxosCommandState::PRE_ACCEPTED_EQ) {
        na++;
      }
    }
    if (na >= n) {
      return na;
    }
    Coroutine::Sleep(100000);
    Log_debug("%d pre-accepted servers for replica: %d instance: %d", na, replica_id, instance_no);
  }
  Log_debug("%d pre-accepted servers for replica: %d instance: %d", na, replica_id, instance_no);
  return na;
}

int EpaxosTestConfig::DoAgreement(int cmd, 
                                  string dkey, 
                                  int n, 
                                  bool retry, 
                                  bool *cnoop, 
                                  string *cdkey, 
                                  uint64_t *cseq, 
                                  unordered_map<uint64_t, uint64_t> *cdeps) {
  Log_debug("Doing 1 round of Epaxos agreement");
  auto start = chrono::steady_clock::now();
  while ((chrono::steady_clock::now() - start) < chrono::seconds{10}) {
    uint64_t replica_id, instance_no;
    Coroutine::Sleep(20000);
    // Call Start() to all servers until alive command leader is found
    for (int i = 0; i < NSERVERS; i++) {
      // skip disconnected servers
      if (replicas[i]->svr_->IsDisconnected())
        continue;
      Start(i, cmd, dkey, &replica_id, &instance_no);
      Log_debug("starting cmd ldr=%d cmd=%d", replicas[i]->svr_->loc_id_, cmd); // TODO: Print instance and ballot
      break;
    }
    // If Start() successfully called, wait for agreement
    int status = NCommitted(replica_id, instance_no, n, cnoop, cdkey, cseq, cdeps);
    if (status > 0) {
      return status;
    }
    if (!retry) {
      Log_debug("failed to reach agreement for replica: %d instance: %d", replica_id, instance_no);
      return 0;
    }
  }
  Log_debug("failed to reach agreement end");
  return 0;
}

void EpaxosTestConfig::Disconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(!disconnected_[svr]);
  disconnect(svr, true);
  disconnected_[svr] = true;
}

void EpaxosTestConfig::Reconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(disconnected_[svr]);
  reconnect(svr);
  disconnected_[svr] = false;
}

int EpaxosTestConfig::NDisconnected(void) {
  int count = 0;
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i])
      count++;
  }
  return count;
}

bool EpaxosTestConfig::IsDisconnected(int svr) {
  return isDisconnected(svr);
}

void EpaxosTestConfig::SetUnreliable(bool unreliable) {
  std::unique_lock<std::mutex> lk(cv_m_);
  verify(!finished_);
  if (unreliable) {
    verify(!unreliable_);
    // lk acquired cv_m_ in state 1 or 0
    unreliable_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  } else {
    verify(unreliable_);
    // lk acquired cv_m_ in state 2 or 0
    unreliable_ = false;
    // wait until netctlLoop moves cv_m_ from state 2 (or 0) to state 1,
    // restoring the network to reliable state in the process.
    lk.unlock();
    lk.lock();
  }
}

bool EpaxosTestConfig::IsUnreliable(void) {
  return unreliable_;
}


bool EpaxosTestConfig::AnySlow(void) {
  std::lock_guard<std::mutex> prlk(disconnect_mtx_);
  for (int i = 0; i < NSERVERS; i++) {
    if (slow_[i])
      return true;
  }
  return false;
}

void EpaxosTestConfig::SetSlow(int svr, int msec) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> prlk(disconnect_mtx_);
  verify(!slow_[svr]);
  slow_[svr] = true;
  this->slow(svr, msec);
}

void EpaxosTestConfig::ResetSlow(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> prlk(disconnect_mtx_);
  verify(slow_[svr]);
  slow_[svr] = false;
  this->slow(svr, 0);
}

void EpaxosTestConfig::Shutdown(void) {
  // trigger netctlLoop shutdown
  {
    std::unique_lock<std::mutex> lk(cv_m_);
    verify(!finished_);
    // lk acquired cv_m_ in state 0, 1, or 2
    finished_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  }
  // wait for netctlLoop thread to exit
  th_.join();
  // Reconnect() all Deconnect()ed servers
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i]) {
      Reconnect(i);
    }
  }
}

uint64_t EpaxosTestConfig::RpcCount(int svr, bool reset) {
  std::lock_guard<std::recursive_mutex> lk(replicas[svr]->commo_->rpc_mtx_);
  uint64_t count = replicas[svr]->commo_->rpc_count_;
  uint64_t count_last = rpc_count_last[svr];
  if (reset) {
    rpc_count_last[svr] = count;
  }
  verify(count >= count_last);
  return count - count_last;
}

uint64_t EpaxosTestConfig::RpcTotal(void) {
  uint64_t total = 0;
  for (int i = 0; i < NSERVERS; i++) {
    total += replicas[i]->commo_->rpc_count_;
  }
  return total;
}

void EpaxosTestConfig::netctlLoop(void) {
  int i;
  bool isdown;
  // cv_m_ unlocked state 0 (finished_ == false)
  std::unique_lock<std::mutex> lk(cv_m_);
  while (!finished_) {
    if (!unreliable_) {
      {
        std::lock_guard<std::mutex> prlk(disconnect_mtx_);
        // unset all unreliable-related disconnects and slows
        for (i = 0; i < NSERVERS; i++) {
          if (!disconnected_[i]) {
            reconnect(i, true);
            slow(i, 0);
          }
        }
      }
      // sleep until unreliable_ or finished_ is set
      // cv_m_ unlocked state 1 (unreliable_ == false && finished_ == false)
      cv_.wait(lk, [this](){ return unreliable_ || finished_; });
      continue;
    }
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      for (i = 0; i < NSERVERS; i++) {
        // skip server if it was disconnected using Disconnect()
        if (disconnected_[i]) {
          continue;
        }
        // server has DOWNRATE_N / DOWNRATE_D chance of being down
        if ((rand() % DOWNRATE_D) < DOWNRATE_N) {
          // disconnect server if not already disconnected in the previous period
          disconnect(i, true);
        } else {
          // Server not down: random slow timeout
          // Reconnect server if it was disconnected in the previous period
          reconnect(i, true);
          // server's slow timeout should be btwn 0-(MAXSLOW-1) ms
          slow(i, rand() % MAXSLOW);
        }
      }
    }
    // change unreliable state every 0.1s
    lk.unlock();
    usleep(100000);
    // cv_m_ unlocked state 2 (unreliable_ == true && finished_ == false)
    lk.lock();
  }
  // If network is still unreliable, unset it
  if (unreliable_) {
    unreliable_ = false;
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      // unset all unreliable-related disconnects and slows
      for (i = 0; i < NSERVERS; i++) {
        if (!disconnected_[i]) {
          reconnect(i, true);
          slow(i, 0);
        }
      }
    }
  }
  // cv_m_ unlocked state 3 (unreliable_ == false && finished_ == true)
}

bool EpaxosTestConfig::isDisconnected(int svr) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  return replicas[svr]->svr_->IsDisconnected();
}

void EpaxosTestConfig::disconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (!isDisconnected(svr)) {
    // simulate disconnected server
    replicas[svr]->svr_->Disconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void EpaxosTestConfig::reconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (isDisconnected(svr)) {
    // simulate reconnected server
    replicas[svr]->svr_->Reconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void EpaxosTestConfig::slow(int svr, uint32_t msec) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  verify(!isDisconnected(svr));
  replicas[svr]->commo_->rpc_poll_->slow(msec * 1000);
}

#endif

}
