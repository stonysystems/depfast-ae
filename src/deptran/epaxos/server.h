#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../classic/tpc_command.h"
#include "commo.h"
#include "graph.h"

namespace janus {

#define NOOP_DKEY string("")

enum EpaxosCommandState {
  NOT_STARTED = 0,
  PRE_ACCEPTED = 1,
  PRE_ACCEPTED_EQ = 2,
  ACCEPTED = 3,
  COMMITTED = 4,
  EXECUTED = 5
};

class EpaxosCommand : public EVertex<EpaxosCommand> {
 private:
  uint64_t id_;

 public:
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  map<uint64_t, uint64_t> deps;
  EpaxosCommandState state;
  ballot_t highest_seen;
  ballot_t highest_accepted;
  uint64_t replica_id;
  uint64_t instance_no;
  SharedIntEvent committed_ev{};
  
  EpaxosCommand() {
    cmd = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
    dkey = NOOP_DKEY;
    state = EpaxosCommandState::NOT_STARTED;
    highest_seen = -1;
    highest_accepted =  -1;
    id_ = 0;
  }
  
  uint64_t id() override {
    if (id_ == 0) {
      id_ = replica_id << 32 | instance_no;
    }
    return id_;
  }

  bool isFirstInSCC(shared_ptr<EpaxosCommand>& rhs) override {
    if (this->seq == rhs->seq && this->replica_id == rhs->replica_id) {
      return this->instance_no < rhs->instance_no;
    }
    if (this->seq == rhs->seq) {
      return this->replica_id < rhs->replica_id;
    }
    return this->seq < rhs->seq;
  }
};

class EpaxosGraph: public EGraph<EpaxosCommand> {};

class EpaxosRequest {
 public:
  shared_ptr<Marshallable> cmd;
  string dkey;

  EpaxosRequest(shared_ptr<Marshallable>& cmd, string& dkey) {
    this->cmd = cmd;
    this->dkey = dkey;
  }
};

class EpaxosServer : public TxLogServer {
 private:
  uint64_t curr_replica_id;
  uint64_t curr_epoch = 1;
  uint64_t next_instance_no = 0;
  map<uint64_t, map<uint64_t, shared_ptr<EpaxosCommand>>> cmds;
  map<string, map<uint64_t, uint64_t>> dkey_deps;
  map<string, uint64_t> dkey_seq;
  map<uint64_t, map<uint64_t, pair<uint64_t, uint64_t>>> deferred;

  list<EpaxosRequest> reqs;
  map<uint64_t, uint64_t> received_till;
  map<uint64_t, uint64_t> prepared_till;
  map<uint64_t, uint64_t> committed_till;
  map<string, map<uint64_t, uint64_t>> executed_till;
  map<string, PubSubEvent> in_exec_dkeys;
  map<string, QueueEvent> exec_limiter;

  #ifdef EPAXOS_TEST_CORO
  int commit_timeout = 300000;
  int rpc_timeout = 2000000;
  #else
  int commit_timeout = 10000000; // 10 seconds
  int rpc_timeout = 5000000; // 5 seconds
  #endif

  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  map<uint64_t, Timer> start_times;
  vector<float> commit_times;
  vector<float> exec_times;
  map<int, pair<uint64_t, uint64_t>> instance;
  list<pair<uint64_t, uint64_t>> prepare_reqs;
  bool pause_execution = false;
  int fast = 0;
  int slow = 0;
  #endif

  void HandleRequest(shared_ptr<Marshallable>& cmd, string& dkey);
  bool StartPreAccept(shared_ptr<Marshallable>& cmd, 
                      string& dkey, 
                      ballot_t& ballot, 
                      uint64_t& replica_id, 
                      uint64_t& instance_no,
                      int64_t leader_prev_dep_instance_no,
                      bool recovery);
  bool StartAccept(shared_ptr<Marshallable>& cmd, 
                   string& dkey, 
                   ballot_t& ballot, 
                   uint64_t& seq,
                   map<uint64_t, uint64_t>& deps, 
                   uint64_t& replica_id, 
                   uint64_t& instance_no);
  bool StartCommit(shared_ptr<Marshallable>& cmd, 
                   string& dkey, 
                   ballot_t& ballot, 
                   uint64_t& seq,
                   map<uint64_t, uint64_t>& deps, 
                   uint64_t& replica_id, 
                   uint64_t& instance_no);
  bool StartTryPreAccept(shared_ptr<Marshallable>& cmd, 
                         string& dkey, 
                         ballot_t& ballot, 
                         uint64_t& seq,
                         map<uint64_t, uint64_t>& deps, 
                         uint64_t& replica_id, 
                         uint64_t& instance_no,
                         int64_t leader_prev_dep_instance_no,
                         unordered_set<siteid_t>& preaccepted_sites);
  bool StartPrepare(uint64_t& replica_id, uint64_t& instance_no);
  void PrepareTillCommitted(uint64_t& replica_id, uint64_t& instance_no);
  void StartExecution(uint64_t& replica_id, uint64_t& instance_no);

  /* Helpers */

  shared_ptr<EpaxosCommand> GetCommand(uint64_t replica_id, uint64_t instance_no);
  template<class ClassT> void UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t& replica_id, uint64_t& instance_no);
  void GetLatestAttributes(string& dkey, uint64_t& leader_replica_id, int64_t& leader_prev_dep_instance_no, uint64_t *seq, map<uint64_t, uint64_t> *deps);
  void UpdateAttributes(string& dkey, uint64_t& replica_id, uint64_t& instance_no, uint64_t& seq);
  void MergeAttributes(vector<EpaxosPreAcceptReply>& replies, uint64_t *seq, map<uint64_t, uint64_t> *deps);
  vector<shared_ptr<EpaxosCommand>> GetDependencies(shared_ptr<EpaxosCommand>& ecmd);
  void Execute(shared_ptr<EpaxosCommand>& ecmd);
  void UpdateCommittedTill(uint64_t& replica_id, uint64_t& instance_no);
  bool AllDependenciesCommitted(vector<EpaxosPreAcceptReply>& replies, map<uint64_t, uint64_t>& merged_deps);
  unordered_set<uint64_t> GetCommittedDependencies(map<uint64_t, uint64_t>& merged_deps);
  bool IsInitialBallot(ballot_t& ballot) ;
  int CompareBallots(ballot_t& b1, ballot_t& b2);
  ballot_t GetInitialBallot();
  ballot_t GetNextBallot(ballot_t &ballot);

  void FindTryPreAcceptConflict(shared_ptr<Marshallable>& cmd, 
                                string& dkey, 
                                uint64_t& seq,
                                map<uint64_t, uint64_t>& deps, 
                                uint64_t& replica_id, 
                                uint64_t& instance_no,
                                EpaxosTryPreAcceptStatus *conflict_state,
                                uint64_t *conflict_replica_id, 
                                uint64_t *conflict_instance_no);

  /* RPC handlers */

 public:
  void Start(shared_ptr<Marshallable>& cmd, string dkey);
  EpaxosPreAcceptReply OnPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                          string dkey, 
                                          ballot_t ballot, 
                                          uint64_t seq, 
                                          map<uint64_t, uint64_t> deps, 
                                          uint64_t replica_id, 
                                          uint64_t instance_no);
  EpaxosAcceptReply OnAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                    string dkey, 
                                    ballot_t ballot, 
                                    uint64_t seq, 
                                    map<uint64_t, uint64_t> deps, 
                                    uint64_t replica_id, 
                                    uint64_t instance_no);
  void OnCommitRequest(shared_ptr<Marshallable>& cmd, 
                       string dkey, 
                       ballot_t ballot, 
                       uint64_t seq, 
                       map<uint64_t, uint64_t> deps, 
                       uint64_t replica_id, 
                       uint64_t instance_no);
  EpaxosTryPreAcceptReply OnTryPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                string dkey, 
                                                ballot_t ballot, 
                                                uint64_t seq, 
                                                map<uint64_t, uint64_t> deps, 
                                                uint64_t replica_id, 
                                                uint64_t instance_no);
  EpaxosPrepareReply OnPrepareRequest(ballot_t ballot, 
                                      uint64_t replica_id, 
                                      uint64_t instance_no);

  /* Client request handlers */

public:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  void SetInstance(shared_ptr<Marshallable>& cmd, uint64_t& replica_id, uint64_t& instance_no);
  pair<int64_t, int64_t> GetInstance(int& cmd);
  void GetState(uint64_t& replica_id, 
                uint64_t& instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *dkey,
                uint64_t *seq, 
                map<uint64_t, uint64_t> *deps, 
                status_t *state);
  void Prepare(uint64_t& replica_id, uint64_t& instance_no);
  void PauseExecution(bool pause);
  pair<int, int> GetFastAndSlowPathCount();
  pair<vector<float>, vector<float>> GetLatencies();
  #endif

  /* Do not modify this class below here */

 public:
  EpaxosServer(Frame *frame) ;
  ~EpaxosServer() ;
  
 private:
  bool disconnected_ = false;
  void Setup();

 public:
  void Disconnect(const bool disconnect = true);
  void Reconnect() {
    Disconnect(false);
  }
  bool IsDisconnected();

  virtual bool HandleConflicts(Tx& dtxn,
                               innid_t inn_id,
                               vector<string>& conflicts) {
    verify(0);
  };

  EpaxosCommo* commo() {
    return (EpaxosCommo*)commo_;
  }

};
} // namespace janus
