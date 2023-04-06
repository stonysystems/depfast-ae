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

class EpaxosBallot {
 public:
  epoch_t epoch;
  ballot_t ballot_no;
  uint64_t replica_id;

  EpaxosBallot() {
    this->epoch = 0;
    this->ballot_no = -1;
    this->replica_id = 0;
  }

  EpaxosBallot(epoch_t epoch, ballot_t ballot_no, uint64_t replica_id) {
    this->epoch = epoch;
    this->ballot_no = ballot_no;
    this->replica_id = replica_id;
  }

  bool isDefault() {
    return ballot_no == 0;
  }

  int isGreater(EpaxosBallot& rhs) {
    return epoch > rhs.epoch 
           || (epoch == rhs.epoch && ballot_no > rhs.ballot_no)
           || (epoch == rhs.epoch && ballot_no == rhs.ballot_no && replica_id > rhs.replica_id);
  }

  int isGreaterOrEqual(EpaxosBallot& rhs) {
    return epoch > rhs.epoch 
           || (epoch == rhs.epoch && ballot_no > rhs.ballot_no)
           || (epoch == rhs.epoch && ballot_no == rhs.ballot_no && replica_id >= rhs.replica_id);
  }

  int operator==(EpaxosBallot& rhs) {
    return epoch == rhs.epoch && ballot_no == rhs.ballot_no && replica_id == rhs.replica_id;
  }
};

class EpaxosCommand {
 public:
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  unordered_map<uint64_t, uint64_t> deps;
  EpaxosCommandState state;
  EpaxosBallot highest_seen;
  EpaxosBallot highest_accepted;
  bool preparing;

  EpaxosCommand() {
    cmd = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
    dkey = NOOP_DKEY;
    state = EpaxosCommandState::NOT_STARTED;
    preparing = false;
  }

  EpaxosCommand(shared_ptr<Marshallable> cmd, string dkey, uint64_t seq, unordered_map<uint64_t, uint64_t>& deps, EpaxosBallot highest_seen, EpaxosCommandState state) {
    this->cmd = cmd;
    this->dkey = dkey;
    this->seq = seq;
    this->deps = deps;
    this->highest_seen = highest_seen;
    this->state = state;
    this->preparing = false;
  }
};

class EpaxosRequest {
 public:
  shared_ptr<Marshallable> cmd;
  string dkey;

  EpaxosRequest(shared_ptr<Marshallable> cmd, string dkey) {
    this->cmd = cmd;
    this->dkey = dkey;
  }
};

class EpaxosVertex: public EVertex<EpaxosVertex> {
 private:
  uint64_t id_;

 public:
  EpaxosCommand *cmd;
  uint64_t replica_id;
  uint64_t instance_no;

  EpaxosVertex(EpaxosCommand *cmd, uint64_t replica_id, uint64_t instance_no) : EVertex() {
    this->cmd = cmd;
    this->replica_id = replica_id;
    this->instance_no = instance_no;
    this->id_ = replica_id << 32 | instance_no;
  }

  uint64_t id() override {
    return id_;
  }

  bool isFirstInSCC(shared_ptr<EpaxosVertex> &rhs) override {
    if (this->cmd->seq == rhs->cmd->seq && this->replica_id == rhs->replica_id) {
      return this->instance_no < rhs->instance_no;
    }
    if (this->cmd->seq == rhs->cmd->seq) {
      return this->replica_id < rhs->replica_id;
    }
    return this->cmd->seq < rhs->cmd->seq;
  }
};

class EpaxosGraph: public EGraph<EpaxosVertex> {};

class EpaxosServer : public TxLogServer {
 private:
  uint64_t replica_id_;
  epoch_t curr_epoch = 1;
  uint64_t next_instance_no = 0;
  unordered_map<uint64_t, unordered_map<uint64_t, EpaxosCommand>> cmds;
  unordered_map<string, unordered_map<uint64_t, uint64_t>> dkey_deps;
  unordered_map<string, uint64_t> dkey_seq;
  unordered_map<uint64_t, unordered_map<uint64_t, pair<uint64_t, uint64_t>>> deferred;
  list<EpaxosRequest> reqs;
  unordered_map<uint64_t, uint64_t> received_till;
  unordered_map<uint64_t, uint64_t> prepared_till;
  unordered_map<string, unordered_map<uint64_t, uint64_t>> executed_till;
  unordered_set<string> in_process_dkeys;
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  unordered_map<int, pair<uint64_t, uint64_t>> instance;
  list<pair<uint64_t, uint64_t>> prepare_reqs;
  bool pause_execution = false;
  int inprocess_reqs = 0;
  int fast = 0;
  int slow = 0;
  #endif
  int rpc_timeout = 20000000;

  EpaxosRequest CreateEpaxosRequest(shared_ptr<Marshallable> cmd, string dkey);
  bool StartPreAccept(shared_ptr<Marshallable> cmd, 
                      string dkey, 
                      EpaxosBallot ballot, 
                      uint64_t replica_id, 
                      uint64_t instance_no,
                      int64_t leader_dep_instance,
                      bool recovery);
  bool StartAccept(shared_ptr<Marshallable> cmd, 
                   string dkey, 
                   EpaxosBallot ballot, 
                   uint64_t seq,
                   unordered_map<uint64_t, uint64_t> deps, 
                   uint64_t replica_id, 
                   uint64_t instance_no);
  bool StartCommit(shared_ptr<Marshallable> cmd, 
                   string dkey, 
                   EpaxosBallot ballot, 
                   uint64_t seq,
                   unordered_map<uint64_t, uint64_t> deps, 
                   uint64_t replica_id, 
                   uint64_t instance_no);
  bool StartTryPreAccept(shared_ptr<Marshallable> cmd, 
                         string dkey, 
                         EpaxosBallot ballot, 
                         uint64_t seq,
                         unordered_map<uint64_t, uint64_t> deps, 
                         uint64_t replica_id, 
                         uint64_t instance_no,
                         int64_t leader_dep_instance,
                         unordered_set<siteid_t> preaccepted_sites);
  void PrepareTillCommitted(uint64_t replica_id, uint64_t instance_no);
  bool StartPrepare(uint64_t replica_id, uint64_t instance_no);
  // returns 0 if noop, 1 if executed, 2 if added to graph
  int CreateEpaxosGraph(uint64_t replica_id, uint64_t instance_no, EpaxosGraph *graph);
  void StartExecution(uint64_t replica_id, uint64_t instance_no);
  void StartExecutionAsync(uint64_t replica_id, uint64_t instance_no);

  void FindTryPreAcceptConflict(shared_ptr<Marshallable> cmd, 
                                string dkey, 
                                uint64_t seq,
                                unordered_map<uint64_t, uint64_t> deps, 
                                uint64_t replica_id, 
                                uint64_t instance_no,
                                EpaxosTryPreAcceptStatus *conflict_state,
                                uint64_t *conflict_replica_id, 
                                uint64_t *conflict_instance_no);
  template<class ClassT>
  void UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t replica_id, uint64_t instance_no);
  pair<uint64_t, unordered_map<uint64_t, uint64_t>>
  MergeAttributes(vector<EpaxosPreAcceptReply>& replies);
  bool AllDependenciesCommitted(vector<EpaxosPreAcceptReply>& replies, unordered_map<uint64_t, uint64_t> deps);
  void UpdateInternalAttributes(shared_ptr<Marshallable> &cmd,
                                string dkey, 
                                uint64_t replica_id, 
                                uint64_t instance_no, 
                                uint64_t seq, 
                                unordered_map<uint64_t, uint64_t> deps);

  /* RPC handlers */

 public:
  EpaxosPreAcceptReply OnPreAcceptRequest(shared_ptr<Marshallable> cmd, 
                                          string dkey, 
                                          EpaxosBallot ballot, 
                                          uint64_t seq, 
                                          unordered_map<uint64_t, uint64_t> deps, 
                                          uint64_t replica_id, 
                                          uint64_t instance_no);
  EpaxosAcceptReply OnAcceptRequest(shared_ptr<Marshallable> cmd_, 
                                    string dkey, 
                                    EpaxosBallot ballot, 
                                    uint64_t seq, 
                                    unordered_map<uint64_t, uint64_t> deps, 
                                    uint64_t replica_id, 
                                    uint64_t instance_no);
  void OnCommitRequest(shared_ptr<Marshallable> cmd_, 
                       string dkey, 
                       EpaxosBallot ballot, 
                       uint64_t seq, 
                       unordered_map<uint64_t, uint64_t> deps, 
                       uint64_t replica_id, 
                       uint64_t instance_no);
  EpaxosTryPreAcceptReply OnTryPreAcceptRequest(shared_ptr<Marshallable> cmd_, 
                                                string dkey, 
                                                EpaxosBallot ballot, 
                                                uint64_t seq_, 
                                                unordered_map<uint64_t, uint64_t> deps_, 
                                                uint64_t replica_id, 
                                                uint64_t instance_no);
  EpaxosPrepareReply OnPrepareRequest(EpaxosBallot ballot, 
                                      uint64_t replica_id, 
                                      uint64_t instance_no);

  /* Client request handlers */

public:
  void Start(shared_ptr<Marshallable> cmd, string dkey);
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  void SetInstance(shared_ptr<Marshallable> cmd, uint64_t replica_id, uint64_t instance_no);
  pair<int64_t, int64_t> GetInstance(int cmd);
  void GetState(uint64_t replica_id, 
                uint64_t instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *dkey,
                uint64_t *seq, 
                unordered_map<uint64_t, uint64_t> *deps, 
                status_t *state);
  void Prepare(uint64_t replica_id, uint64_t instance_no);
  void PauseExecution(bool pause);
  int GetRequestCount();
  pair<int, int> GetFastAndSlowPathCount();
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
