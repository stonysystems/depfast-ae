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

class EpaxosCommand : public EVertex<EpaxosCommand> {
 private:
  uint64_t id_;

 public:
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  unordered_map<uint64_t, uint64_t> deps;
  EpaxosCommandState state;
  EpaxosBallot highest_seen;
  EpaxosBallot highest_accepted;
  uint64_t replica_id;
  uint64_t instance_no;
  SharedIntEvent committed_ev{};
  
  EpaxosCommand() {
    cmd = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
    dkey = NOOP_DKEY;
    state = EpaxosCommandState::NOT_STARTED;
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

class InstanceEvent : public IntEvent {
 public:
  uint64_t replica_{};
  uint64_t instance_{};

  InstanceEvent(uint64_t replica, uint64_t instance): IntEvent() {
    replica_ = replica;
    instance_ = instance;
  }

};

class ExecutionEvent {
 public:
  list<shared_ptr<InstanceEvent>> events_{};
  std::map<uint64_t, uint64_t> curr_max;
  std::map<uint64_t, std::vector<uint64_t>> higher_instances;

  void WaitIfGreater(uint64_t replica, uint64_t instance) {
    if (curr_max[replica] == 0 || instance <= curr_max[replica]) {
      higher_instances[replica].push_back(instance);
      curr_max[replica] = instance;
      return;
    }
    auto sp_ev = Reactor::CreateSpEvent<InstanceEvent>(replica, instance);
    events_.insert(events_.end(), sp_ev);
    sp_ev->Wait();
    curr_max[replica] = instance;
    higher_instances[replica].push_back(instance);
  }

  void Reset(uint64_t replica, uint64_t instance) {
    higher_instances[replica].erase(find(higher_instances[replica].begin(), higher_instances[replica].end(), instance));
    if (curr_max[replica] == instance) {
      if (higher_instances[replica].size() == 0) {
        curr_max[replica] = 0;
      } else {
        curr_max[replica] = *min_element(begin(higher_instances[replica]), end(higher_instances[replica]));
      }
    }
    if (higher_instances[replica].size() == 0) {
      auto itr = events_.begin(); 
      while (itr != events_.end()) {
        if ((*itr)->status_ <= Event::WAIT) {
          if ((*itr)->replica_ == replica && (curr_max[(*itr)->replica_] == 0 || (*itr)->instance_ <= curr_max[(*itr)->replica_])) {
            auto sp_ev = *itr;
            events_.erase(itr++);
            sp_ev->Set(1);
          } else {
            itr++;
          }
        } else {
          itr++;
        }
      }
    }
  }
};

class EpaxosServer : public TxLogServer {
 private:
  uint64_t replica_id_;
  epoch_t curr_epoch = 1;
  uint64_t next_instance_no = 0;
  unordered_map<uint64_t, unordered_map<uint64_t, shared_ptr<EpaxosCommand>>> cmds;
  unordered_map<string, unordered_map<uint64_t, uint64_t>> dkey_deps;
  unordered_map<string, uint64_t> dkey_seq;
  unordered_map<uint64_t, unordered_map<uint64_t, pair<uint64_t, uint64_t>>> deferred;
  list<EpaxosRequest> reqs;
  unordered_map<uint64_t, uint64_t> received_till;
  unordered_map<uint64_t, uint64_t> prepared_till;
  unordered_map<string, unordered_map<uint64_t, uint64_t>> executed_till;
  unordered_map<string, PubSubEvent> in_process_dkeys;
  unordered_map<string, PubSubEvent> in_exec_dkeys;
  // unordered_map<string, ExecutionEvent> in_exec_dkeys;
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  unordered_map<int, pair<uint64_t, uint64_t>> instance;
  list<pair<uint64_t, uint64_t>> prepare_reqs;
  bool pause_execution = false;
  int fast = 0;
  int slow = 0;
  #endif
  #ifdef WIDE_AREA
  int rpc_timeout = 5000000;
  #else
  int rpc_timeout = 2000000;
  #endif

  void HandleRequest(shared_ptr<Marshallable>& cmd, string& dkey);
  bool StartPreAccept(shared_ptr<Marshallable>& cmd, 
                      string& dkey, 
                      EpaxosBallot& ballot, 
                      uint64_t& replica_id, 
                      uint64_t& instance_no,
                      int64_t leader_dep_instance,
                      bool recovery);
  bool StartAccept(shared_ptr<Marshallable>& cmd, 
                   string& dkey, 
                   EpaxosBallot& ballot, 
                   uint64_t& seq,
                   unordered_map<uint64_t, uint64_t>& deps, 
                   uint64_t& replica_id, 
                   uint64_t& instance_no);
  bool StartCommit(shared_ptr<Marshallable>& cmd, 
                   string& dkey, 
                   EpaxosBallot& ballot, 
                   uint64_t& seq,
                   unordered_map<uint64_t, uint64_t>& deps, 
                   uint64_t& replica_id, 
                   uint64_t& instance_no);
  bool StartTryPreAccept(shared_ptr<Marshallable>& cmd, 
                         string& dkey, 
                         EpaxosBallot& ballot, 
                         uint64_t& seq,
                         unordered_map<uint64_t, uint64_t>& deps, 
                         uint64_t& replica_id, 
                         uint64_t& instance_no,
                         int64_t leader_dep_instance,
                         unordered_set<siteid_t>& preaccepted_sites);
  bool StartPrepare(uint64_t& replica_id, uint64_t& instance_no);
  void PrepareTillCommitted(uint64_t& replica_id, uint64_t& instance_no);
  void StartExecution(uint64_t& replica_id, uint64_t& instance_no);

  vector<shared_ptr<EpaxosCommand>> GetDependencies(shared_ptr<EpaxosCommand>& vertex);
  void Execute(shared_ptr<EpaxosCommand>& vertex);
  void FindTryPreAcceptConflict(shared_ptr<Marshallable>& cmd, 
                                string& dkey, 
                                uint64_t& seq,
                                unordered_map<uint64_t, uint64_t>& deps, 
                                uint64_t& replica_id, 
                                uint64_t& instance_no,
                                EpaxosTryPreAcceptStatus *conflict_state,
                                uint64_t *conflict_replica_id, 
                                uint64_t *conflict_instance_no);
  template<class ClassT>
  void UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t& replica_id, uint64_t& instance_no);
  pair<uint64_t, unordered_map<uint64_t, uint64_t>>
  MergeAttributes(vector<EpaxosPreAcceptReply>& replies);
  bool AllDependenciesCommitted(vector<EpaxosPreAcceptReply>& replies, unordered_map<uint64_t, uint64_t>& deps);
  void UpdateInternalAttributes(shared_ptr<Marshallable>& cmd,
                                string& dkey, 
                                uint64_t& replica_id, 
                                uint64_t& instance_no, 
                                uint64_t& seq, 
                                unordered_map<uint64_t, uint64_t>& deps);
  shared_ptr<EpaxosCommand> GetCommand(uint64_t replica_id, uint64_t instance_no);

  /* RPC handlers */

 public:
  EpaxosPreAcceptReply OnPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                          string dkey, 
                                          EpaxosBallot& ballot, 
                                          uint64_t seq, 
                                          unordered_map<uint64_t, uint64_t> deps, 
                                          uint64_t replica_id, 
                                          uint64_t instance_no);
  EpaxosAcceptReply OnAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                    string dkey, 
                                    EpaxosBallot& ballot, 
                                    uint64_t seq, 
                                    unordered_map<uint64_t, uint64_t> deps, 
                                    uint64_t replica_id, 
                                    uint64_t instance_no);
  void OnCommitRequest(shared_ptr<Marshallable>& cmd, 
                       string dkey, 
                       EpaxosBallot& ballot, 
                       uint64_t seq, 
                       unordered_map<uint64_t, uint64_t> deps, 
                       uint64_t replica_id, 
                       uint64_t instance_no);
  EpaxosTryPreAcceptReply OnTryPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                                string dkey, 
                                                EpaxosBallot& ballot, 
                                                uint64_t seq, 
                                                unordered_map<uint64_t, uint64_t> deps, 
                                                uint64_t replica_id, 
                                                uint64_t instance_no);
  EpaxosPrepareReply OnPrepareRequest(EpaxosBallot& ballot, 
                                      uint64_t replica_id, 
                                      uint64_t instance_no);

  /* Client request handlers */

public:
  void Start(shared_ptr<Marshallable>& cmd, string& dkey);
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  void SetInstance(shared_ptr<Marshallable>& cmd, uint64_t& replica_id, uint64_t& instance_no);
  pair<int64_t, int64_t> GetInstance(int& cmd);
  void GetState(uint64_t& replica_id, 
                uint64_t& instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *dkey,
                uint64_t *seq, 
                unordered_map<uint64_t, uint64_t> *deps, 
                status_t *state);
  void Prepare(uint64_t& replica_id, uint64_t& instance_no);
  void PauseExecution(bool pause);
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
