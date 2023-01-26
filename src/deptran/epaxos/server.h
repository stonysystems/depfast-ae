#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include "commo.h"


namespace janus {

enum EpaxosCommandState {
  NOT_STARTED = 0,
  PRE_ACCEPTED = 1,
  ACCEPTED = 2,
  COMMITTED = 3,
  EXECUTED = 4
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

  int isGreater(EpaxosBallot& rhs) {
    return epoch > rhs.epoch || (epoch == rhs.epoch && ballot_no > rhs.ballot_no);
  }

  int isGreaterOrEqual(EpaxosBallot& rhs) {
    return epoch > rhs.epoch 
        || (epoch == rhs.epoch && ballot_no > rhs.ballot_no) 
        || (epoch == rhs.epoch && ballot_no == rhs.ballot_no && replica_id == rhs.replica_id);
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

  EpaxosCommand() {}

  EpaxosCommand(shared_ptr<Marshallable>& cmd, string dkey, uint64_t seq, unordered_map<uint64_t, uint64_t>& deps, EpaxosBallot highest_seen, EpaxosCommandState state) {
    this->cmd = cmd;
    this->dkey = dkey;
    this->seq = seq;
    this->deps = deps;
    this->highest_seen = highest_seen;
    this->state = state;
  }
};

class EpaxosRecoveryCommand {
 public:
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  unordered_map<uint64_t, uint64_t> deps;
  int count;

  EpaxosRecoveryCommand(shared_ptr<Marshallable>& cmd, string dkey, uint64_t seq, unordered_map<uint64_t, uint64_t>& deps, int count) {
    this->cmd = cmd;
    this->dkey = dkey;
    this->seq = seq;
    this->deps = deps;
    this->count = count;
  }
};

class EpaxosRequest {
 public:
  shared_ptr<Marshallable> cmd;
  string dkey;
  EpaxosBallot ballot;
  uint64_t replica_id;
  uint64_t instance_no;
  int64_t leader_dep_instance;

  EpaxosRequest(shared_ptr<Marshallable>& cmd, 
                string dkey, 
                EpaxosBallot& ballot, 
                uint64_t replica_id, 
                uint64_t instance_no, 
                int64_t leader_dep_instance) {
    this->cmd = cmd;
    this->dkey = dkey;
    this->ballot = ballot;
    this->replica_id = replica_id;
    this->instance_no = instance_no;
    this->leader_dep_instance = leader_dep_instance;
  }
};

// class EpaxosInstance: public Vertex<EpaxosInstance> {
//  public:
//   uint64_t replica_id;
//   uint64_t instance_no;

//   EpaxosInstance(uint64_t id) {
//     this->replica_id = id >> 8;
//     this->instance_no = id - (this->replica_id << 8);
//   }

//   EpaxosInstance(uint64_t replica_id, uint64_t instance_no) {
//     this->replica_id = replica_id;
//     this->instance_no = instance_no;
//   }

//   uint64_t id() override {
//     return replica_id << 8 + instance_no;
//   }

//   bool operator==(EpaxosInstance &rhs) const {
//     return replica_id == rhs.replica_id && instance_no == rhs.instance_no;
//   }
  
// };

class EpaxosServer : public TxLogServer {
 public:
  uint64_t replica_id_;
  epoch_t curr_epoch = 0;
  uint64_t next_instance_no = 0;
  unordered_map<uint64_t, unordered_map<uint64_t, EpaxosCommand>> cmds;
  unordered_map<string, unordered_map<uint64_t, uint64_t>> dkey_deps;
  unordered_map<string, uint64_t> dkey_seq;
  shared_ptr<Marshallable> NO_OP_CMD = dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>());
  string NO_OP_DKEY = "";
  list<EpaxosRequest> reqs;
  set<pair<uint64_t, uint64_t>> committed_cmds;
  bool prepare = false;

  // uint64_t id(uint64_t replica_id, uint64_t instance_no) {
  //   return replica_id << 8 + instance_no;
  // }

  EpaxosRequest CreateEpaxosRequest(shared_ptr<Marshallable>& cmd, string dkey);
  void StartPreAccept(shared_ptr<Marshallable>& cmd, 
                      string dkey, 
                      EpaxosBallot ballot, 
                      uint64_t replica_id, 
                      uint64_t instance_no,
                      int64_t leader_dep_instance,
                      bool recovery);
  EpaxosPreAcceptReply OnPreAcceptRequest(shared_ptr<Marshallable>& cmd, 
                                          string dkey, 
                                          EpaxosBallot ballot, 
                                          uint64_t seq, 
                                          unordered_map<uint64_t, uint64_t> deps, 
                                          uint64_t replica_id, 
                                          uint64_t instance_no);
  void StartAccept(uint64_t replica_id, uint64_t instance_no);
  EpaxosAcceptReply OnAcceptRequest(shared_ptr<Marshallable>& cmd_, 
                                    string dkey, 
                                    EpaxosBallot ballot, 
                                    uint64_t seq, 
                                    unordered_map<uint64_t, uint64_t> deps, 
                                    uint64_t replica_id, 
                                    uint64_t instance_no);
  void StartCommit(uint64_t replica_id, uint64_t instance_no);
  void OnCommitRequest(shared_ptr<Marshallable>& cmd_, 
                       string dkey, 
                       EpaxosBallot ballot, 
                       uint64_t seq, 
                       unordered_map<uint64_t, uint64_t> deps, 
                       uint64_t replica_id, 
                       uint64_t instance_no);
  void StartPrepare(uint64_t replica_id, uint64_t instance_no);
  EpaxosPrepareReply OnPrepareRequest(EpaxosBallot ballot, uint64_t replica_id, uint64_t instance_no);
  bool StartExecution(uint64_t replica_id, uint64_t instance_no);

  template<class ClassT>
  void UpdateHighestSeenBallot(vector<ClassT>& replies, uint64_t replica_id, uint64_t instance_no);
  void UpdateAttributes(vector<EpaxosPreAcceptReply>& replies, uint64_t replica_id, uint64_t instance_no);
  void UpdateInternalAttributes(string dkey, uint64_t seq, unordered_map<uint64_t, uint64_t> deps);

  /* do not modify this class below here */

 public:
  EpaxosServer(Frame *frame) ;
  ~EpaxosServer() ;

  void Start(shared_ptr<Marshallable>& cmd, string dkey, uint64_t *replica_id, uint64_t *instance_no);
  void GetState(uint64_t replica_id, 
                uint64_t instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *dkey,
                uint64_t *seq, 
                unordered_map<uint64_t, uint64_t> *deps, 
                bool *committed);
  void PrepareAllUncommitted();
  
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
