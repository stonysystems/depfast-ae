#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
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

  int isGreater(EpaxosBallot& ballot) {
    if (epoch > ballot.epoch || (epoch == ballot.epoch && ballot_no > ballot.ballot_no)) {
      return true;
    }
    return false;
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

  EpaxosRequest(shared_ptr<Marshallable>& cmd, string dkey) {
    this->cmd = cmd;
    this->dkey = dkey;
  }
};

// class EpaxosInstance: public Vertex<EpaxosInstance> {
//  public:
//   uint64_t replica_id;
//   uint64_t instance_no;
//   EpaxosCommand cmd;

//   EpaxosInstance(uint64_t replica_id, uint64_t instance_no, EpaxosCommand &cmd) {
//     this->replica_id = replica_id;
//     this->instance_no = instance_no;
//     this->cmd = cmd;
//   }

//   uint64_t id() override {
//     return cmd.seq;
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
  shared_ptr<Marshallable> NO_OP_CMD = make_shared<Marshallable>(10);
  int NO_OP_KIND = 10;
  string NO_OP_DKEY = "";
  list<EpaxosRequest> reqs;

  void StartPreAccept(shared_ptr<Marshallable>& cmd, string dkey);
  void StartPreAccept(shared_ptr<Marshallable>& cmd, 
                      string dkey, 
                      EpaxosBallot ballot, 
                      uint64_t replica_id, 
                      uint64_t instance_no,
                      uint64_t leader_dep_instance,
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

  void Request(shared_ptr<Marshallable>& cmd, string dkey);

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
