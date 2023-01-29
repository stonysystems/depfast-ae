#pragma once

#include "../__dep__.h"
#include "../communicator.h"


namespace janus {

#define NSERVERS 5

enum EpaxosPreAcceptStatus {
  FAILED = 0,
  IDENTICAL = 1,
  NON_IDENTICAL = 2
};

class EpaxosPreAcceptReply {
 public:
  EpaxosPreAcceptStatus status;
  epoch_t epoch;
  ballot_t ballot_no;
  uint64_t replica_id;
  uint64_t seq;
  unordered_map<uint64_t, uint64_t> deps;

  EpaxosPreAcceptReply(EpaxosPreAcceptStatus status, epoch_t epoch, ballot_t ballot_no, uint64_t replica_id) {
    this->status = status;
    this->epoch = epoch;
    this->ballot_no = ballot_no;
    this->replica_id = replica_id;
  }

  EpaxosPreAcceptReply(EpaxosPreAcceptStatus status, epoch_t epoch, ballot_t ballot_no, uint64_t replica_id, uint64_t seq, unordered_map<uint64_t, uint64_t>& deps) {
    this->status = status;
    this->epoch = epoch;
    this->ballot_no = ballot_no;
    this->replica_id = replica_id;
    this->seq = seq;
    this->deps = deps;
  }
};

class EpaxosPreAcceptQuorumEvent : public QuorumEvent {
 private:
  int n_voted_identical_ = 0;
  int n_voted_nonidentical_ = 0;
  int fast_path_quorum_;
  int slow_path_quorum_;
  bool is_recovery;
 public:
  vector<EpaxosPreAcceptReply> replies;

  EpaxosPreAcceptQuorumEvent(int n_total_, bool is_recovery, int fast_path_quorum, int slow_path_quorum) : QuorumEvent(n_total_, n_total_) {
    this->is_recovery = is_recovery;
    this->fast_path_quorum_ = fast_path_quorum;
    this->slow_path_quorum_ = slow_path_quorum;
  }

  void VoteIdentical(EpaxosPreAcceptReply& reply) {
    n_voted_identical_++;
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNonIdentical(EpaxosPreAcceptReply& reply) {
    n_voted_nonidentical_++;
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNo(EpaxosPreAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }

  bool FastPath() {
    if (!is_recovery && n_voted_identical_ >= fast_path_quorum_) {
      return true;
    }
    return false;
  }

  bool SlowPath() {
    if (is_recovery && n_voted_yes_ >= slow_path_quorum_) {
      return true;
    }
    if ((n_voted_yes_ >= slow_path_quorum_) && ((n_voted_nonidentical_ + n_voted_no_) > (n_total_-fast_path_quorum_))) {
      return true;
    }
    return false;
  }

  bool Yes() override {
    return FastPath() || SlowPath();
  }

  bool No() override {
    if (n_voted_no_ > (n_total_-slow_path_quorum_)) {
      return true;
    }
    return false;
  }
};

class EpaxosAcceptReply {
 public:
  bool_t status;
  epoch_t epoch;
  ballot_t ballot_no;
  uint64_t replica_id;

  EpaxosAcceptReply(bool_t status, epoch_t epoch, ballot_t ballot_no, uint64_t replica_id) {
    this->status = status;
    this->epoch = epoch;
    this->ballot_no = ballot_no;
    this->replica_id = replica_id;
  }
};

class EpaxosAcceptQuorumEvent : public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  vector<EpaxosAcceptReply> replies;

  void VoteYes(EpaxosAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNo(EpaxosAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }
};

class EpaxosPrepareReply {
 public:
  bool_t status;
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  unordered_map<uint64_t, uint64_t> deps;
  status_t cmd_state;
  uint64_t acceptor_replica_id;
  epoch_t epoch;
  ballot_t ballot_no;
  uint64_t replica_id;

  EpaxosPrepareReply() {
    status = false;
    cmd_state = 0;
    epoch = 0;
    ballot_no = -1;
    replica_id = 0;
  }

  EpaxosPrepareReply(bool_t status,
                     shared_ptr<Marshallable> cmd,
                     string dkey,
                     uint64_t seq,
                     unordered_map<uint64_t, uint64_t> deps,
                     status_t cmd_state,
                     uint64_t acceptor_replica_id,
                     epoch_t epoch,
                     ballot_t ballot_no,
                     uint64_t replica_id) {
    this->status = status;
    this->cmd = cmd;
    this->dkey = dkey;
    this->seq = seq;
    this->deps = deps;
    this->cmd_state = cmd_state;
    this->acceptor_replica_id = acceptor_replica_id;
    this->epoch = epoch;
    this->ballot_no = ballot_no;
    this->replica_id = replica_id;
  }
};

class EpaxosPrepareQuorumEvent : public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  vector<EpaxosPrepareReply> replies;

  void VoteYes(EpaxosPrepareReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNo(EpaxosPrepareReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }
};

class TxData;
class EpaxosCommo : public Communicator {
 public:
  EpaxosCommo() = delete;
  EpaxosCommo(PollMgr*);

  shared_ptr<EpaxosPreAcceptQuorumEvent> 
  SendPreAccept(const siteid_t& site_id,
                const parid_t& par_id,
                const bool& is_recovery,
                const epoch_t& epoch,
                const ballot_t& ballot_no,
                const uint64_t& ballot_replica_id,
                const uint64_t& leader_replica_id,
                const uint64_t& instance_no,
                const shared_ptr<Marshallable>& cmd,
                const string& dkey,
                const uint64_t& seq,
                const unordered_map<uint64_t, uint64_t>& deps);

  
  shared_ptr<EpaxosAcceptQuorumEvent>
  SendAccept(const siteid_t& site_id,
             const parid_t& par_id,
             const epoch_t& epoch,
             const ballot_t& ballot_no,
             const uint64_t& ballot_replica_id,
             const uint64_t& leader_replica_id,
             const uint64_t& instance_no,
             const shared_ptr<Marshallable>& cmd,
             const string& dkey,
             const uint64_t& seq,
             const unordered_map<uint64_t, uint64_t>& deps);

  shared_ptr<QuorumEvent>
  SendCommit(const siteid_t& site_id,
             const parid_t& par_id,
             const epoch_t& epoch,
             const ballot_t& ballot_no,
             const uint64_t& ballot_replica_id,
             const uint64_t& leader_replica_id,
             const uint64_t& instance_no,
             const shared_ptr<Marshallable>& cmd,
             const string& dkey,
             const uint64_t& seq,
             const unordered_map<uint64_t, uint64_t>& deps);
         
  shared_ptr<EpaxosPrepareQuorumEvent>
  SendPrepare(const siteid_t& site_id,
              const parid_t& par_id,
              const epoch_t& epoch,
              const ballot_t& ballot_no,
              const uint64_t& ballot_replica_id,
              const uint64_t& leader_replica_id,
              const uint64_t& instance_no);
                     
  /* Do not modify this class below here */

  friend class FpgaEpaxosProxy;
 public:
  #ifdef EPAXOS_TEST_CORO
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
  #endif
};

} // namespace janus

