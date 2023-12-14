#pragma once

#include "../__dep__.h"
#include "../communicator.h"
#include "epaxos_rpc.h"


namespace janus {

#ifdef EPAXOS_TEST_CORO
#define NSERVERS 7
#else
#define NSERVERS 3
#endif
#define FAST_PATH_QUORUM ((NSERVERS/2)+(((NSERVERS/2)+1)/2))
#define SLOW_PATH_QUORUM ((NSERVERS/2) + 1)
#define WIDE_AREA_DELAY 40000 + (rand() % 10000)

enum EpaxosPreAcceptStatus {
  NOT_INITIALIZED = 0,
  FAILED = 1,
  IDENTICAL = 2,
  NON_IDENTICAL = 3
};

class EpaxosPreAcceptReply {
 public:
  EpaxosPreAcceptStatus status;
  ballot_t ballot;
  uint64_t seq;
  map<uint64_t, uint64_t> deps;
  unordered_set<uint64_t> committed_deps;

  EpaxosPreAcceptReply() {
    status = EpaxosPreAcceptStatus::NOT_INITIALIZED;
  }

  EpaxosPreAcceptReply(EpaxosPreAcceptStatus status, ballot_t ballot) {
    this->status = status;
    this->ballot = ballot;
  }

  EpaxosPreAcceptReply(EpaxosPreAcceptStatus status, ballot_t ballot, uint64_t seq, map<uint64_t, uint64_t>& deps, unordered_set<uint64_t>& committed_deps) {
    this->status = status;
    this->ballot = ballot;
    this->seq = seq;
    this->deps = deps;
    this->committed_deps = committed_deps;
  }
};

class EpaxosPreAcceptQuorumEvent : public QuorumEvent {
 private:
  int n_voted_identical_ = 0;
  int n_voted_nonidentical_ = 0;
  int fast_path_quorum_;
  int slow_path_quorum_;
  bool is_recovery;
  bool thrifty;
  bool all_equal;
 public:
  EpaxosPreAcceptReply eq_reply;
  vector<EpaxosPreAcceptReply> replies;

  EpaxosPreAcceptQuorumEvent(bool thrifty, bool is_recovery) : QuorumEvent(thrifty ? FAST_PATH_QUORUM - 1 : NSERVERS - 1, thrifty ? FAST_PATH_QUORUM - 1 : NSERVERS - 1) {
    this->is_recovery = is_recovery;
    this->fast_path_quorum_ = FAST_PATH_QUORUM - 1;
    this->slow_path_quorum_ = SLOW_PATH_QUORUM - 1;
    this->thrifty = thrifty;
    this->all_equal = true;
  }

  void VoteIdentical(EpaxosPreAcceptReply& reply) {
    n_voted_identical_++;
    if (eq_reply.status == EpaxosPreAcceptStatus::NOT_INITIALIZED) {
      eq_reply = reply;
    } else if (thrifty && (reply.seq != eq_reply.seq || reply.deps != eq_reply.deps)) {
      all_equal = false;
    }
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNonIdentical(EpaxosPreAcceptReply& reply) {
    n_voted_nonidentical_++;
    if (thrifty) {
      if (eq_reply.status == EpaxosPreAcceptStatus::NOT_INITIALIZED) {
        eq_reply = reply;
      } else if (reply.seq != eq_reply.seq || reply.deps != eq_reply.deps) {
        all_equal = false;
      }
    }
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNo(EpaxosPreAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }

  bool FastPath() {
    return !is_recovery 
           && ((!thrifty && n_voted_identical_ >= fast_path_quorum_)
               || (thrifty && all_equal && (n_voted_nonidentical_ + n_voted_identical_ >= fast_path_quorum_)));
  }

  bool SlowPath() {
    return (n_voted_yes_ >= slow_path_quorum_) 
            && (is_recovery
                || (!thrifty && (n_voted_nonidentical_ + n_voted_no_) > (n_total_ - fast_path_quorum_))
                || (thrifty && (!all_equal || n_voted_no_ > 0)));
  }

  bool Yes() override {
    return FastPath() || SlowPath();
  }

  bool No() override {
    return n_voted_no_ > (n_total_ - slow_path_quorum_);
  }
};

class EpaxosAcceptReply {
 public:
  bool_t status;
  ballot_t ballot;

  EpaxosAcceptReply(bool_t status, ballot_t ballot) {
    this->status = status;
    this->ballot = ballot;
  }
};

class EpaxosAcceptQuorumEvent : public QuorumEvent {
 public:
  vector<EpaxosAcceptReply> replies;

  EpaxosAcceptQuorumEvent(bool thrifty) : QuorumEvent(thrifty ? FAST_PATH_QUORUM - 1 : NSERVERS - 1, SLOW_PATH_QUORUM - 1) {}

  void VoteYes(EpaxosAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }

  void VoteNo(EpaxosAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }
};

enum EpaxosTryPreAcceptStatus {
  REJECTED = 0,
  NO_CONFLICT = 1,
  COMMITTED_CONFLICT = 2,
  UNCOMMITTED_CONFLICT = 3,
  MOVED_ON = 4
};

class EpaxosTryPreAcceptReply {
 public:
  EpaxosTryPreAcceptStatus status;
  ballot_t ballot;
  uint64_t conflict_replica_id;
  uint64_t conflict_instance_no;

  EpaxosTryPreAcceptReply(EpaxosTryPreAcceptStatus status,
                          ballot_t ballot,
                          uint64_t conflict_replica_id, 
                          uint64_t conflict_instance_no) {
    this->status = status;
    this->ballot = ballot;
    this->conflict_replica_id = conflict_replica_id;
    this->conflict_instance_no = conflict_instance_no;
  }
};

class EpaxosTryPreAcceptQuorumEvent : public QuorumEvent {
 private:
  int n_voted_noconflict_ = 0;
  int n_voted_conflict_ = 0;
  bool voted_committed_conflict_ = false;
  bool voted_moved_on_ = false;
 public:
  vector<EpaxosTryPreAcceptReply> replies;

  EpaxosTryPreAcceptQuorumEvent(int preaccepted_site_count) : QuorumEvent(NSERVERS - preaccepted_site_count, SLOW_PATH_QUORUM - preaccepted_site_count) {}
 
  void VoteYes(EpaxosTryPreAcceptReply& reply) {
    switch(reply.status) {
      case EpaxosTryPreAcceptStatus::NO_CONFLICT:
        n_voted_noconflict_++;
        break;
      case EpaxosTryPreAcceptStatus::COMMITTED_CONFLICT:
        voted_committed_conflict_ = true;
        n_voted_conflict_++;
        break;
      case EpaxosTryPreAcceptStatus::UNCOMMITTED_CONFLICT:
        n_voted_conflict_++;
        break;
      case EpaxosTryPreAcceptStatus::MOVED_ON:
        voted_moved_on_ = true;
        n_voted_conflict_++;
        break;
    }
    replies.push_back(reply);
    this->QuorumEvent::VoteYes();
  }
  
  void VoteNo(EpaxosTryPreAcceptReply& reply) {
    replies.push_back(reply);
    this->QuorumEvent::VoteNo();
  }
  
  bool NoConflict() {
    return n_voted_noconflict_ >= quorum_;
  }

  bool MovedOn() {
    return voted_moved_on_;
  }

  bool CommittedConflict() {
    return voted_committed_conflict_;
  }
  
  bool Conflict() {
    return (n_voted_no_ + n_voted_conflict_) > (n_total_ - quorum_);
  }

  bool Yes() override {
    return NoConflict() || Conflict() || CommittedConflict() || MovedOn();
  }

  bool No() override {
    return n_voted_no_ > (n_total_-quorum_);
  }
};

class EpaxosPrepareReply {
 public:
  bool_t status;
  shared_ptr<Marshallable> cmd;
  string dkey;
  uint64_t seq;
  map<uint64_t, uint64_t> deps;
  status_t cmd_state;
  uint64_t acceptor_replica_id;
  ballot_t ballot;

  EpaxosPrepareReply() {
    status = false;
    cmd_state = 0;
    ballot = -1;
  }

  EpaxosPrepareReply(bool_t status,
                     shared_ptr<Marshallable> cmd,
                     string dkey,
                     uint64_t seq,
                     map<uint64_t, uint64_t> deps,
                     status_t cmd_state,
                     uint64_t acceptor_replica_id,
                     ballot_t ballot) {
    this->status = status;
    this->cmd = cmd;
    this->dkey = dkey;
    this->seq = seq;
    this->deps = deps;
    this->cmd_state = cmd_state;
    this->acceptor_replica_id = acceptor_replica_id;
    this->ballot = ballot;
  }
};

class EpaxosPrepareQuorumEvent : public QuorumEvent {
 public:
  vector<EpaxosPrepareReply> replies;
  map<uint64_t, siteid_t> replicaid_siteid_map;

  EpaxosPrepareQuorumEvent(bool thrifty) : QuorumEvent(thrifty ? FAST_PATH_QUORUM - 1 : NSERVERS - 1, SLOW_PATH_QUORUM - 1) {}

  void VoteYes(siteid_t site_id, EpaxosPrepareReply& reply) {
    replies.push_back(reply);
    replicaid_siteid_map[reply.acceptor_replica_id] = site_id;
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

  #ifdef THRIFTY
  bool thrifty = true;
  #else
  bool thrifty = false;
  #endif
  
  void 
  SendStart(const siteid_t& site_id,
            const parid_t& par_id, 
            const shared_ptr<Marshallable>& cmd, 
            const string& dkey,
            const function<void(void)>& callback);

  shared_ptr<EpaxosPreAcceptQuorumEvent> 
  SendPreAccept(const siteid_t& site_id,
                const parid_t& par_id,
                const bool& is_recovery,
                const ballot_t& ballot,
                const uint64_t& replica_id,
                const uint64_t& instance_no,
                const shared_ptr<Marshallable>& cmd,
                const string& dkey,
                const uint64_t& seq,
                const map<uint64_t, uint64_t>& deps);

  
  shared_ptr<EpaxosAcceptQuorumEvent>
  SendAccept(const siteid_t& site_id,
             const parid_t& par_id,
             const ballot_t& ballot,
             const uint64_t& replica_id,
             const uint64_t& instance_no,
             const shared_ptr<Marshallable>& cmd,
             const string& dkey,
             const uint64_t& seq,
             const map<uint64_t, uint64_t>& deps);

  void
  SendCommit(const siteid_t& site_id,
             const parid_t& par_id,
             const uint64_t& replica_id,
             const uint64_t& instance_no,
             const shared_ptr<Marshallable>& cmd,
             const string& dkey,
             const uint64_t& seq,
             const map<uint64_t, uint64_t>& deps);
  
  shared_ptr<EpaxosTryPreAcceptQuorumEvent> 
  SendTryPreAccept(const siteid_t& site_id,
                   const parid_t& par_id,
                   const unordered_set<siteid_t>& preaccepted_sites,
                   const ballot_t& ballot,
                   const uint64_t& replica_id,
                   const uint64_t& instance_no,
                   const shared_ptr<Marshallable>& cmd,
                   const string& dkey,
                   const uint64_t& seq,
                   const map<uint64_t, uint64_t>& deps);

  shared_ptr<EpaxosPrepareQuorumEvent>
  SendPrepare(const siteid_t& site_id,
              const parid_t& par_id,
              const ballot_t& ballot,
              const uint64_t& replica_id,
              const uint64_t& instance_no);
  
shared_ptr<IntEvent>
CollectMetrics(const siteid_t& site_id,
               const parid_t& par_id, 
               uint64_t *fast_path_count,
               vector<double> *commit_times,
               vector<double> *exec_times);
                                  
  /* Do not modify this class below here */

 public:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  std::recursive_mutex rpc_mtx_ = {};
  uint64_t rpc_count_ = 0;
  #endif
};

} // namespace janus
