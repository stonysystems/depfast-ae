#include "../marshallable.h"
#include "service.h"
#include "server.h"


namespace janus {

EpaxosServiceImpl::EpaxosServiceImpl(TxLogServer *sched) : svr_((EpaxosServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}

void EpaxosServiceImpl::HandlePreAccept(const epoch_t& epoch,
                                        const ballot_t& ballot_no,
                                        const uint64_t& ballot_replica_id,
                                        const uint64_t& leader_replica_id,
                                        const uint64_t& instance_no,
                                        const MarshallDeputy& md_cmd,
                                        const string& dkey,
                                        const uint64_t& seq,
                                        const unordered_map<uint64_t,uint64_t>& deps,
                                        status_t* status,
                                        epoch_t* highest_seen_epoch,
                                        ballot_t* highest_seen_ballot_no,
                                        uint64_t* highest_seen_replica_id,
                                        uint64_t* updated_seq,
                                        unordered_map<uint64_t,uint64_t>* updated_deps,
                                        rrr::DeferredReply* defer) {
  EpaxosBallot ballot(epoch, ballot_no, ballot_replica_id);
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosPreAcceptReply reply = svr_->OnPreAcceptRequest(cmd, dkey, ballot, seq, deps, leader_replica_id, instance_no);
  *status = reply.status;
  *highest_seen_epoch = reply.epoch;
  *highest_seen_ballot_no = reply.ballot_no;
  *highest_seen_replica_id = reply.replica_id;
  *updated_seq = reply.seq;
  *updated_deps = reply.deps;
  Log_debug("Return pre-accept reply for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d as status: %d", 
            leader_replica_id, instance_no, dkey.c_str(), ballot.ballot_no, ballot.replica_id, reply.status);
  defer->reply();
}

void EpaxosServiceImpl::HandleAccept(const epoch_t& epoch,
                                     const ballot_t& ballot_no,
                                     const uint64_t& ballot_replica_id,
                                     const uint64_t& leader_replica_id,
                                     const uint64_t& instance_no,
                                     const MarshallDeputy& md_cmd,
                                     const string& dkey,
                                     const uint64_t& seq,
                                     const unordered_map<uint64_t,uint64_t>& deps,
                                     bool_t* status,
                                     epoch_t* highest_seen_epoch,
                                     ballot_t* highest_seen_ballot_no,
                                     uint64_t* highest_seen_replica_id,
                                     rrr::DeferredReply* defer) {
  EpaxosBallot ballot(epoch, ballot_no, ballot_replica_id);
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosAcceptReply reply = svr_->OnAcceptRequest(cmd, dkey, ballot, seq, deps, leader_replica_id, instance_no);
  *status = reply.status;
  *highest_seen_epoch = reply.epoch;
  *highest_seen_ballot_no = reply.ballot_no;
  *highest_seen_replica_id = reply.replica_id;
  Log_debug("Return accept reply for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d as status: %d", 
            leader_replica_id, instance_no, dkey.c_str(), ballot.ballot_no, ballot.replica_id, reply.status);
  defer->reply();
}

void EpaxosServiceImpl::HandleCommit(const epoch_t& epoch,
                                     const ballot_t& ballot_no,
                                     const uint64_t& ballot_replica_id,
                                     const uint64_t& leader_replica_id,
                                     const uint64_t& instance_no,
                                     const MarshallDeputy& md_cmd,
                                     const string& dkey,
                                     const uint64_t& seq,
                                     const unordered_map_uint64_uint64_t& deps,
                                     bool_t* status,
                                     rrr::DeferredReply* defer) {
  EpaxosBallot ballot(epoch, ballot_no, ballot_replica_id);
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  svr_->OnCommitRequest(cmd, dkey, ballot, seq, deps, leader_replica_id, instance_no);
  *status = true;
  Log_debug("Return commit reply for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d", 
            leader_replica_id, instance_no, dkey.c_str(), ballot.ballot_no, ballot.replica_id);
  defer->reply();
}

void EpaxosServiceImpl::HandleTryPreAccept(const epoch_t& epoch,
                                           const ballot_t& ballot_no,
                                           const uint64_t& ballot_replica_id,
                                           const uint64_t& leader_replica_id,
                                           const uint64_t& instance_no,
                                           const MarshallDeputy& md_cmd,
                                           const string& dkey,
                                           const uint64_t& seq,
                                           const unordered_map<uint64_t,uint64_t>& deps,
                                           bool_t* status,
                                           epoch_t* highest_seen_epoch,
                                           ballot_t* highest_seen_ballot_no,
                                           uint64_t* highest_seen_replica_id,
                                           rrr::DeferredReply* defer) {
  EpaxosBallot ballot(epoch, ballot_no, ballot_replica_id);
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosTryPreAcceptReply reply = svr_->OnTryPreAcceptRequest(cmd, dkey, ballot, seq, deps, leader_replica_id, instance_no);
  *status = reply.status;
  *highest_seen_epoch = reply.epoch;
  *highest_seen_ballot_no = reply.ballot_no;
  *highest_seen_replica_id = reply.replica_id;
  Log_debug("Return try-pre-accept reply for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d as status: %d", 
            leader_replica_id, instance_no, dkey.c_str(), reply.ballot_no, ballot.replica_id, reply.status);
  defer->reply();
}

void EpaxosServiceImpl::HandlePrepare(const epoch_t& epoch,
                                      const ballot_t& ballot_no,
                                      const uint64_t& ballot_replica_id,
                                      const uint64_t& leader_replica_id,
                                      const uint64_t& instance_no,
                                      bool_t* status,
                                      MarshallDeputy* md_cmd,
                                      string* dkey,
                                      uint64_t* seq,
                                      unordered_map<uint64_t,uint64_t>* deps,
                                      status_t* cmd_state,
                                      uint64_t* acceptor_replica_id,
                                      epoch_t* highest_seen_epoch,
                                      ballot_t* highest_seen_ballot_no,
                                      uint64_t* highest_seen_replica_id,
                                      rrr::DeferredReply* defer) {
  EpaxosBallot ballot(epoch, ballot_no, ballot_replica_id);
  EpaxosPrepareReply reply = svr_->OnPrepareRequest(ballot, leader_replica_id, instance_no);
  *status = reply.status;
  *md_cmd = MarshallDeputy(reply.cmd);
  *dkey = reply.dkey;
  *seq = reply.seq;
  *deps = reply.deps;
  *cmd_state = reply.cmd_state;
  *acceptor_replica_id = reply.acceptor_replica_id;
  *highest_seen_epoch = reply.epoch;
  *highest_seen_ballot_no = reply.ballot_no;
  *highest_seen_replica_id = reply.replica_id;
  Log_debug("Return prepare reply for replica: %d instance: %d dep_key: %s with ballot: %d leader: %d as status: %d cmd_state: %d acceptor_replica_id: %d", 
            leader_replica_id, instance_no, reply.dkey.c_str(), reply.ballot_no, ballot.replica_id, reply.status, reply.cmd_state, reply.acceptor_replica_id);
  defer->reply();
}

} // namespace janus;
