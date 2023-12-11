#include "../marshallable.h"
#include "service.h"
#include "server.h"


namespace janus {

EpaxosServiceImpl::EpaxosServiceImpl(TxLogServer *sched) : svr_((EpaxosServer*)sched) {
	struct timespec curr_time;
	clock_gettime(CLOCK_MONOTONIC_RAW, &curr_time);
	srand(curr_time.tv_nsec);
}

void EpaxosServiceImpl::HandleStart(const MarshallDeputy& md_cmd,
                                    const string& dkey,
                                    rrr::DeferredReply* defer) {
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  svr_->Start(cmd, dkey);
  defer->reply();
}

void EpaxosServiceImpl::HandlePreAccept(const ballot_t& ballot,
                                        const uint64_t& replica_id,
                                        const uint64_t& instance_no,
                                        const MarshallDeputy& md_cmd,
                                        const string& dkey,
                                        const uint64_t& seq,
                                        const map<uint64_t,uint64_t>& deps,
                                        status_t* status,
                                        ballot_t* highest_seen,
                                        uint64_t* updated_seq,
                                        map_uint64_uint64_t* updated_deps,
                                        unordered_set<uint64_t>* committed_deps,
                                        rrr::DeferredReply* defer) {
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosPreAcceptReply reply = svr_->OnPreAcceptRequest(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  *status = reply.status;
  *highest_seen = reply.ballot;
  *updated_seq = reply.seq;
  *updated_deps = reply.deps;
  *committed_deps = reply.committed_deps;
  Log_debug("Return pre-accept reply for replica: %d instance: %d dep_key: %s with ballot: %d acceptor_replica_id: %d as status: %d", 
            replica_id, instance_no, dkey.c_str(), ballot, svr_->site_id_, reply.status);
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  defer->reply();
}

void EpaxosServiceImpl::HandleAccept(const ballot_t& ballot,
                                     const uint64_t& replica_id,
                                     const uint64_t& instance_no,
                                     const MarshallDeputy& md_cmd,
                                     const string& dkey,
                                     const uint64_t& seq,
                                     const map<uint64_t,uint64_t>& deps,
                                     bool_t* status,
                                     ballot_t* highest_seen,
                                     rrr::DeferredReply* defer) {
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosAcceptReply reply = svr_->OnAcceptRequest(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  *status = reply.status;
  *highest_seen = reply.ballot;
  Log_debug("Return accept reply for replica: %d instance: %d dep_key: %s with ballot: %d acceptor_replica_id: %d as status: %d", 
            replica_id, instance_no, dkey.c_str(), ballot, svr_->site_id_, reply.status);
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  defer->reply();
}

void EpaxosServiceImpl::HandleCommit(const uint64_t& replica_id,
                                     const uint64_t& instance_no,
                                     const MarshallDeputy& md_cmd,
                                     const string& dkey,
                                     const uint64_t& seq,
                                     const map_uint64_uint64_t& deps,
                                     bool_t* status,
                                     rrr::DeferredReply* defer) {
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  svr_->OnCommitRequest(cmd, dkey, seq, deps, replica_id, instance_no);
  *status = true;
  Log_debug("Return commit reply for replica: %d instance: %d dep_key: %s acceptor_replica_id: %d", 
            replica_id, instance_no, dkey.c_str(), svr_->site_id_);
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  defer->reply();
}

void EpaxosServiceImpl::HandleTryPreAccept(const ballot_t& ballot,
                                           const uint64_t& replica_id,
                                           const uint64_t& instance_no,
                                           const MarshallDeputy& md_cmd,
                                           const string& dkey,
                                           const uint64_t& seq,
                                           const map<uint64_t,uint64_t>& deps,
                                           status_t* status,
                                           ballot_t* highest_seen,
                                           uint64_t* conflict_replica_id,
                                           uint64_t* conflict_instance_no,
                                           rrr::DeferredReply* defer) {
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  shared_ptr<Marshallable> cmd = const_cast<MarshallDeputy&>(md_cmd).sp_data_;
  EpaxosTryPreAcceptReply reply = svr_->OnTryPreAcceptRequest(cmd, dkey, ballot, seq, deps, replica_id, instance_no);
  *status = reply.status;
  *highest_seen = reply.ballot;
  *conflict_replica_id = reply.conflict_replica_id;
  *conflict_instance_no = reply.conflict_instance_no;
  Log_debug("Return try-pre-accept reply for replica: %d instance: %d dep_key: %s with ballot: %d as status: %d", 
            replica_id, instance_no, dkey.c_str(), reply.ballot, reply.status);
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  defer->reply();
}

void EpaxosServiceImpl::HandlePrepare(const ballot_t& ballot,
                                      const uint64_t& replica_id,
                                      const uint64_t& instance_no,
                                      bool_t* status,
                                      MarshallDeputy* md_cmd,
                                      string* dkey,
                                      uint64_t* seq,
                                      map<uint64_t,uint64_t>* deps,
                                      status_t* cmd_state,
                                      uint64_t* acceptor_replica_id,
                                      ballot_t* highest_seen,
                                      rrr::DeferredReply* defer) {
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
  EpaxosPrepareReply reply = svr_->OnPrepareRequest(ballot, replica_id, instance_no);
  *status = reply.status;
  *md_cmd = MarshallDeputy(reply.cmd);
  *dkey = reply.dkey;
  *seq = reply.seq;
  *deps = reply.deps;
  *cmd_state = reply.cmd_state;
  *acceptor_replica_id = reply.acceptor_replica_id;
  *highest_seen = reply.ballot;
  Log_debug("Return prepare reply for replica: %d instance: %d dep_key: %s with acceptor_replica_id: %d ballot: %d as status: %d cmd_state: %d", 
            replica_id, instance_no, reply.dkey.c_str(), reply.acceptor_replica_id, reply.ballot, reply.status, reply.cmd_state);
  defer->reply();
  #ifdef WIDE_AREA
  Coroutine::Sleep(WIDE_AREA_DELAY);
  #endif
}

} // namespace janus;
