#pragma once

#include "__dep__.h"
#include "epaxos_rpc.h"
#include "server.h"
#include "macros.h"


namespace janus {

class TxLogServer;
class EpaxosServer;
class EpaxosServiceImpl : public EpaxosService {
 public:
  EpaxosServer* svr_;
  EpaxosServiceImpl(TxLogServer* sched);

  RpcHandler(Start, 2,
             const MarshallDeputy&, md_cmd,
             const string&, dkey) {}

  RpcHandler(PreAccept, 12,
             const ballot_t&, ballot,
             const uint64_t&, replica_id,
             const uint64_t&, instance_no,
             const MarshallDeputy&, md_cmd,
             const string&, dkey,
             const uint64_t&, seq,
             const map_uint64_uint64_t&, deps,
             status_t*, status,
             ballot_t*, highest_seen,
             uint64_t*, updated_seq,
             map_uint64_uint64_t*, updated_deps,
             unordered_set<uint64_t>*, committed_deps) {
    *status = EpaxosPreAcceptStatus::FAILED;
    *highest_seen = -1;
    *updated_seq = 0;
    *updated_deps = map<uint64_t, uint64_t>();
    *committed_deps = unordered_set<uint64_t>();
  }

  RpcHandler(Accept, 9,
             const ballot_t&, ballot,
             const uint64_t&, replica_id,
             const uint64_t&, instance_no,
             const MarshallDeputy&, md_cmd,
             const string&, dkey,
             const uint64_t&, seq,
             const map_uint64_uint64_t&, deps,
             bool_t*, status,
             ballot_t*, highest_seen) {
    *status = false;
    *highest_seen = -1;
  }

  RpcHandler(Commit, 7,
             const uint64_t&, replica_id,
             const uint64_t&, instance_no,
             const MarshallDeputy&, md_cmd,
             const string&, dkey,
             const uint64_t&, seq,
             const map_uint64_uint64_t&, deps,
             bool_t*, status) {
    *status = false;
  }

  RpcHandler(TryPreAccept, 11,
             const ballot_t&, ballot,
             const uint64_t&, replica_id,
             const uint64_t&, instance_no,
             const MarshallDeputy&, md_cmd,
             const string&, dkey,
             const uint64_t&, seq,
             const map_uint64_uint64_t&, deps,
             status_t*, status,
             ballot_t*, highest_seen,
             uint64_t*, conflict_replica_id,
             uint64_t*, conflict_instance_no) {
    *status = EpaxosTryPreAcceptStatus::REJECTED;
    *highest_seen = -1;
    *conflict_replica_id = 0;
    *conflict_instance_no = 0;
  }

  RpcHandler(Prepare, 11,
             const ballot_t&, ballot,
             const uint64_t&, replica_id,
             const uint64_t&, instance_no,
             bool_t*, status,
             MarshallDeputy*, md_cmd,
             string*, dkey,
             uint64_t*, seq,
             map_uint64_uint64_t*, deps,
             status_t*, cmd_state,
             uint64_t*, acceptor_replica_id,
             ballot_t*, highest_seen) {
    *status = false;
    *md_cmd = MarshallDeputy(dynamic_pointer_cast<Marshallable>(make_shared<TpcNoopCommand>()));
    *dkey = "";
    *seq = 0;
    *deps = map<uint64_t, uint64_t>();
    *cmd_state = EpaxosCommandState::NOT_STARTED;
    *acceptor_replica_id = 0;
    *highest_seen = -1;
  }

  RpcHandler(CollectMetrics, 3,
             uint64_t*, fast_path_count,
             vector<double>*, commit_times,
             vector<double>*, exec_times) {
    *fast_path_count = 0;
    *commit_times = vector<double>();
    *exec_times = vector<double>();
  }
  
};

} // namespace janus
