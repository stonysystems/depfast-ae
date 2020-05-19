#pragma once

#include "__dep__.h"
#include "constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "deptran/procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"
#include <chrono>

class SimpleCommand;
namespace janus {

class TxLogServer;
class PaxosServer;
class MultiPaxosServiceImpl : public MultiPaxosService {
 public:
  PaxosServer* sched_;
  MultiPaxosServiceImpl(TxLogServer* sched);
  void Forward(const MarshallDeputy& cmd,
               const uint64_t& dep_id,
               uint64_t* coro_id,
               rrr::DeferredReply* defer) override;

  void Prepare(const uint64_t& slot,
               const ballot_t& ballot,
               ballot_t* max_ballot,
               uint64_t* coro_id,
               rrr::DeferredReply* defer) override;

  void Accept(const uint64_t& slot,
	      const uint64_t& time,
              const ballot_t& ballot,
              const MarshallDeputy& cmd,
              ballot_t* max_ballot,
              uint64_t* coro_id,
              rrr::DeferredReply* defer) override;

  void Decide(const uint64_t& slot,
              const ballot_t& ballot,
              const MarshallDeputy& cmd,
              rrr::DeferredReply* defer) override;

  void BulkDecide(const MarshallDeputy& cmd,
                  i32* valid,
                  rrr::DeferredReply* defer) override;

  void BulkAccept(const MarshallDeputy& cmd,
                  i32* valid,
                  rrr::DeferredReply* defer) override;

};

} // namespace janus
