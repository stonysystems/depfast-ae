#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"

namespace janus {

class TxLogServer;
class CopilotServer;

class CopilotServiceImpl : public CopilotService {
  CopilotServer* sched_;
 public:
  CopilotServiceImpl(TxLogServer *sched);

  void Forward(const MarshallDeputy& cmd,
               rrr::DeferredReply* defer) override;

  void Prepare(const uint8_t& is_pilot,
               const uint64_t& slot,
               const ballot_t& ballot,
               MarshallDeputy* ret_cmd,
               ballot_t* max_ballot,
               uint64_t* dep,
               status_t* status,
               rrr::DeferredReply* defer) override;

  void FastAccept(const uint8_t& is_pilot,
                  const uint64_t& slot,
                  const ballot_t& ballot,
                  const uint64_t& dep,
                  const MarshallDeputy& cmd,
                  ballot_t* max_ballot,
                  uint64_t* ret_dep,
                  rrr::DeferredReply* defer) override;

  void Accept(const uint8_t& is_pilot,
              const uint64_t& slot,
              const ballot_t& ballot,
              const uint64_t& dep,
              const MarshallDeputy& cmd,
              ballot_t* max_ballot,
              rrr::DeferredReply* defer) override;

  void Commit(const uint8_t& is_pilot,
              const uint64_t& slot,
              const uint64_t& dep,
              const MarshallDeputy& cmd,
              rrr::DeferredReply* defer) override;
};

} // namespace janus