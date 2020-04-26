//
// Created by shuai on 11/25/15.
//
#pragma once

#include "../coordinator.h"

namespace janus {
class ClientControlServiceImpl;

class CoordinatorNoTx : public Coordinator {
 public:
  CoordinatorNoTx(uint32_t coo_id,
                     int benchmark,
                     ClientControlServiceImpl* ccsi,
                     uint32_t thread_id);

  virtual ~CoordinatorNoTx() {}

  inline TxData& tx_data() {
    return *(TxData*) cmd_;
  }

  Communicator* commo();


  /** do it asynchronously, thread safe. */
  virtual void DoTxAsync(TxRequest&) override;
  virtual void SetNewLeader(parid_t par_id, volatile locid_t* cur_pause) override ;
  virtual void Reset() override;
  void Restart() override  { verify(0);};

  virtual void DispatchAsync() { verify(0);};
  virtual void DispatchAck(phase_t phase,
                           int res,
                           map<innid_t, map<int32_t, Value>>& outputs) { verify(0);};
  void Prepare();
  void PrepareAck(phase_t phase, int res);
  virtual void Commit() { verify(0);};
  void End() ;

  virtual void GotoNextPhase() { verify(0);};

  void Report(TxReply& txn_reply, double last_latency) ;

};
} // namespace janus
