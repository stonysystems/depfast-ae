#pragma once

#include "../classic/scheduler.h"
#include "../classic/tx.h"

namespace janus {

class SchedulerNone: public SchedulerClassic {
 using SchedulerClassic::SchedulerClassic;
 public:

  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output) override {
    SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
    ExecutePiece(tx, cmd, ret_output);
    return true;
  }

  virtual bool Guard(Tx &tx_box, Row* row, int col_id, bool write=true)
      override {
    // do nothing.
    return true;
  }

  virtual bool DoPrepare(txnid_t tx_id) override {
    return false;
  }

  virtual bool Dispatch(cmdid_t cmd_id,
                        shared_ptr<Marshallable> cmd,
                        TxnOutput& ret_output) override;

};

} // janus
