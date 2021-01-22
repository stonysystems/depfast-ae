#pragma once

#include <unordered_map>
#include <unordered_set>

#include "../classic/scheduler.h"

namespace janus {

class SimpleCommand;
class SchedulerCarousel : public SchedulerClassic {
 public:
/*//  using Scheduler::Scheduler;
  SchedulerCarousel() : SchedulerClassic() {
    epoch_enabled_ = true;
  }*/
  SchedulerCarousel();

  virtual bool Guard(Tx &tx_box, Row *row, int col_id, bool write) override;

  int OnDecide(txid_t tx_id,
               int32_t decision,
               const function<void()> &callback);

  virtual bool HandleConflicts(Tx &dtxn,
                               innid_t inn_id,
                               vector<string> &conflicts) override {
    verify(0);
  };


  void GeneralPrint(const char* msg, txnid_t tx_id, Row* row, uint64_t* key_hash);

  bool OnPrepare(cmdid_t);
  bool DoPrepare(txnid_t tx_id, Marshallable* cmd = nullptr);
  bool DoPrepareResult(txnid_t tx_id, Marshallable& cmd);
  void DoCommit(Tx& tx) override;
  void Next(Marshallable&) override;
  int PrepareReplicated(Marshallable& cmd) ;
  int PrepareCarouselReplicated(Marshallable& cmd);
  int CommitReplicated(Marshallable& cmd);

  mdb::Txn* get_mdb_txn(const i64 tid);

  virtual bool DispatchPiece(Tx& tx,
                             TxPieceData& cmd,
                             TxnOutput& ret_output) override {
    SchedulerClassic::DispatchPiece(tx, cmd, ret_output);
    ExecutePiece(tx, cmd, ret_output);
    return true;
  }

  // Protect the below memebers.
  SpinLock lock_;
  // Key is ptr addr of the row (key), Value is the counter.
  unordered_map<uint64_t, int> pending_write_row_map_;
  unordered_map<uint64_t, int> pending_read_row_map_;
  unordered_set<txnid_t> prepared_trans_ids_;
  // for followers to check if the txn is prepared without a rep group.
  unordered_set<txnid_t> prepared_trans_ids_follower_;
  bool using_basic_ = false;
};

} // namespace janus
