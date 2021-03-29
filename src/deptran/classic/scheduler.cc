#include "../constants.h"
#include "../tx.h"
#include "../procedure.h"
#include "../coordinator.h"
#include "../2pl/tx.h"
#include "scheduler.h"
#include "tpc_command.h"
#include "tx.h"

namespace janus {

void SchedulerClassic::MergeCommands(vector<shared_ptr<TxPieceData>>& ops,
                                     shared_ptr<Marshallable> cmd2) {

  verify(0);
//  auto& sp_v2 = dynamic_pointer_cast<VecPieceData>(cmd2)->sp_vec_piece_data_;
//  verify(sp_v2);
//  for (auto& cmd: *sp_v2) {
//    verify(std::all_of(sp_v1->begin(), sp_v1->end(), [&cmd] (TxPieceData& d) {
//      return (cmd.root_id_ == d.root_id_) && (cmd.inn_id_ != d.inn_id_);
//    }));
//    sp_v1->push_back(cmd);
//  }
}

bool SchedulerClassic::ExecutePiece(Tx& tx,
                                    TxPieceData& piece_data,
                                    TxnOutput& ret_output) {
  auto roottype = piece_data.root_type_;
  auto subtype = piece_data.type_;
  TxnPieceDef& piece_def = txn_reg_->get(roottype, subtype);
  int ret_code;
  auto& conflicts = piece_def.conflicts_;
  piece_data.input.Aggregate(tx.ws_);
// TODO enable this verify
  piece_data.input.VerifyReady();
  piece_def.proc_handler_(nullptr,
                          tx,
                          piece_data,
                          &ret_code,
                          ret_output[piece_data.inn_id()]);
  tx.ws_.insert(ret_output[piece_data.inn_id()]);
  return true;
}

bool SchedulerClassic::DispatchPiece(Tx& tx,
                                     TxPieceData& piece_data,
                                     TxnOutput& ret_output) {
  TxnPieceDef
      & piece_def = txn_reg_->get(piece_data.root_type_, piece_data.type_);
  auto& conflicts = piece_def.conflicts_;
//  auto id = piece_data.inn_id();
  // Two phase locking won't pass these
//  verify(!tx.inuse);
//  tx.inuse = true;

	/*struct timespec begin, end;
	clock_gettime(CLOCK_MONOTONIC, &begin);*/
  for (auto& c: conflicts) {
    vector<Value> pkeys;
    for (int i = 0; i < c.primary_keys.size(); i++) {
      pkeys.push_back(piece_data.input.at(c.primary_keys[i]));
    }
    auto row = tx.Query(tx.GetTable(c.table), pkeys, c.row_context_id);
    verify(row != nullptr);
    for (auto col_id : c.columns) {
      if (!Guard(tx, row, col_id)) {
        tx.inuse = false;
//        auto reactor = Reactor::GetReactor();
//        auto sz = reactor->coros_.size();
//        verify(sz > 0);
        auto id = piece_data.inn_id();
        ret_output[id] = {}; // the client uses this to identify ack.
        return false; // abort
      }
    }
  }
	/*clock_gettime(CLOCK_MONOTONIC, &end);
	Log_info("time of dispatch: %d", end.tv_nsec-begin.tv_nsec);*/
//  tx.inuse = false;
  return true;
}

bool SchedulerClassic::Dispatch(cmdid_t cmd_id,
                                struct DepId dep_id,
                                shared_ptr<Marshallable> cmd,
                                TxnOutput& ret_output) {
  auto sp_vec_piece =
      dynamic_pointer_cast<VecPieceData>(cmd)->sp_vec_piece_data_;
  verify(sp_vec_piece);
  auto tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(cmd_id));
  verify(tx);
//  MergeCommands(tx.cmd_, cmd);
  Log_info("received dispatch for tx id: %" PRIx64, tx->tid_);
//  verify(partition_id_ == piece_data.partition_id_);
  // pre-proces
  // TODO separate pre-process and process/commit
  // TODO support user-customized pre-process.
// for debug purpose
//  bool b1 = false, b2 = false;
//  for (auto& piece_data : *sp_vec_piece) {
//    if (piece_data.inn_id_ == 200) b1 = true;
//    if (piece_data.inn_id_ == 205) b2 = true;
//  }
//  verify(b1 == b2);
  verify(cmd) ;
  if (!tx->cmd_) {
    tx->cmd_ = cmd;
  } else if (tx->cmd_ != cmd) {
    auto present_cmd =
        dynamic_pointer_cast<VecPieceData>(tx->cmd_)->sp_vec_piece_data_;
    for (auto& sp_piece_data : *sp_vec_piece) {
      present_cmd->push_back(sp_piece_data);
    }
  } else {
    // do nothing
//    verify(0);
  }

	struct timespec begin, end;
	//clock_gettime(CLOCK_MONOTONIC, &begin);
  bool ret = true;
  for (const auto& sp_piece_data : *sp_vec_piece) {
    verify(sp_piece_data);
    ret = DispatchPiece(*tx, *sp_piece_data, ret_output);
    if (!ret) {
      break;
    }
  }
	/*clock_gettime(CLOCK_MONOTONIC, &end);
	Log_info("time of dispatch2: %d", end.tv_nsec-begin.tv_nsec);*/
  // TODO reimplement this.
  if (tx->fully_dispatched_->value_ == 0) {
    tx->fully_dispatched_->Set(1);
  }
  return ret;
}

// On prepare with replication
//   1. dispatch the whole transaction to others.
//   2. use a paxos command to commit the prepare request.
//   3. after that, run the function to prepare.
//   0. an non-optimized version would be.
//      dispatch the transaction command with paxos instance
bool SchedulerClassic::OnPrepare(cmdid_t tx_id,
                                 const std::vector<i32>& sids,
                                 struct DepId dep_id,
																 bool& null_cmd,
																 std::vector<shared_ptr<QuorumEvent>>& quorum_events) {
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  verify(sp_tx);
	/*if(sp_tx->cmd_ == NULL){
		null_cmd = true;
		return false;
	}*/
  Log_debug("%s: at site %d, tx: %"
                PRIx64, __FUNCTION__, this->site_id_, tx_id);
  if (Config::GetConfig()->IsReplicated()) {
    auto sp_prepare_cmd = std::make_shared<TpcPrepareCommand>();
    verify(sp_prepare_cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE);
    sp_prepare_cmd->tx_id_ = tx_id;
    sp_prepare_cmd->cmd_ = sp_tx->cmd_;
    auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_cmd);
    sp_tx->is_leader_hint_ = true;
		
		struct timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
    //Log_info("This is dep_id: %d", dep_id);
    // here, we need to let the paxos coordinator know what request we are working with
    // thsi could be the transaction id or we can add a new id
    auto coo = CreateRepCoord(dep_id.id);
		
		/*clock_gettime(CLOCK_MONOTONIC, &end);
		Log_info("time of prepare on server: %d", end.tv_nsec-begin.tv_nsec);*/
    //Log_info("The locale id: %d", coo->loc_id_);
    coo->Submit(sp_m);
    sp_tx->prepare_result->Wait();
		slow_ = coo->slow_;
		
		quorum_events = coo->quorum_events_;
//    Log_debug("finished prepare command replication");
    return sp_tx->prepare_result->Get();
  } else if (Config::GetConfig()->do_logging()) {
    string log;
    this->get_prepare_log(tx_id, sids, &log);
    //   recorder_->submit(log, callback);
  } else {
    return DoPrepare(tx_id);
  }
  return false;
}

int SchedulerClassic::PrepareReplicated(TpcPrepareCommand& prepare_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  // TODO verify it is the same leader, error if not.
  // TODO and return the prepare callback here.
  auto tx_id = prepare_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  if (!sp_tx->cmd_)
    sp_tx->cmd_ = prepare_cmd.cmd_;
  if (!sp_tx->is_leader_hint_) {
    return 0;
  }
  // else: is the leader.
  sp_tx->prepare_result->Set(DoPrepare(sp_tx->tid_));
  Log_debug("prepare request replicated and executed for %" PRIx64 ", result: %x, sid: %x",
      sp_tx->tid_, sp_tx->prepare_result->Get(), (int)this->site_id_);
  Log_debug("triggering prepare replication callback %" PRIx64, sp_tx->tid_);
  return 0;
}

int SchedulerClassic::OnEarlyAbort(txnid_t tx_id) {
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  DoAbort(*sp_tx);
  return 0;
}

int SchedulerClassic::OnCommit(txnid_t tx_id,
															 struct DepId dep_id,
															 int commit_or_abort,
															 std::vector<shared_ptr<QuorumEvent>>& quorum_events) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  Log_debug("%s: at site %d, tx: %" PRIx64,
            __FUNCTION__, this->site_id_, tx_id);
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  // TODO maybe change inuse to an event?
//  verify(!sp_tx->inuse);
//  sp_tx->inuse = true;
//
  //always true
  if (Config::GetConfig()->IsReplicated()) {
    auto cmd = std::make_shared<TpcCommitCommand>();
    cmd->tx_id_ = tx_id;
    cmd->ret_ = commit_or_abort;
		
		struct timespec begin, end;
		//clock_gettime(CLOCK_MONOTONIC, &begin);
    auto sp_m = dynamic_pointer_cast<Marshallable>(cmd);
    //here, we need to let the paxos coordinator know what the request is
    //Log_info("This is dep_id: %d", dep_id);
    auto coo = CreateRepCoord(dep_id.id);

		/*clock_gettime(CLOCK_MONOTONIC, &end);
		Log_info("time of commit on server: %d", (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec);*/
    coo->Submit(sp_m);
    //Log_info("Before failing verify");
    sp_tx->commit_result->Wait();
		slow_ = coo->slow_;
		
		quorum_events = coo->quorum_events_;
  } else {
    if (commit_or_abort == SUCCESS) {
      DoCommit(*sp_tx);
    } else if (commit_or_abort == REJECT) {
//      exec->AbortLaunch(res, callback);
      DoAbort(*sp_tx);
    } else {
      verify(0);
    }
  }
  return 0;
}

void SchedulerClassic::DoCommit(Tx& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->commit();
  tx_box.mdb_txn_ = nullptr;
  delete mdb_txn; // TODO remove this
}

void SchedulerClassic::DoAbort(Tx& tx_box) {
  auto mdb_txn = RemoveMTxn(tx_box.tid_);
  verify(mdb_txn == tx_box.mdb_txn_);
  mdb_txn->abort();
  delete mdb_txn; // TODO remove this
  tx_box.mdb_txn_ = nullptr;
}

int SchedulerClassic::CommitReplicated(TpcCommitCommand& tpc_commit_cmd) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx_id = tpc_commit_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  int commit_or_abort = tpc_commit_cmd.ret_;
  if (!sp_tx->is_leader_hint_) {
    if (commit_or_abort == REJECT) {
      sp_tx->commit_result->Set(1);
      return 0;
    } else {
      verify(sp_tx->cmd_);
      unique_ptr<TxnOutput> out = std::make_unique<TxnOutput>();
			DepId di;
			di.str = "dep";
			di.id = 0;
      Dispatch(sp_tx->tid_, di, sp_tx->cmd_, *out);
      DoPrepare(sp_tx->tid_);
    }
  }
  if (commit_or_abort == SUCCESS) {
    sp_tx->committed_ = true;
    DoCommit(*sp_tx);
  } else if (commit_or_abort == REJECT) {
    sp_tx->aborted_ = true;
    DoAbort(*sp_tx);
  } else {
    verify(0);
  }
  if (sp_tx->is_leader_hint_) {
    // mostly for debug
    sp_tx->commit_result->Set(1);
  }
//  sp_tx->commit_result->Set(1);
  sp_tx->ev_execute_ready_->Set(1);
  return 0;
}

void SchedulerClassic::Next(Marshallable& cmd) {
  if (cmd.kind_ == MarshallDeputy::CMD_TPC_PREPARE) {
    auto& c = dynamic_cast<TpcPrepareCommand&>(cmd);
    PrepareReplicated(c);
  } else if (cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    auto& c = dynamic_cast<TpcCommitCommand&>(cmd);
    CommitReplicated(c);
  } else if (cmd.kind_ == MarshallDeputy::CMD_TPC_EMPTY) {
    // do nothing
    auto& c = dynamic_cast<TpcEmptyCommand&>(cmd);
    c.Done();
  } else {
    verify(0);
  }
}

} // namespace janus
