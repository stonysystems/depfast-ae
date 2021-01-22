#include "../__dep__.h"
#include "../command.h"
#include "../occ/tx.h"
#include "tx.h"
#include "scheduler.h"
#include "coordinator.h"
#include "../classic/tpc_command.h"

namespace janus {

SchedulerCarousel::SchedulerCarousel() : SchedulerClassic() {
  // TODO: yidawu use OCC txnmgr by now.
  mdb_txn_mgr_ = make_shared<mdb::TxnMgrOCC>();
  using_basic_ = Config::GetConfig()->carousel_basic_mode();
}

bool SchedulerCarousel::Guard(Tx &tx, Row *row, int col_id, bool write) {
  // do nothing
  return true;
}

int SchedulerCarousel::OnDecide(txid_t tx_id,
                             int32_t decision,
                             const function<void()> &callback) {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto cmd = std::make_shared<TpcCommitCommand>();
  cmd->tx_id_ = tx_id;
  cmd->ret_ = decision;
  auto sp_m = dynamic_pointer_cast<Marshallable>(cmd);
  CreateRepCoord()->Submit(sp_m);
  auto tx = dynamic_pointer_cast<TxCarousel>(GetTx(tx_id));
  tx->commit_result->Wait();
  callback();
  return 0;
}


mdb::Txn* SchedulerCarousel::get_mdb_txn(const i64 tid) {
  mdb::Txn *txn = nullptr;
  auto it = mdb_txns_.find(tid);
  if (it == mdb_txns_.end()) {
    //verify(IS_MODE_2PL);
    txn = mdb_txn_mgr_->start(tid);
    //XXX using occ lazy mode: increment version at commit time
    ((mdb::TxnOCC *) txn)->set_policy(mdb::OCC_LAZY);
    auto ret = mdb_txns_.insert(std::pair<i64, mdb::Txn *>(tid, txn));
    verify(ret.second);
  } else {
    txn = it->second;
  }
  verify(mdb_txn_mgr_->rtti() == mdb::symbol_t::TXN_OCC);
  verify(txn->rtti() == mdb::symbol_t::TXN_OCC);
  verify(txn != nullptr);
  return txn;
}

void SchedulerCarousel::GeneralPrint(const char* msg, txnid_t tx_id, Row* row, uint64_t* key_hash){
  stringstream ss;
  ss << msg;
  ss << " txn_id:" << (void*)tx_id;
  if (row != nullptr) {
    ss << " row:" << (uint64_t)row;
  }
  if (key_hash != nullptr) {
    ss << " key_hash:" << *key_hash;
  }
  ss << " thread id:" << std::this_thread::get_id();
  Log_debug(ss.str().c_str());
}


bool SchedulerCarousel::DoPrepare(txnid_t tx_id, Marshallable* cmd) {
  Log_debug("receive fast accept for cmd_id: %llx", tx_id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);  
  auto tx = dynamic_pointer_cast<TxCarousel>(GetOrCreateTx(tx_id));
  {
    lock_guard<SpinLock> l(lock_);
    if (prepared_trans_ids_.find(tx->tid_) != prepared_trans_ids_.end()) {
      // already prepared, return immediately.
      return true;
    }
  }

  // my understanding was that this is a wait-die locking for 2PC-prepare.
  // but to be safe, let us follow the stock protocol.
  // validate read versions
  tx->fully_dispatched_->Wait();
  if (tx->aborted_in_dispatch_) {
    GeneralPrint("DoPrepare failed aborted_in_dispatch_", tx_id, nullptr, nullptr);
    return false;
  }

  auto& read_vers = tx->read_vers_;
  unordered_map<uint64_t,int> read_hash_keys;
  unordered_map<uint64_t,int> write_hash_keys;

  lock_.lock();

  // TODO: yidawu may need to consider if the msg comes very late, may already committed,
  // so a committed list may be needed for checking to avoid the situation.  
  if (prepared_trans_ids_.find(tx->tid_) != prepared_trans_ids_.end()) {
    // already prepared, return immediately.
    lock_.unlock();
    return true;
  }
  
  for (auto& pair1 : read_vers) {
    Row* r = pair1.first;
    verify(r->rtti() == symbol_t::ROW_VERSIONED);
    auto row = dynamic_cast<mdb::VersionedRow*>(r);
    verify(row != nullptr);
    // reject if it exists in write row map.
    auto key_hash = mdb::MultiBlob::hash()(row->get_key());
    for (auto& pair2: pair1.second) {
      auto key_col_hash = key_hash + pair2.first;
      auto it = pending_write_row_map_.find(key_col_hash);
      if (it != pending_write_row_map_.end()) {
        GeneralPrint("DoPrepare failed  pending_read_row_map_ rw failed", tx_id, row, &key_col_hash);
        lock_.unlock();
        return false;
      }
      // read_hash_keys.emplace(key_col_hash, (uint64_t)row);
      read_hash_keys.emplace(key_col_hash, 1);
    }
  }
  // validate write set.
  verify(tx->cmd_ != nullptr);
  auto& txn_cmds =
      dynamic_pointer_cast<VecPieceData>(tx->cmd_)->sp_vec_piece_data_;  
  auto ver_write = ((*txn_cmds)[0])->timestamp_;
  for (auto& pair1 : tx->write_bufs_) {
    Row* r = pair1.first;
    verify(r->rtti() == symbol_t::ROW_VERSIONED);    
    mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(r);
    verify(row != nullptr);
    auto key_hash = mdb::MultiBlob::hash()(row->get_key());
    for (auto& pair2: pair1.second) {
      auto key_col_hash = key_hash + pair2.first;
      auto it = pending_read_row_map_.find(key_col_hash);
      if (it != pending_read_row_map_.end()) {
        if (it->second > 0) {
          GeneralPrint("DoPrepare failed  pending_read_row_map_ ww failed", tx_id, row, &key_col_hash);
          lock_.unlock();
          fflush(stdout);
          return false;
        } else {
          pending_read_row_map_.erase(it);
        }
      }
      it = pending_write_row_map_.find(key_col_hash);
      if (it != pending_write_row_map_.end()) {
        GeneralPrint("DoPrepare failed  pending_write_row_map_ ww failed", tx_id, row, &key_col_hash);
        lock_.unlock();
        return false;
      }
//      write_hash_keys.emplace(key_col_hash, (uint64_t)row);
      write_hash_keys.emplace(key_col_hash, 1);
    }
  }

  // Insert into the read map.
  for (auto& key_col_hash_it: read_hash_keys) {
    auto key_col_hash = key_col_hash_it.first;
    auto it = pending_read_row_map_.find(key_col_hash);
    if (it != pending_read_row_map_.end()) {
      GeneralPrint("DoPrepare add read hash", tx_id, (Row*)(key_col_hash_it.second), &key_col_hash);
      it->second = it->second + 1;
    } else {
      GeneralPrint("DoPrepare insert read hash", tx_id, (Row*)(key_col_hash_it.second), &key_col_hash);
      pending_read_row_map_.emplace(key_col_hash, 1) ;
    }
  }

  // Insert into the write map.
  for (auto& key_col_hash_it: write_hash_keys) {
    auto key_col_hash = key_col_hash_it.first;
    GeneralPrint("DoPrepare insert write hash", tx_id, (Row*)(key_col_hash_it.second), &key_col_hash);
    pending_write_row_map_.emplace(key_col_hash, 1) ;
  }

  // DEBUG
  verify(txn_cmds->size() > 0);
  prepared_trans_ids_.emplace(tx_id);
  Log_debug("Prepared txn: %llx, thread id: %lld", tx_id, std::this_thread::get_id());  
  // TODO: yidawu remove fflush
  fflush(stdout);

  if (cmd != nullptr) {
    lock_.unlock();
    auto prepare_cmd = (TpcPrepareCarouselCommand*)(cmd);    
    // Make a copy of result for followers.
    prepare_cmd->pending_write_row_map_ = write_hash_keys;
    prepare_cmd->pending_read_row_map_ = read_hash_keys;
/*    
    prepare_cmd->pending_write_row_map_.insert(pending_write_row_map_.begin(),
      pending_write_row_map_.end());
    prepare_cmd->pending_read_row_map_.insert(pending_read_row_map_.begin(),
      pending_read_row_map_.end());*/
  } else {
    prepared_trans_ids_follower_.emplace(tx_id);
    lock_.unlock();
  }

  return true;
}


bool SchedulerCarousel::DoPrepareResult(txnid_t tx_id, Marshallable& cmd) {
  Log_debug("receive fast accept for cmd_id: %llx", tx_id);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx = dynamic_pointer_cast<TxCarousel>(GetOrCreateTx(tx_id));
  auto& prepare_cmd = dynamic_cast<TpcPrepareCarouselCommand&>(cmd);  
  lock_guard<SpinLock> l(lock_);
  // TODO: yidawu do the swap anyway, double check
  /*if (prepared_trans_ids_.find(tx->tid_) != prepared_trans_ids_.end()) {
    // already prepared, return immediately.
    return true;
  }*/  
/*
  unordered_set<uint64_t> read_ad_hash_keys;
  unordered_set<uint64_t> read_rm_hash_keys;
  unordered_set<uint64_t> write_ad_hash_keys;
  unordered_set<uint64_t> write_rm_hash_keys;

  for (auto& it: prepare_cmd.pending_read_row_map_) {
    if (pending_read_row_map_.find(it.first) == pending_read_row_map_.end()) {
      read_ad_hash_keys.emplace(it.first);
    }
  }

  for (auto it: pending_read_row_map_) {
    if (prepare_cmd.pending_read_row_map_.find(it.first) ==
      prepare_cmd.pending_read_row_map_.end()) {
      read_rm_hash_keys.emplace(it.first);
    }
  }

  for (auto it: prepare_cmd.pending_write_row_map_) {
    if (pending_write_row_map_.find(it.first) == pending_write_row_map_.end()) {
      write_ad_hash_keys.emplace(it.first);
    }
  }

  for (auto it: pending_write_row_map_) {
    if (prepare_cmd.pending_write_row_map_.find(it.first) ==
      prepare_cmd.pending_write_row_map_.end()) {
      write_rm_hash_keys.emplace(it.first);
    }
  }*/

  // Make a copy from leader to followers.
  for (auto write_it:prepare_cmd.pending_write_row_map_) {
    pending_write_row_map_.emplace(write_it.first, write_it.second);
  }
  for (auto read_it:prepare_cmd.pending_read_row_map_) {
    auto it = pending_read_row_map_.find(read_it.first);
    if (it != pending_read_row_map_.end()) {
      it->second = it->second +1; 
    } else {
      pending_read_row_map_.emplace(read_it.first, read_it.second);
    }
  }
  //swap(prepare_cmd.pending_write_row_map_, pending_write_row_map_);
  //swap(prepare_cmd.pending_read_row_map_, pending_read_row_map_);

  prepared_trans_ids_.emplace(tx_id);
/*
  stringstream ss;
  ss << "read_ad_hash_keys: ";
  for( auto key: read_ad_hash_keys) {
    ss << key << ",";
  }
  ss << "read_rm_hash_keys: "  ;
  for( auto key: read_rm_hash_keys) {
    ss << key << ",";
  }
  ss << "write_ad_hash_keys: "  ;
  for( auto key: write_ad_hash_keys) {
    ss << key << ",";
  }
  ss << "write_rm_hash_keys: " ;
  for( auto key: write_rm_hash_keys) {
    ss << key << ",";
  }
//  Log_debug("Prepared Result txn: %llx, thread id: %lld, %s", tx_id,
//    std::this_thread::get_id(), ss.str().c_str());
  Log_debug("Prepared Result txn: %llx, thread id: %lld, %s", tx_id, std::this_thread::get_id(), "");
  */
  return true;
}

void SchedulerCarousel::DoCommit(Tx& tx_input) {
    std::lock_guard<std::recursive_mutex> lock(mtx_);
   unordered_set<uint64_t> read_hash_keys;
    unordered_set<uint64_t> write_hash_keys;  
    
    auto tx = static_cast<TxCarousel*>(&tx_input);
    auto& read_vers = tx->read_vers_;
    bool committed = tx->committed_;
    lock_.lock();
    auto tx_it = prepared_trans_ids_.find(tx->tid_);
    if (tx_it == prepared_trans_ids_.end()) {
      if (committed) {
        verify(false);
      } else {
        // may not exist if doing abort.
        lock_.unlock();
        return;
      }
    }

    if (!committed) {
      auto tx_follower_it = prepared_trans_ids_follower_.find(tx->tid_);
      if (tx_follower_it != prepared_trans_ids_follower_.end()) {
        // we don't do anything if the txn is prepared without a rep group and
        // now is aborted, the write and read map would be updated by a prepare rep
        // from leader later.
        prepared_trans_ids_follower_.erase(tx_follower_it);
        lock_.unlock();
        return;
      }
    }

    // Decrease the count of row in the read map.
    for (auto& pair1 : read_vers) {
      Row* r = pair1.first;
      verify(r->rtti() == symbol_t::ROW_VERSIONED);
      auto row = dynamic_cast<mdb::VersionedRow*>(r);
      verify(row != nullptr);
      auto key_hash = mdb::MultiBlob::hash()(row->get_key());
      // TODO: yidawu optimization to static arr in tx
      for (auto& pair2: pair1.second) {
        auto key_col_hash = key_hash + pair2.first;      
        if(!read_hash_keys.emplace(key_col_hash).second) {
          continue;
        }
        auto it = pending_read_row_map_.find(key_col_hash);
        verify (it != pending_read_row_map_.end()) ;
        verify(it->second > 0);
        it->second = it->second -1;
        if (it->second == 0) {
          pending_read_row_map_.erase(it);
        }
        GeneralPrint("DoCommit decrease read", tx->tid_, row, &key_col_hash);
        fflush(stdout);
      }
    }
    
    // Update value and remove the tx in the write map.
    for (auto& pair1 : tx->write_bufs_) {
      Row* r = pair1.first;
      verify(r->rtti() == symbol_t::ROW_VERSIONED);    
      mdb::VersionedRow* row = dynamic_cast<mdb::VersionedRow*>(r);
      verify(row != nullptr);
      if (committed) {
        // Update the value from the buffer to the row.
        for (auto& pair2: pair1.second) {
          auto& col_id = pair2.first;
          auto& value = pair2.second;
          row->update(col_id, value);
          row->set_column_ver(col_id, value.ver_);
        }
      }
      auto key_hash = mdb::MultiBlob::hash()(row->get_key());
      for (auto& pair2: pair1.second) {
        auto key_col_hash = key_hash + pair2.first;            
        if(!write_hash_keys.emplace(key_col_hash).second) {
          continue;
        }
        auto it = pending_write_row_map_.find(key_col_hash);
        // TODO: yidawu here is a bug there could be an unknown
        // row suddenly appears in the followers if it uses pre replicate,
        // the row doesn't exist in other
        if (it == pending_write_row_map_.end()) {
          verify(row->get_table() == nullptr);
          continue;
        }
        verify(it->second == 1);
        pending_write_row_map_.erase(it);
        GeneralPrint("DoCommit erase write ", tx->tid_, row, &key_col_hash);
        fflush(stdout);
      }
    }

    prepared_trans_ids_.erase(tx_it);

    if (committed && !prepared_trans_ids_follower_.empty()) {
      prepared_trans_ids_follower_.erase(tx->tid_);
    }
    lock_.unlock();
    // TODO: yidawu remove fflush
    Log_debug("Committed txn: %llx, is committed %d, thread id: %lld", tx->tid_, committed,
      std::this_thread::get_id());
    fflush(stdout);

    tx->read_vers_.clear();
    tx->write_bufs_.clear();
    DestroyTx(tx->tid_);    
}

int SchedulerCarousel::PrepareReplicated(Marshallable& cmd) {
  auto& prepare_cmd = dynamic_cast<TpcPrepareCommand&>(cmd);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx_id = prepare_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  if (!sp_tx->cmd_) {
    sp_tx->cmd_ = prepare_cmd.cmd_;
  }
  sp_tx->prepare_result->Set(DoPrepare(sp_tx->tid_));
  Log_debug("prepare request replicated and executed for %" PRIx64 ", result: %x, sid: %x",
      sp_tx->tid_, sp_tx->prepare_result->Get(), (int)this->site_id_);
  Log_debug("triggering prepare replication callback %" PRIx64, sp_tx->tid_);
  return 0;
}

int SchedulerCarousel::PrepareCarouselReplicated(Marshallable& cmd) {
  auto& prepare_cmd = dynamic_cast<TpcPrepareCarouselCommand&>(cmd);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx_id = prepare_cmd.tx_id_;
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
  if (!sp_tx->cmd_) {
    sp_tx->cmd_ = prepare_cmd.cmd_;
  }
  
  if (sp_tx->is_leader_hint_) {
    sp_tx->prepare_result->Set(true);
  } else {
    sp_tx->prepare_result->Set(DoPrepareResult(sp_tx->tid_, prepare_cmd));
  }
  Log_debug("prepare request replicated and executed for %" PRIx64 ", result: %x, sid: %x",
      sp_tx->tid_, sp_tx->prepare_result->Get(), (int)this->site_id_);
  Log_debug("triggering prepare replication callback %" PRIx64, sp_tx->tid_);
  return 0;
}

int SchedulerCarousel::CommitReplicated(Marshallable& cmd) {
    auto& tpc_commit_cmd = dynamic_cast<TpcCommitCommand&>(cmd);  
    std::lock_guard<std::recursive_mutex> lock(mtx_);
    auto tx_id = tpc_commit_cmd.tx_id_;
    auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
    int decision = tpc_commit_cmd.ret_;
    if (decision == CoordinatorCarousel::Decision::COMMIT) {
      sp_tx->committed_ = true;
      DoCommit(*sp_tx);
    } else if (decision == CoordinatorCarousel::Decision::ABORT) {
      sp_tx->aborted_ = true;
      DoCommit(*sp_tx);
    } else {
      verify(0);
    }
    sp_tx->commit_result->Set(1);
    sp_tx->ev_execute_ready_->Set(1);
    return 0;
}

bool SchedulerCarousel::OnPrepare(cmdid_t tx_id) {
  auto sp_prepare_crs_cmd = std::make_shared<TpcPrepareCarouselCommand>();
  verify(sp_prepare_crs_cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE_CAROUSEL);
  std::lock_guard<std::recursive_mutex> lock(mtx_);
/*
  if (using_basic_) {
    auto sp_prepare_cmd = std::make_shared<TpcPrepareCommand>();
    verify(sp_prepare_cmd->kind_ == MarshallDeputy::CMD_TPC_PREPARE);
    // replicate the command to the rep group.
    auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
    sp_prepare_cmd->tx_id_ = tx_id;
    sp_prepare_cmd->cmd_ = sp_tx->cmd_;
    auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_cmd);
    sp_tx->is_leader_hint_ = true;
    CreateRepCoord()->Submit(sp_m);
    sp_tx->prepare_result->Wait();
    return sp_tx->prepare_result->Get();
  }*/
  
  if (DoPrepare(tx_id, sp_prepare_crs_cmd.get())) {
    const auto& func = [tx_id, sp_prepare_crs_cmd, this]() {
      // replicate the command to the rep group.
      auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
      sp_prepare_crs_cmd->tx_id_ = tx_id;
      sp_prepare_crs_cmd->cmd_ = sp_tx->cmd_;
      auto sp_m = dynamic_pointer_cast<Marshallable>(sp_prepare_crs_cmd);
      sp_tx->is_leader_hint_ = true;
      CreateRepCoord()->Submit(sp_m);
      sp_tx->prepare_result->Wait();
      verify(sp_tx->prepare_result->Get());
    };
    if (using_basic_) {
      func();
    }  else {
      // Don't wait for the replication.
      Coroutine::CreateRun(func);
    }
    return true;    
  }
  // Leader prepare failed, return failed and abort.
  return false;
}

void SchedulerCarousel::Next(Marshallable& cmd) {
  if (cmd.kind_ == MarshallDeputy::CMD_TPC_PREPARE) {
    PrepareReplicated(cmd);
  } else if (cmd.kind_ == MarshallDeputy::CMD_TPC_PREPARE_CAROUSEL) {
    PrepareCarouselReplicated(cmd);
  }  else if (cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT) {
    CommitReplicated(cmd);
  } else if (cmd.kind_ == MarshallDeputy::CMD_TPC_EMPTY) {
    // do nothing
    auto& c = dynamic_cast<TpcEmptyCommand&>(cmd);
    c.Done();
  } else {
    verify(0);
  }
}


} // namespace janus
