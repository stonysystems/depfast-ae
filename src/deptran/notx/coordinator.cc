
/**
 * What shoud we do to change this to asynchronous?
 * 1. Fisrt we need to have a queue to hold all transaction requests.
 * 2. pop next request, send start request for each piece until there is no
 *available left.
 *          in the callback, send the next piece of start request.
 *          if responses to all start requests are collected.
 *              send the finish request
 *                  in callback, remove it from queue.
 *
 */

#include "coordinator.h"
#include "../frame.h"
#include "../benchmark_control_rpc.h"

namespace janus {

CoordinatorNoTx::CoordinatorNoTx(uint32_t coo_id,
                                       int benchmark,
                                       ClientControlServiceImpl* ccsi,
                                       uint32_t thread_id)
    : Coordinator(coo_id,
                  benchmark,
                  ccsi,
                  thread_id) {
  verify(commo_ == nullptr);
}

Communicator* CoordinatorNoTx::commo() {
  if (commo_ == nullptr) {
    commo_ = new Communicator;
  }
  verify(commo_ != nullptr);
  return commo_;
}

void CoordinatorNoTx::DoTxAsync(TxRequest& req) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  TxData* cmd = frame_->CreateTxnCommand(req, txn_reg_);
  verify(txn_reg_ != nullptr);
  cmd->root_id_ = this->next_txn_id();
  cmd->id_ = cmd->root_id_;
  ongoing_tx_id_ = cmd->id_;
  Log_debug("assigning tx id: %" PRIx64, ongoing_tx_id_);
  cmd->timestamp_ = GenerateTimestamp();
  cmd_ = cmd;
  n_retry_ = 0;
  Reset(); // In case of reuse.

  Log_debug("do one request txn_id: %d", cmd_->id_);
  Coroutine::CreateRun([this]() { Prepare(); });
}


void CoordinatorNoTx::Reset() {
  Coordinator::Reset();
  for (int i = 0; i < site_prepare_.size(); i++) {
    site_prepare_[i] = 0;
  }
  n_dispatch_ = 0;
  n_dispatch_ack_ = 0;
  n_prepare_req_ = 0;
  n_prepare_ack_ = 0;
  n_finish_req_ = 0;
  n_finish_ack_ = 0;
  dispatch_acks_.clear();
  committed_ = false;
  aborted_ = false;
}


/** caller should be thread_safe */
void CoordinatorNoTx::Prepare() {
  TxData* cmd = (TxData*) cmd_;
  auto mode = Config::GetConfig()->tx_proto_;
  verify(mode == MODE_NOTX);

  std::vector<i32> sids;
  for (auto& site : cmd->partition_ids_) {
    sids.push_back(site);
  }
  int partition_id = 0 ;

  //for (auto& partition_id : cmd->partition_ids_) {
    Log_debug("send prepare tid: %ld; partition_id %d",
              cmd_->id_,
              partition_id);

    SimpleCommand* simpleCmd = new SimpleCommand();

    commo()->SendSimpleCmd(partition_id,
                         *simpleCmd,
                         sids,
                         std::bind(&CoordinatorNoTx::PrepareAck,
                                   this,
                                   phase_,
                                   std::placeholders::_1));
    verify(site_prepare_[partition_id] == 0);
    site_prepare_[partition_id]++;
    verify(site_prepare_[partition_id] == 1);
  //}
}

void CoordinatorNoTx::PrepareAck(phase_t phase, int res) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  /*if (phase != phase_) return;*/
  TxData* cmd = (TxData*) cmd_;
  n_prepare_ack_++;
  committed_ = true ;
  End() ;
}

void CoordinatorNoTx::End() {
  TxData* tx_data = (TxData*) cmd_;
  TxReply& tx_reply_buf = tx_data->get_reply();
  double last_latency = tx_data->last_attempt_latency();
  if (committed_) {
    tx_data->reply_.res_ = SUCCESS;
    this->Report(tx_reply_buf, last_latency);
  } else if (aborted_) {
    tx_data->reply_.res_ = REJECT;
  } else {
    verify(0);
  }
  tx_reply_buf.tx_id_ = ongoing_tx_id_;
  Log_debug("call reply for tx_id: %"
                PRIx64, ongoing_tx_id_);
  tx_data->callback_(tx_reply_buf);
  ongoing_tx_id_ = 0;
  delete tx_data;
}

void CoordinatorNoTx::Report(TxReply& txn_reply,
                                double last_latency) {

  bool not_forwarding = forward_status_ != PROCESS_FORWARD_REQUEST;
  if (ccsi_ && not_forwarding) {
    if (txn_reply.res_ == SUCCESS) {
      ccsi_->txn_success_one(thread_id_,
                             txn_reply.txn_type_,
                             txn_reply.start_time_,
                             txn_reply.time_,
                             last_latency,
                             txn_reply.n_try_);
    } else
      ccsi_->txn_reject_one(thread_id_,
                            txn_reply.txn_type_,
                            txn_reply.start_time_,
                            txn_reply.time_,
                            last_latency,
                            txn_reply.n_try_);
  }
}

void CoordinatorNoTx::SetNewLeader(parid_t par_id, volatile locid_t* cur_pause) {
    locid_t prev_pause_srv = *cur_pause ;
retry:
    Log_debug("start setting a new leader from %d", prev_pause_srv );
    auto e = commo()->BroadcastGetLeader(par_id, prev_pause_srv); 
    e->Wait() ;
    if (e->Yes()) {
      // assign new leader
      Log_debug("set a new leader %d", e->leader_id_ );
      commo()->SetNewLeaderProxy(par_id, e->leader_id_) ;
      if ( prev_pause_srv != e->leader_id_ )
      {
        *cur_pause = e->leader_id_ ;
      }
    } else if (e->No()) {
        auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(300*1000);
        sp_e->Wait(300*1000) ;
        //usleep(300 * 1000) ;  // 300 ms
        goto retry ;
    } else {
        verify(0);
    }      
}

} // namespace janus
