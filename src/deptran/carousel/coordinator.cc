#include "../__dep__.h"
#include "coordinator.h"
#include "frame.h"
#include "commo.h"
#include "benchmark_control_rpc.h"

namespace janus {

CoordinatorCarousel::CoordinatorCarousel(uint32_t coo_id,
                                       int benchmark,
                                       ClientControlServiceImpl* ccsi,
                                       uint32_t thread_id)
    : CoordinatorClassic(coo_id,
                  benchmark,
                  ccsi,
                  thread_id), using_basic_(Config::GetConfig()->carousel_basic_mode()) {
  verify(commo_ == nullptr);
}

CarouselCommo *CoordinatorCarousel::commo() {
  if (commo_ == nullptr) {
    commo_ = new CarouselCommo();
    commo_->loc_id_ = loc_id_;
    ((CarouselCommo*)commo_)->using_basic_ = using_basic_;
    verify(loc_id_ < 100);
  }
  auto commo = dynamic_cast<CarouselCommo *>(commo_);
  verify(commo != nullptr);
  return commo;
}

void CoordinatorCarousel::ReadAndPrepare() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  auto tx_data = (TxData *) cmd_;

  int cnt = 0;
  auto cmds_by_par = tx_data->GetReadyPiecesData();

  for (auto &pair: cmds_by_par) {
    const parid_t &par_id = pair.first;
    auto& cmds = pair.second;
    n_dispatch_ += cmds.size();
    cnt += cmds.size();
    auto sp_vec_pieces = std::make_shared<vector<shared_ptr<TxPieceData>>>();
    for (auto& c: cmds) {
      c->id_ = next_pie_id();
      dispatch_acks_[c->inn_id_] = false;
      sp_vec_pieces->push_back(c);
    }
    commo()->BroadcastReadAndPrepare(sp_vec_pieces,
                               this,
                               std::bind(&CoordinatorCarousel::ReadAndPrepareAck,
                                         this,
                                         phase_,
                                         sp_vec_pieces->at(0)->root_id_,
                                         sp_vec_pieces->at(0)->PartitionId(),
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3));
  }
  Log_debug("sent %d SubCmds", n_dispatch_);
}

void CoordinatorCarousel::ReadAndPrepareAck(phase_t phase,
                                        cmdid_t txn_id,
                                        parid_t par_id,
                                        bool leader,
                                        int32_t res,
                                        TxnOutput &outputs) {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  if (phase != phase_ ) return;
  TxData *txn = (TxData *) cmd_;
  if (leader) {
    for (auto &pair : outputs) {
      n_dispatch_ack_++;  
      const innid_t &inn_id = pair.first;
      verify(dispatch_acks_[inn_id] == false);
      dispatch_acks_[inn_id] = true;
      Log_debug("get start ack %ld/%ld for cmd_id: %lx, inn_id: %d",
                n_dispatch_ack_, n_dispatch_, cmd_->id_, inn_id);
      // txn->Merge(pair.first, pair.second);
    }
  }

  if (using_basic_) {
    if (res != SUCCESS) {
      aborted_ = true;
      GotoNextPhase();
    } else if (AllDispatchAcked() && !aborted_) {
      committed_ = true;
      GotoNextPhase();
    } else {
      // do nothing
    }
    return;
  }
  
  if (leader) {
    if (res != SUCCESS) {
      aborted_ = true;
      GotoNextPhase();
      return;
    } 
    n_leader_reply_[par_id] = true;
  }
  if (res == SUCCESS) {
    n_prepare_oks_[par_id]++;
  } else {
    n_prepare_rejects_[par_id]++;
  }
  if (!aborted_ && (SuperMajorPrepareOK() && AllDispatchAcked())) {
    committed_ = true;
    GotoNextPhase();
    return;
  }

  if(NoMajorPrepareOK()) {
    aborted_ = true;
    GotoNextPhase();
  }

}

bool CoordinatorCarousel::AllDispatchAcked() {
  bool ret = std::all_of(dispatch_acks_.begin(),
                          dispatch_acks_.end(),
                          [](std::pair<innid_t, bool> pair) {
                            return pair.second;
                          });
  if (ret)
    verify(n_dispatch_ack_ == n_dispatch_);
  return ret;
}

bool CoordinatorCarousel::SuperMajorPrepareOK() {
  bool super_maj_ok = std::all_of(n_prepare_oks_.begin(),
                          n_prepare_oks_.end(),
                          [this](std::pair<parid_t, int> pair) {
                            return pair.second >= n_prepare_super_maj_[pair.first];
                          });
  if (super_maj_ok) {
    // Need all leader replies.
    return std::all_of(n_leader_reply_.begin(),
                            n_leader_reply_.end(),
                            [this](std::pair<parid_t, int> pair) {
                              return pair.second;
                            });
  }
  return false;
}

bool CoordinatorCarousel::NoMajorPrepareOK() {
  return std::any_of(n_prepare_rejects_.begin(),
                          n_prepare_rejects_.end(),
                          [this](std::pair<parid_t, int> pair) {
                            return pair.second > n_prepare_no_maj_[pair.first];
                          });
}

void CoordinatorCarousel::Decide() {
  verify(committed_ != aborted_);
  int32_t d = 0;
  if (committed_) {
    d = Decision::COMMIT;
  } else if (aborted_) {
    d = Decision::ABORT;
  } else {
    verify(0);
  }
  auto pars = cmd_->GetPartitionIds();
  auto id = cmd_->id_;
  Log_debug("send out decide request, cmd_id: %llx, ", id);
  for (auto par_id : pars) {
    commo()->BroadcastDecide(par_id, id, d);
  }
  GotoNextPhase();
}

void CoordinatorCarousel::Reset() {
  CoordinatorClassic::Reset();
  dispatch_acks_.clear();
  n_prepare_oks_.clear();
  n_prepare_rejects_.clear();
}

void CoordinatorCarousel::Restart() {
  std::lock_guard<std::recursive_mutex> lock(this->mtx_);
  cmd_->root_id_ = this->next_txn_id();
  cmd_->id_ = cmd_->root_id_;
  CoordinatorClassic::Restart();
}

void CoordinatorCarousel::GotoNextPhase() {
  int n_phase = 3;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:ReadAndPrepare();
      verify(phase_ % n_phase == Phase::READPREPARE);
      break;
    case Phase::READPREPARE:verify(phase_ % n_phase == Phase::DECIDE);
      Decide();
      break;
    case Phase::DECIDE:verify(phase_ % n_phase == Phase::INIT_END);
      if (committed_)
        End();
      else if (aborted_)
        Restart();
      else
        verify(0);
      break;
    default:verify(0);
  }
}

} // namespace janus
