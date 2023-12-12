#pragma once

#include "../__dep__.h"
#include "../coordinator.h"
#include "../frame.h"

namespace janus {

class EpaxosCommo;
class EpaxosServer;
class EpaxosCoordinator : public Coordinator {
 public:
  EpaxosServer* sch_ = nullptr;
 private:
  enum Phase { INIT = 0, PREACCEPT = 1, ACCEPT = 2, COMMIT = 3 };
  const int32_t n_phase_ = 4;

	EpaxosCommo *commo() {
    verify(commo_ != nullptr);
    return (EpaxosCommo *) commo_;
  }
  
  bool in_submission_ = false; // debug;
  bool in_prepare_ = false; // debug
  bool in_accept = false; // debug
  bool in_append_entries = false; // debug
  uint64_t minIndex = 0;
 public:
  shared_ptr<Marshallable> cmd_{nullptr};
  EpaxosCoordinator(uint32_t coo_id,
                    int32_t benchmark,
                    ClientControlServiceImpl *ccsi,
                    uint32_t thread_id);
  ballot_t curr_ballot_ = 1; // TODO
  uint32_t n_replica_ = 0;   // TODO
  slotid_t slot_id_ = 0;
  slotid_t *slot_hint_ = nullptr;
  uint64_t cmt_idx_ = 0 ;

  uint32_t n_replica() {
    verify(n_replica_ > 0);
    return n_replica_;
  }

  void DoTxAsync(TxRequest &req) override {}

  void Submit(shared_ptr<Marshallable> &cmd,
              const std::function<void()> &func = []() {},
              const std::function<void()> &exe_callback = []() {}) override;

  void Commit();

  void Reset() override {}
  void Restart() override { verify(0); }

  void GotoNextPhase();
};

} //namespace janus