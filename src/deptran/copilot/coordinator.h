#pragma once

#include "../__dep__.h"
#include "../coordinator.h"
#include "../frame.h"

namespace janus {

const uint64_t takeover_timeout_us = 10000;  // TODO: what to set here?
const uint64_t finalize_timeout_us = 200000;

class CopilotCommo;
class CopilotServer;
struct CopilotData;
class CoordinatorCopilot : public Coordinator {
  ballot_t curr_ballot_ = 0;

  shared_ptr<Marshallable> cmd_now_{nullptr};
  int current_phase_ = 0;

  bool fast_path_ = false;
  bool direct_commit_ = false;
  bool in_fast_takeover_ = false;
  bool done_ = false;

  uint64_t begin = 0;
  uint64_t fac = 0;
  uint64_t ac = 0;
  uint64_t cmt = 0;

 private:
  CopilotCommo *commo() {
    // TODO: fix this (fix what?)
    verify(commo_);
    return (CopilotCommo *)commo_;
  }
  ballot_t makeUniqueBallot(ballot_t ballot);
  ballot_t pickGreaterBallot(ballot_t ballot);
  void initFastTakeover(shared_ptr<CopilotData>& ins);
  void clearStatus();

  inline int maxFail() {
    return (n_replica_ - 1) / 2;
  }
  inline uint32_t getQuorum() {
    return n_replica_ / 2 + 1;
  }

 public:
  CopilotServer* sch_ = nullptr;
  enum Phase : int { INIT_END = 0, PREPARE, FAST_ACCEPT, ACCEPT, COMMIT };
  CoordinatorCopilot(uint32_t coo_id,
  					         int32_t benchmark,
                     ClientControlServiceImpl *ccsi,
                     uint32_t thread_id);
  virtual ~CoordinatorCopilot();

  inline bool IsPilot() {
    return loc_id_ == 0;
  }

  inline bool IsCopilot() {
    return loc_id_ == 1;
  }

  void DoTxAsync(TxRequest &req) override {}

  void Submit(shared_ptr<Marshallable> &cmd,
              const std::function<void()> &func = []() {},
              const std::function<void()> &exe_callback = []() {}) override;
  
  // Protocol operations
  void Prepare();
  void FastAccept();
  void Accept();
  void Commit();

  void Restart() override { verify(0); }

  void GotoNextPhase();

 public:
  uint32_t n_replica_ = 0;

  /* info of the current instance being coordinated */
  uint8_t is_pilot_ = 0;
  slotid_t slot_id_ = 0;
  slotid_t *slot_hint_ = nullptr;
  slotid_t dep_ = 0;
};

} // namespace janus
