#pragma once

#include "../__dep__.h"
#include "../coordinator.h"
#include "../frame.h"

namespace janus {

class FpgaRaftCommo;
class FpgaRaftServer;
class CoordinatorFpgaRaft : public Coordinator {
 public:
//  static ballot_t next_slot_s;
  FpgaRaftServer* sch_ = nullptr;
 private:
  enum Phase { INIT_END = 0, PREPARE = 1, ACCEPT = 2, COMMIT = 3, FORWARD = 4 };
  const int32_t n_phase_ = 4;

	FpgaRaftCommo *commo() {
    // TODO fix this.
    verify(commo_ != nullptr);
    return (FpgaRaftCommo *) commo_;
  }
  bool in_submission_ = false; // debug;
  bool in_prepare_ = false; // debug
  bool in_accept = false; // debug
  bool in_append_entries = false; // debug
  uint64_t minIndex = 0;
 public:
  shared_ptr<Marshallable> cmd_{nullptr};
  CoordinatorFpgaRaft(uint32_t coo_id,
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

  bool IsLeader() ;
  bool IsFPGALeader() ;

  slotid_t GetNextSlot() {
    verify(0);
    verify(slot_hint_ != nullptr);
    slot_id_ = (*slot_hint_)++;
    return 0;
  }

  uint32_t GetQuorum() {
    return n_replica() / 2 + 1;
  }

  void DoTxAsync(TxRequest &req) override {}
  void Forward(shared_ptr<Marshallable> &cmd,
              const std::function<void()> &func = []() {},
              const std::function<void()> &exe_callback = []() {}) ;

  void Submit(shared_ptr<Marshallable> &cmd,
              const std::function<void()> &func = []() {},
              const std::function<void()> &exe_callback = []() {}) override;

  void AppendEntries();
  void Commit();
  void LeaderLearn();

  void Reset() override {}
  void Restart() override { verify(0); }

  void GotoNextPhase();
};

} //namespace janus
