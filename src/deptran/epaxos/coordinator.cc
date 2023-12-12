
#include "../__dep__.h"
#include "../constants.h"
#include "coordinator.h"
#include "commo.h"

#include "server.h"

namespace janus {

EpaxosCoordinator::EpaxosCoordinator(uint32_t coo_id,
                                         int32_t benchmark,
                                         ClientControlServiceImpl* ccsi,
                                         uint32_t thread_id)
    : Coordinator(coo_id, benchmark, ccsi, thread_id) {
}

void EpaxosCoordinator::Submit(shared_ptr<Marshallable>& cmd,
                                   const function<void()>& func,
                                   const function<void()>& exe_callback) {
	std::lock_guard<std::recursive_mutex> lock(mtx_);
  verify(!in_submission_);
  verify(cmd_ == nullptr);
  in_submission_ = true;
  cmd_ = cmd;
  verify(cmd_->kind_ != MarshallDeputy::UNKNOWN);
  commit_callback_ = func;
  GotoNextPhase();
}

void EpaxosCoordinator::Commit() {
  std::lock_guard<std::recursive_mutex> lock(mtx_);
  commit_callback_();
  verify(phase_ == Phase::COMMIT);
  GotoNextPhase();
}

void EpaxosCoordinator::GotoNextPhase() {
  int n_phase = n_phase_;
  int current_phase = phase_ % n_phase;
  phase_++;
  switch (current_phase) {
    case Phase::INIT:
      verify(phase_ % n_phase == Phase::PREACCEPT);
    case Phase::PREACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
    case Phase::ACCEPT:
      verify(phase_ % n_phase == Phase::COMMIT);
      break;
    case Phase::COMMIT:
      break;
    default:
      verify(0);
  }
}

} // namespace janus