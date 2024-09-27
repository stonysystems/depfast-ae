
#include "coordinator.h"
#include "frame.h"
#include "benchmark_control_rpc.h"

namespace janus {

/** thread safe */

// On client side, dispatch the transaction to the server (CoordinatorClassic::DispatchAsync()).
void CoordinatorNone::GotoNextPhase() {
  int n_phase = 2;
  switch (phase_++ % n_phase) {
    case Phase::INIT_END:
      DispatchAsync();
      verify(phase_ % n_phase == Phase::DISPATCH);
      break;
    case Phase::DISPATCH:
      committed_ = true;
      verify(phase_ % n_phase == Phase::INIT_END);
      End(); // Mark this request is completed on client side - 2.
      break;
    default:
      verify(0);
  }
}

} // namespace janus
