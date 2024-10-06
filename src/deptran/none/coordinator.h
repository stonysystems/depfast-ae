#pragma once

#include "deptran/classic/coordinator.h"
#include <chrono>

namespace janus {
class CoordinatorNone : public CoordinatorClassic {
 public:
  using CoordinatorClassic::CoordinatorClassic;
  enum Phase {INIT_END=0, DISPATCH=1};
  void GotoNextPhase() override;
  
  uint64_t dispatch_time_;

  uint64_t GetNowInns() {
    auto now = std::chrono::system_clock::now();
    auto now_ns = std::chrono::time_point_cast<std::chrono::nanoseconds>(now);
    auto value = now_ns.time_since_epoch();
    return (uint64_t) value.count();
  }
};
} // namespace janus
