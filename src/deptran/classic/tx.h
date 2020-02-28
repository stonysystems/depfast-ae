#pragma once

#include "../tx.h"

namespace janus {

class TxClassic: public Tx {
 public:
  using Tx::Tx;
  shared_ptr<BoxEvent<int>> prepare_result{Reactor::CreateSpEvent<BoxEvent<int>>()};
  shared_ptr<BoxEvent<int>> commit_result{Reactor::CreateSpEvent<BoxEvent<int>>()};
  bool is_leader_hint_{false};
};

} // namespace janus
