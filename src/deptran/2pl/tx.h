#pragma once

#include "../classic/tx.h"

namespace janus {

using rrr::ALockGroup;

class Tx2pl: public TxClassic {
 public:
  vector<std::pair<ALock*, uint64_t>> locked_locks_ = {};
  bool woundable_{true};
  bool wounded_{false};
  int _debug_n_lock_requested_{0};
  int _debug_n_lock_granted_{0};

  Tx2pl(epoch_t epoch, txnid_t tid, TxLogServer *);
};

} // namespace janus
