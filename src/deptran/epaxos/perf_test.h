#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_PERF_TEST_CORO

class EpaxosPerfTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t init_rpcs_;

 public:
  EpaxosPerfTest(EpaxosTestConfig *config) : config_(config) {}
  int Run(void);

};

#endif

} // namespace janus
