#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

class EpaxosTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t init_rpcs_;
  uint64_t cmd = 0;

 public:
  EpaxosTest(EpaxosTestConfig *config) : config_(config) {}
  int Run(void);

 private:
  // Pre-accept/Accept/Commit tests
  int testBasicAgree(void);
  int testFastPathIndependentAgree(void);
  int testFastPathDependentAgree(void);
  int testSlowPathIndependentAgree(void);
  int testSlowPathDependentAgree(void);
  int testNonIdenticalAttrsAgree(void);
  int testFailNoQuorum(void);
  // Prepare tests
  int testPrepareCommittedCommandAgree(void);
  int testPrepareAcceptedCommandAgree(void);
  int testPreparePreAcceptedCommandAgree(void);
  int testPrepareNoopCommandAgree(void);
  int testConcurrentAgree(void);
  int testConcurrentUnreliableAgree(void);

};

#endif

} // namespace janus
