#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

class EpaxosLabTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t init_rpcs_;

 public:
  EpaxosLabTest(EpaxosTestConfig *config) : config_(config) {}
  int Run(void);
  void Cleanup(void);

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
  int testConcurrentUnreliableAgree1(void);
  int testConcurrentUnreliableAgree2(void);

};

#endif

} // namespace janus
