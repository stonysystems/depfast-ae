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
  int testBasicAgree(void);
  int testFastPathIndependentAgree(void);
  int testFastPathDependentAgree(void);
  int testSlowPathIndependentAgree(void);
  int testSlowPathDependentAgree(void);
  int testFailNoQuorum(void);
  int testPrepareCommittedCommand(void);
  int testPrepareAcceptedCommand(void);
  int testPreparePreAcceptedCommand(void);
  int testPrepareNoopCommand(void);
  int testConcurrentAgree(void);
  int testConcurrentUnreliableAgree(void);

};

#endif

} // namespace janus
