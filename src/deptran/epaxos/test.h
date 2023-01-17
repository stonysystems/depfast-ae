#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

class EpaxosLabTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t index_;
  uint64_t init_rpcs_;

 public:
  EpaxosLabTest(EpaxosTestConfig *config) : config_(config), index_(1) {}
  int Run(void);
  void Cleanup(void);

 private:

  int testBasicAgree(void);
//   int testFailAgree(void);
//   int testFailNoAgree(void);
//   int testRejoin(void);
//   int testConcurrentStarts(void);
//   int testBackup(void);
//   int testCount(void);

//   int testUnreliableAgree(void);
//   int testFigure8(void);

  // void wait(uint64_t microseconds);

};

#endif

} // namespace janus
