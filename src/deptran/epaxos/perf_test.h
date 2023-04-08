#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_PERF_TEST_CORO

class EpaxosPerfTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t init_rpcs_;
  int submitted_count = 0;
  int finished_count = 0;
  std::mutex finish_mtx_;
  std::condition_variable finish_exec_cond_;
  unordered_map<int, pair<int, int>> start_time;
  unordered_map<int, float> min_exec_times;
  unordered_map<int, float> max_exec_times;
  unordered_map<int, float> leader_exec_times;
  unordered_map<int, float> leader_commit_times;
  unordered_map<int, int> leader;

 public:
  EpaxosPerfTest(EpaxosTestConfig *config) : config_(config) {}
  int Run(void);

};

#endif

} // namespace janus
