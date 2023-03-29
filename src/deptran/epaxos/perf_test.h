#pragma once

#include "testconf.h"

namespace janus {

#ifdef EPAXOS_PERF_TEST_CORO

class PercentileCalculator {
 private:
  vector<float> values;
 public:
  PercentileCalculator(vector<float> values) {
    sort(values.begin(), values.end());
    this->values = values;
  }

  float CalculateNthPercentile(int n) {
    return values[((values.size() - 1) * n) / 100];
  }
};

class EpaxosPerfTest {

 private:
  EpaxosTestConfig *config_;
  uint64_t init_rpcs_;
  int submitted_count = 0;
  int finished_count = 0;
  std::mutex finish_mtx_;
  std::condition_variable finish_cond_;
  unordered_map<int, pair<int, int>> start_time;
  vector<float> res_times;
  atomic<int> next{0};

 public:
  EpaxosPerfTest(EpaxosTestConfig *config) : config_(config) {}
  int Run(void);

};

#endif

} // namespace janus
