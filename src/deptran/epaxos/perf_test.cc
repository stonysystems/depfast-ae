#include "perf_test.h"
#ifdef CPU_PROFILE
#include <gperftools/profiler.h>
#endif

namespace janus {


#ifdef EPAXOS_PERF_TEST_CORO

int EpaxosPerfTest::Run(void) {
  Print("START PERFORMANCE TESTS");
  int concurrent = Config::GetConfig()->get_concurrent_txn();
  int tot_req_num = Config::GetConfig()->get_tot_req();
  int conflicts = Config::GetConfig()->get_conflict_perc();
  config_->SetRepeatedLearnerAction(conflicts, concurrent, tot_req_num);
  uint64_t start_rpc = config_->RpcTotal();
  Log_info("Perf test args - concurrent: %d conflicts: %d tot_req_num: %d", concurrent, conflicts, tot_req_num);

  #ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  ProfilerStart(prof_file);
  #endif
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  EpaxosTestConfig *config_ = this->config_;
  vector<std::thread> ths;
  int svr = 0;
  string dkey;
  for (int i = 1; i <= concurrent; i++) {
    svr = svr % NSERVERS;
    dkey = ((rand() % 100) < conflicts) ? "0" : to_string(i);
    ths.push_back(std::thread([config_, i, dkey, conflicts, svr]() {
      uint64_t replica_id, instance_no;
      config_->Start(svr, i, dkey, &replica_id, &instance_no);
      Log_info("server %d submitted value %d", svr, i);
    }));
    svr++;
  }
  Log_info("waiting for submission threads.");
  for (auto& th : ths) {
    th.join();
  }
  while (1) {
    int executed_num = 0;
    for (int svr = 0; svr < NSERVERS; svr++) {
      executed_num += config_->GetExecutedCount(svr);
    }
    if (executed_num >= tot_req_num) {
      break;
    }
    Coroutine::Sleep(100000);
  }
  Log_info("execution done.");

  gettimeofday(&t2, NULL);
  int tot_sec_ = t2.tv_sec - t1.tv_sec;
  int tot_usec_ = t2.tv_usec - t1.tv_usec;
  #ifdef CPU_PROFILE
  ProfilerStop();
  #endif

  Print("PERFORMANCE TESTS COMPLETED");
  Print("Time consumed: %lf", tot_sec_ + ((float)tot_usec_) / 1000000);
  Print("Total RPC count: %ld", config_->RpcTotal() - start_rpc);
  return 0;
}

#endif

}
