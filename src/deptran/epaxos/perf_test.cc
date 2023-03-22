#include "perf_test.h"

namespace janus {


#ifdef EPAXOS_PERF_TEST_CORO

int EpaxosPerfTest::Run(void) {
  Print("START PERFORMANCE TESTS");
  config_->SetLearnerAction();
  uint64_t start_rpc = config_->RpcTotal();
  int concurrent = Config::GetConfig()->get_concurrent_txn();
  vector<string> commands(concurrent);
  for (int i = 0; i < concurrent; i++) {
    commands[i] = to_string(i+1);
  }
  int tot_num = concurrent * NSERVERS;

  #ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  ProfilerStart(prof_file);
  #endif
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  EpaxosTestConfig *config_ = this->config_;
  vector<std::thread> ths;
  for (int i = 0; i < concurrent; i++) {
    for (int svr = 0; svr < NSERVERS; svr++) {
      ths.push_back(std::thread([config_, commands, i, svr]() {
        uint64_t replica_id, instance_no;
        config_->Start(svr, i+1, commands[i], &replica_id, &instance_no);
      }));
    }
  }
  Log_info("waiting for submission threads.");
  for (auto& th : ths) {
    th.join();
  }
  while (1) {
    bool flag = true;
    for (int svr = 0; svr < NSERVERS; svr++) {
      if (config_->GetExecutedCommands(svr).size() < tot_num)
        flag = false;
    }
    if (flag) {
      break;
    }
    Log_info("execution going.");
    Coroutine::Sleep(10);
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
