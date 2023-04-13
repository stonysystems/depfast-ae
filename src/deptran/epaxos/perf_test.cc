#include "perf_test.h"
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#ifdef CPU_PROFILE
#include <gperftools/profiler.h>
#endif

namespace janus {


#ifdef EPAXOS_PERF_TEST_CORO

int EpaxosPerfTest::Run(void) {
  Print("START PERFORMANCE TESTS");
  concurrent = Config::GetConfig()->get_concurrent_txn();
  tot_req_num = Config::GetConfig()->get_tot_req();
  conflict_perc = Config::GetConfig()->get_conflict_perc();
  submitted_count = 0;
  config_->SetCustomLearnerAction([this](int svr) {
    return ([this, svr](Marshallable& cmd) {
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      finish_mtx_.lock();
      int cmd_leader = leader[command.tx_id_];
      finish_mtx_.unlock();
      if (cmd_leader != svr) return;
      struct timeval t1;
      gettimeofday(&t1, NULL);
      finish_mtx_.lock();
      pair<int, int> startime = start_time[command.tx_id_];
      leader_exec_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
      finish_mtx_.unlock();
      // start next command
      if (submitted_count >= tot_req_num) {
        if (config_->GetRequestCount(svr) == 0) {
          finish_mtx_.lock();
          finished_count++;
          if (finished_count == NSERVERS) {
            finish_cond_.notify_all();
          }
          finish_mtx_.unlock();
        }
        return;
      };
      if (config_->GetRequestCount(svr) >= concurrent) return;
      int next_cmd = ++submitted_count;
      string dkey = ((rand() % 100) < conflict_perc) ? "0" : to_string(next_cmd);
      struct timeval t2;
      gettimeofday(&t2, NULL);
      finish_mtx_.lock();
      start_time[next_cmd] = {t2.tv_sec, t2.tv_usec};
      leader[next_cmd] = svr;
      finish_mtx_.unlock();
      config_->Start(svr, next_cmd, dkey);
    });
  });
  
  config_->SetCommittedLearnerAction([this](int svr) {
    return ([this, svr](Marshallable& cmd) {
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      finish_mtx_.lock();
      int cmd_leader = leader[command.tx_id_];
      finish_mtx_.unlock();
      if (cmd_leader != svr) return;
      struct timeval t1;
      gettimeofday(&t1, NULL);
      finish_mtx_.lock();
      pair<int, int> startime = start_time[command.tx_id_];
      leader_commit_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
      finish_mtx_.unlock();
    });
  });

  uint64_t start_rpc = config_->RpcTotal();
  int threads = min(concurrent, 100);
  boost::asio::thread_pool pool(threads);
  Log_info("Perf test args - concurrent: %d conflicts: %d tot_req_num: %d", concurrent, conflict_perc, tot_req_num);

  #ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  ProfilerStart(prof_file);
  #endif
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  int svr = 0;
  int cmd;
  string dkey;
  for (int i = 1; i <= concurrent; i++) {
    svr = svr % NSERVERS;
    cmd = ++submitted_count;
    dkey = ((rand() % 100) < conflict_perc) ? "0" : to_string(cmd);
    boost::asio::post(pool, [this, cmd, dkey, svr]() {
      struct timeval t;
      gettimeofday(&t, NULL);
      finish_mtx_.lock();
      start_time[cmd] = {t.tv_sec, t.tv_usec};
      leader[cmd] = svr;
      finish_mtx_.unlock();
      config_->Start(svr, cmd, dkey);
    });
    svr++;
  }
  Log_info("waiting for submission threads.");
  pool.join();

  std::unique_lock<std::mutex> lk(finish_mtx_);
  finish_cond_.wait(lk);
  gettimeofday(&t2, NULL);
  int tot_exec_sec_ = t2.tv_sec - t1.tv_sec;
  int tot_exec_usec_ = t2.tv_usec - t1.tv_usec;
  Log_info("execution done.");
  #ifdef CPU_PROFILE
  ProfilerStop();
  #endif
  
  Print("PERFORMANCE TESTS COMPLETED");
  ofstream out_file;
  out_file.open("./plots/epaxos/latencies_" + to_string(concurrent) + "_"  + to_string(tot_req_num) + "_" + to_string(conflict_perc) + ".csv");
  for (pair<int, float> t : leader_exec_times) {
    out_file << t.second << ",";
  }
  out_file << endl;
  for (pair<int, float> t : leader_commit_times) {
    out_file << t.second << ",";
  }
  out_file << endl;
  float throughput = tot_req_num / (tot_exec_sec_ + ((float)tot_exec_usec_) / 1000000);
  int rpc_count = config_->RpcTotal() - start_rpc;
  float fastpath_percentage = config_->GetFastpathPercent();
  out_file << throughput << endl;
  out_file << fastpath_percentage << endl;
  out_file << rpc_count << endl;
  out_file.close();
  Print("Fastpath Percentage: %lf", fastpath_percentage);
  Print("Total RPC count: %ld", rpc_count);
  Print("Throughput: %lf", throughput);
  return 0;
}

#endif

}
