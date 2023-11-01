#include "perf_test.h"
#include <boost/asio/thread_pool.hpp>
#include <boost/asio/post.hpp>
#ifdef CPU_PROFILE
#include <gperftools/profiler.h>
#endif

namespace janus {


#ifdef EPAXOS_PERF_TEST_CORO
int EpaxosPerfTest::enter(int svr) {
  std::unique_lock<std::mutex> lock(finish_mtx_);
  while (inprocess_reqs[svr] >= concurrent) {
    cv[svr].wait(lock);
  }
  if (submitted_count == tot_req_num) {
    return -1;
  }
  inprocess_reqs[svr]++;
  return ++submitted_count;
}

void EpaxosPerfTest::leave(int svr) {
  std::unique_lock<std::mutex> lock(finish_mtx_);
  inprocess_reqs[svr]--;
  if (inprocess_reqs[svr] < concurrent) {
    cv[svr].notify_one();
  }
}

int EpaxosPerfTest::Run(void) {
  #ifdef EPAXOS_EVENTUAL_TEST
  config_->PauseExecution(true);
  #endif

  Print("START PERFORMANCE TESTS");
  concurrent = Config::GetConfig()->get_concurrent_txn();
  tot_req_num = Config::GetConfig()->get_tot_req();
  conflict_perc = Config::GetConfig()->get_conflict_perc();
  Print("Concurrent: %d, TotalRequests: %d, Conflict: %d", concurrent, tot_req_num, conflict_perc);

  submitted_count = 0;
  config_->SetCustomLearnerAction([this](int svr) {
    return ([this, svr](Marshallable& cmd) {
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      // exit if not leader
      finish_mtx_.lock();
      int cmd_leader = leader[command.tx_id_];
      finish_mtx_.unlock();
      if (cmd_leader != svr) return;
      // measure req latency
      struct timeval t1;
      gettimeofday(&t1, NULL);
      finish_mtx_.lock();
      pair<int, int> startime = start_time[command.tx_id_];
      leader_exec_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
      finish_mtx_.unlock();
      // mark req as finished
      leave(svr);
      // mark svr as done (if all done) 
      if (submitted_count == tot_req_num) {
        finish_mtx_.lock();
        if (inprocess_reqs[svr] == 0) {
          finished_count++;
          if (finished_count == NSERVERS) {
            finish_cond_.notify_all();
          }
        }
        finish_mtx_.unlock();
      }
    });
  });
  
  config_->SetCommittedLearnerAction([this](int svr) {
    return ([this, svr](Marshallable& cmd) {
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      // exit if not leader
      finish_mtx_.lock();
      int cmd_leader = leader[command.tx_id_];
      finish_mtx_.unlock();
      if (cmd_leader != svr) return;
      // measure req latency
      struct timeval t1;
      gettimeofday(&t1, NULL);
      finish_mtx_.lock();
      pair<int, int> startime = start_time[command.tx_id_];
      leader_commit_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
      finish_mtx_.unlock();
      #ifdef EPAXOS_EVENTUAL_TEST
      // mark req as finished
      leave(svr);
      #endif
    });
  });

  uint64_t start_rpc = config_->RpcTotal();
  int threads = NSERVERS;
  boost::asio::thread_pool pool(threads);
  Log_info("Perf test args - concurrent: %d conflicts: %d tot_req_num: %d", concurrent, conflict_perc, tot_req_num);

  #ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  ProfilerStart(prof_file);
  #endif
  struct timeval t1, t2;
  gettimeofday(&t1, NULL);
  for (int svr = 0; svr < NSERVERS; svr++) {
    boost::asio::post(pool, [this, svr]() {
      while (submitted_count < tot_req_num) {
        int next_cmd = enter(svr);
        if (next_cmd == -1) break;
        string dkey = ((rand() % 100) < conflict_perc) ? "0" : to_string(next_cmd);
        // mark start time
        finish_mtx_.lock();
        struct timeval t2;
        gettimeofday(&t2, NULL);
        start_time[next_cmd] = {t2.tv_sec, t2.tv_usec};
        leader[next_cmd] = svr;
        finish_mtx_.unlock();
        // start
        config_->Start(svr, next_cmd, dkey);
      }
    });
  }
  Log_info("waiting for submission threads.");
  pool.join();

  std::unique_lock<std::mutex> lk(finish_mtx_);
  finish_cond_.wait(lk);
  lk.unlock();
  gettimeofday(&t2, NULL);
  int tot_exec_sec_ = t2.tv_sec - t1.tv_sec;
  int tot_exec_usec_ = t2.tv_usec - t1.tv_usec;
  Log_info("execution done.");
  #ifdef CPU_PROFILE
  ProfilerStop();
  #endif
  
  Print("PERFORMANCE TESTS COMPLETED");
  vector<float> exec_times;
  vector<float> commit_times;
  ofstream out_file;
  out_file.open("./plots/epaxos/latencies_" + to_string(concurrent) + "_"  + to_string(tot_req_num) + "_" + to_string(conflict_perc) + ".csv");
  for (pair<int, float> t : leader_exec_times) {
    exec_times.push_back(t.second);
    out_file << t.second << ",";
  }
  out_file << endl;
  for (pair<int, float> t : leader_commit_times) {
    commit_times.push_back(t.second);
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
  sort(commit_times.begin(), commit_times.end());
  sort(exec_times.begin(), exec_times.end());
  Print("Commit Latency p50: %lf, p90: %lf, p99: %lf, max: %lf", 
  commit_times[(commit_times.size() - 1) * 0.5],
  commit_times[(commit_times.size() - 1) * 0.9],
  commit_times[(commit_times.size() - 1) * 0.99],
  commit_times[commit_times.size() - 1]);
  #ifndef EPAXOS_EVENTUAL_TEST
  Print("Execution Latency p50: %lf, p90: %lf, p99: %lf, max: %lf", 
  exec_times[(exec_times.size() - 1) * 0.5],
  exec_times[(exec_times.size() - 1) * 0.9],
  exec_times[(exec_times.size() - 1) * 0.99],
  exec_times[exec_times.size() - 1]);
  #endif
  Print("Fastpath Percentage: %lf", fastpath_percentage);
  Print("Total RPC count: %ld", rpc_count);
  Print("Throughput: %lf", throughput);
  return 0;
}

#endif

}
