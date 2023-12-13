#pragma once

#include "../client_worker.h"
#include "../classic/tpc_command.h"
#include "frame.h"
#include <random>

#ifdef CPU_PROFILE
#include <gperftools/profiler.h>
#endif

#ifdef EPAXOS_TEST_CORO
#include "test.h"
#endif

namespace janus {

#define Print(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)

class EpaxosClientWorker : public ClientWorker {
  atomic<int64_t> n_done_tx_;
  uint32_t n_concurrent_;
  uint32_t tot_req_num_;
  uint32_t conflict_perc_;
  uint32_t client_max_undone_{1500}; // 3R (non-thrifty = 500 and thrifty = 1500) 5R (non-thrifty = 1000 and thrifty = 1500)
  bool_t metrics_collection_done_{0};
  #ifdef EPAXOS_TEST_CORO
  EpaxosTestConfig* testconfig_;
  #endif

 public:
  using ClientWorker::ClientWorker;
  
  // This is called from a different thread.
  void Work() override {
    #ifdef EPAXOS_TEST_CORO
    testconfig_ = new EpaxosTestConfig(EpaxosFrame::replicas_);  // todo: destroy
    Coroutine::CreateRun([this] () {
      EpaxosTest test(testconfig_);
      test.Run();
      Coroutine::Sleep(10);
      testconfig_->Shutdown();
      Reactor::GetReactor()->looping_ = false;
    });
    Reactor::GetReactor()->Loop(true, true);
    #else
    n_done_tx_ = 0;
    Print("START PERFORMANCE TESTS");
    n_concurrent_ = Config::GetConfig()->get_concurrent_txn();
    tot_req_num_ = Config::GetConfig()->get_tot_req();
    conflict_perc_ = Config::GetConfig()->get_conflict_perc();
    Print("Concurrent: %d, TotalRequests: %d, Conflict: %d", n_concurrent_, tot_req_num_, conflict_perc_);

    // Precompute command leader, command and dependency-key
    vector<int> cmd_leader(tot_req_num_);
    vector<shared_ptr<Marshallable>> cmds(tot_req_num_);
    vector<string> dkeys(tot_req_num_);
    random_device rd;
    mt19937 gen(rd());
    uniform_int_distribution<int> leader_distribution(0, NSERVERS - 1);
    uniform_int_distribution<int> conflict_distribution(0, 99);
    for (int i = 0; i < cmd_leader.size(); i++) {
      // Construct an empty TpcCommitCommand containing cmd as its tx_id_
      auto cmdptr = std::make_shared<TpcCommitCommand>();
      auto vpd_p = std::make_shared<VecPieceData>();
      vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
      cmdptr->tx_id_ = i;
      cmdptr->cmd_ = vpd_p;
      auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
      cmd_leader[i] = i % NSERVERS;
      cmds[i] = cmdptr_m;
      dkeys[i] = (conflict_distribution(gen) < conflict_perc_) ? "HOT_KEY" : to_string(i);
    }

    #ifdef CPU_PROFILE
    char prof_file[1024];
    Config::GetConfig()->GetProfilePath(prof_file);
    ProfilerStart(prof_file);
    #endif

    for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
      auto sp_job = std::make_shared<OneTimeJob>([this, n_tx, &cmd_leader, &cmds, &dkeys] () {
        // this wait tries to avoid launching clients all at once, especially for open-loop clients.
        Reactor::CreateSpEvent<NeverEvent>()->Wait(RandomGenerator::rand(0, 1000000));
        while (n_tx_issued_ < tot_req_num_) {
          auto n_undone_tx = n_tx_issued_ - n_done_tx_;
          while (client_max_undone_ > 0 && n_undone_tx > client_max_undone_) {
            Reactor::CreateSpEvent<NeverEvent>()->Wait(1000);
            n_undone_tx = n_tx_issued_ - n_done_tx_;
          }
          if (n_tx_issued_ >= tot_req_num_) break;
          int svr = cmd_leader[n_tx_issued_];
          string dkey = dkeys[n_tx_issued_];
          auto cmd = cmds[n_tx_issued_];
          n_tx_issued_++;
          EpaxosCommo* epaxos_commo = dynamic_cast<EpaxosCommo*>(commo_);
          epaxos_commo->SendStart(svr, 0, cmd, dkey, [this]() {
            n_done_tx_++;
          });
        }
        n_ceased_client_.Set(n_ceased_client_.value_+1);
      });
      poll_mgr_->add(dynamic_pointer_cast<Job>(sp_job));
    }

    struct timeval t1;
    gettimeofday(&t1, NULL);
    poll_mgr_->add(dynamic_pointer_cast<Job>(std::make_shared<OneTimeJob>([this](){
      Log_info("wait for all virtual clients to stop issuing new requests.");
      n_ceased_client_.WaitUntilGreaterOrEqualThan(n_concurrent_, (duration+500)*1000000);
      all_done_ = 1;
    })));

    while (all_done_ == 0 || n_done_tx_ < tot_req_num_) {
      Log_info("wait for finish... n_ceased_cleints: %d, n_issued: %d, n_done: %d",
                (int) n_ceased_client_.value_, (int) n_tx_issued_, (int) sp_n_tx_done_.value_);
      sleep(1);
    }

    #ifdef CPU_PROFILE
    ProfilerStop();
    #endif

    // Print Throughput
    struct timeval t2;
    gettimeofday(&t2, NULL);
    int tot_exec_sec_ = t2.tv_sec - t1.tv_sec;
    int tot_exec_usec_ = t2.tv_usec - t1.tv_usec;
    float throughput = tot_req_num_ / (tot_exec_sec_ + ((float)tot_exec_usec_) / 1000000);
    Print("Throughput: %lf", throughput);
    // Collect server-side metrics
    uint64_t fast = 0;
    vector<double> commit_times;
    vector<double> exec_times;
    auto sp_metrics_job = std::make_shared<OneTimeJob>([this, &fast, &commit_times, &exec_times] () mutable {
      EpaxosCommo* epaxos_commo = dynamic_cast<EpaxosCommo*>(commo_);
      for (int svr = 0; svr < NSERVERS; svr++) {
        uint64_t fast_;
        vector<double> commit_times_;
        vector<double> exec_times_;
        auto ev = epaxos_commo->CollectMetrics(svr, 0, &fast_, &commit_times_, &exec_times_);
        ev->Wait(1000000);
        fast += fast_;
        commit_times.insert(commit_times.end(), commit_times_.begin(), commit_times_.end());
        exec_times.insert(exec_times.end(), exec_times_.begin(), exec_times_.end());
      }
      metrics_collection_done_ = 1;
    });
    poll_mgr_->add(dynamic_pointer_cast<Job>(sp_metrics_job));
    while (metrics_collection_done_ == 0) {
      sleep(1);
    }
    // Print fast-path percentage
    float fastpath_percentage = ((float) fast * 100) / tot_req_num_;
    Print("Fastpath Percentage: %lf", fastpath_percentage);
    // Print latency percentiles
    if (commit_times.size() != 0) {
      sort(commit_times.begin(), commit_times.end());
      sort(exec_times.begin(), exec_times.end());
      Print("Commit Latency p50: %lf, p90: %lf, p99: %lf, max: %lf", 
      commit_times[(commit_times.size() - 1) * 0.5],
      commit_times[(commit_times.size() - 1) * 0.9],
      commit_times[(commit_times.size() - 1) * 0.99],
      commit_times[commit_times.size() - 1]);
      Print("Execution Latency p50: %lf, p90: %lf, p99: %lf, max: %lf", 
      exec_times[(exec_times.size() - 1) * 0.5],
      exec_times[(exec_times.size() - 1) * 0.9],
      exec_times[(exec_times.size() - 1) * 0.99],
      exec_times[exec_times.size() - 1]);
    }
    // Write everything to file
    ofstream out_file;
    out_file.open("./plots/epaxos/latencies_" + to_string(n_concurrent_) + "_"  + to_string(tot_req_num_) + "_" + to_string(conflict_perc_) + ".csv"); 
    for (auto t : exec_times) {
      out_file << t << ",";
    }
    out_file << endl;
    for (auto t : commit_times) {
      out_file << t << ",";
    }
    out_file << endl;
    out_file << throughput << endl;
    out_file << fastpath_percentage << endl;
    out_file.close();

    Log_info("PERF TEST COMPLETED");
    #endif
  }
};

} // namespace janus
