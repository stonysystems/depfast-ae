#pragma once

#include "../client_worker.h"
#include "perf_test.h"
#include "test.h"

namespace janus {

class EpaxosClientWorker : public ClientWorker {
  // SharedIntEvent n_tx_done_{};
  atomic<uint32_t> n_tx_done_;
  uint32_t n_tx_issued_{0};
  uint32_t n_concurrent_;
  uint32_t tot_req_num_;
  uint32_t conflict_perc_;
  uint32_t client_max_undone_{1500}; // 3R (non-thrifty = 500 and thrifty = 1500) 5R (non-thrifty = 1000 and thrifty = 1500)
  EpaxosTestConfig* testconfig_;

 public:
  using ClientWorker::ClientWorker;
  
  // This is called from a different thread.
  void Work() override {
    testconfig_ = new EpaxosTestConfig(EpaxosFrame::replicas_);  // todo: destroy

    #ifdef EPAXOS_PERF_TEST_CORO
    Print("START PERFORMANCE TESTS");
    n_concurrent_ = Config::GetConfig()->get_concurrent_txn();
    tot_req_num_ = Config::GetConfig()->get_tot_req();
    conflict_perc_ = Config::GetConfig()->get_conflict_perc();
    Print("Concurrent: %d, TotalRequests: %d, Conflict: %d", n_concurrent_, tot_req_num_, conflict_perc_);
    n_tx_done_ = 0;
    uint64_t start_rpc = testconfig_->RpcTotal();

    testconfig_->SetCustomLearnerAction([this](int svr) {
      return ([this, svr](Marshallable& cmd) {
        auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
        int leader = command.tx_id_ % NSERVERS;
        if (svr != leader) return;
        n_tx_done_++;
  //     // exit if not leader
  //     metrics_mtx_.lock();
  //     int cmd_leader = leader[command.tx_id_];
  //     metrics_mtx_.unlock();
  //     if (cmd_leader != svr) return;
  //     // measure req latency
  //     struct timeval t1;
  //     gettimeofday(&t1, NULL);
  //     metrics_mtx_.lock();
  //     pair<int, int> startime = start_time[command.tx_id_];
  //     leader_exec_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
  //     metrics_mtx_.unlock();
      });
    });
    
    testconfig_->SetCommittedLearnerAction([this](int svr) {
      return ([this, svr](Marshallable& cmd) {
        // auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
  //     // exit if not leader
  //     metrics_mtx_.lock();
  //     int cmd_leader = leader[command.tx_id_];
  //     metrics_mtx_.unlock();
  //     if (cmd_leader != svr) return;
  //     // measure req latency
  //     struct timeval t1;
  //     gettimeofday(&t1, NULL);
  //     metrics_mtx_.lock();
  //     pair<int, int> startime = start_time[command.tx_id_];
  //     leader_commit_times[command.tx_id_] = t1.tv_sec - startime.first + ((float)(t1.tv_usec - startime.second)) / 1000000;
  //     metrics_mtx_.unlock();
      });
    });

    for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
      auto sp_job = std::make_shared<OneTimeJob>([this, n_tx] () {
        // this wait tries to avoid launching clients all at once, especially for open-loop clients.
        Reactor::CreateSpEvent<NeverEvent>()->Wait(RandomGenerator::rand(0, 1000000));
        while (n_tx_issued_ < tot_req_num_) {
          auto n_undone_tx = n_tx_issued_ - n_tx_done_;
          while (client_max_undone_ > 0 && n_undone_tx > client_max_undone_) {
            Reactor::CreateSpEvent<NeverEvent>()->Wait(1000);
            n_undone_tx = n_tx_issued_ - n_tx_done_;
          }
          n_tx_issued_++;
          int cmd = n_tx_issued_;
          int svr = cmd % NSERVERS;
          string dkey = (RandomGenerator::rand(0, 100) < conflict_perc_) ? "0" : to_string(cmd);
          // struct timeval t2;
          // gettimeofday(&t2, NULL);
          // start_time[cmd] = {t2.tv_sec, t2.tv_usec};
          // leader[cmd] = svr;
          testconfig_->SendStart(svr, cmd, dkey);
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
    while (all_done_ == 0 || n_tx_done_ < tot_req_num_) {
      Log_info("wait for finish... n_ceased_cleints: %d, n_issued: %d, n_done: %d",
                (int) n_ceased_client_.value_, (int) n_tx_issued_,
                (int) n_tx_done_);
      sleep(1);
    }
    struct timeval t2;
    gettimeofday(&t2, NULL);
    int tot_exec_sec_ = t2.tv_sec - t1.tv_sec;
    int tot_exec_usec_ = t2.tv_usec - t1.tv_usec;
    float throughput = tot_req_num_ / (tot_exec_sec_ + ((float)tot_exec_usec_) / 1000000);
    int rpc_count = testconfig_->RpcTotal() - start_rpc;
    float fastpath_percentage = testconfig_->GetFastpathPercent();
    Print("Fastpath Percentage: %lf", fastpath_percentage);
    Print("Total RPC count: %ld", rpc_count);
    Print("Throughput: %lf", throughput);
    Log_info("PERF TEST COMPLETED");
    #endif

    #ifdef EPAXOS_TEST_CORO
    Coroutine::CreateRun([this] () {
      EpaxosTest test(testconfig_);
      test.Run();
      Coroutine::Sleep(10);
      testconfig_->Shutdown();
      Reactor::GetReactor()->looping_ = false;
    });
    Reactor::GetReactor()->Loop(true, true);
    #endif
  }
};

} // namespace janus
