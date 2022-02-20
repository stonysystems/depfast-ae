#include <cmath>
#include "client_worker.h"
#include "frame.h"
#include "procedure.h"
#include "coordinator.h"
#include "workload.h"
#include "benchmark_control_rpc.h"

namespace janus {

ClientWorker::~ClientWorker() {
  if (tx_generator_) {
    delete tx_generator_;
  }
  for (auto c : created_coordinators_) {
    delete c;
  }
//  dispatch_pool_->release();
}

void ClientWorker::ForwardRequestDone(Coordinator* coo,
                                      TxReply* output,
                                      DeferredReply* defer,
                                      TxReply& txn_reply) {
  verify(coo != nullptr);
  verify(output != nullptr);

  *output = txn_reply;

  bool have_more_time = timer_->elapsed() < duration;
  if (have_more_time && config_->client_type_ == Config::Open) {
    std::lock_guard<std::mutex> lock(coordinator_mutex);
    coo->forward_status_ = NONE;
    free_coordinators_.push_back(coo);
  } else if (!have_more_time) {
    Log_debug("times up. stop.");
    Log_debug("n_concurrent_ = %d", n_concurrent_);
//    finish_mutex.lock();
    n_concurrent_--;
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
//      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
    }
//    finish_mutex.unlock();
  }

  defer->reply();
}



void ClientWorker::RequestDone(Coordinator* coo, TxReply& txn_reply) {
  verify(0);
  verify(coo != nullptr);

  if (txn_reply.res_ == SUCCESS)
    success++;
  num_txn++;
  num_try.fetch_add(txn_reply.n_try_);

  bool have_more_time = timer_->elapsed() < duration;
  Log_debug("received callback from tx_id %" PRIx64, txn_reply.tx_id_);
  Log_debug("elapsed: %2.2f; duration: %d", timer_->elapsed(), duration);
  if (have_more_time && config_->client_type_ == Config::Open) {
    std::lock_guard<std::mutex> lock(coordinator_mutex);
    free_coordinators_.push_back(coo);
  } else if (have_more_time && config_->client_type_ == Config::Closed) {
    Log_debug("there is still time to issue another request. continue.");
    Coroutine::CreateRun([this,coo]() { 
      DispatchRequest(coo); 
    });
  } else if (!have_more_time) {
    Log_debug("times up. stop.");
    Log_debug("n_concurrent_ = %d", n_concurrent_);
//    finish_mutex.lock();
    if (coo->offset_ == 0) {
      *failover_server_quit_ = true;
    }
    n_concurrent_--;
    n_pause_concurrent_[coo->coo_id_] = true;
    verify(n_concurrent_ >= 0);
    if (n_concurrent_ == 0) {
      Log_debug("all coordinators finished... signal done");
//      finish_cond.signal();
    } else {
      Log_debug("waiting for %d more coordinators to finish", n_concurrent_);
      Log_debug("transactions they are processing:");
      // for debug purpose, print ongoing transaction ids.
      for (auto c : created_coordinators_) {
        if (c->ongoing_tx_id_ > 0) {
          Log_debug("\t %" PRIx64, c->ongoing_tx_id_);
        }
      }
    }
//    finish_mutex.unlock();
  } else {
    verify(0);
  }
}

Coordinator* ClientWorker::FindOrCreateCoordinator() {
  std::lock_guard<std::mutex> lock(coordinator_mutex);

  Coordinator* coo = nullptr;

  if (!free_coordinators_.empty()) {
    coo = dynamic_cast<Coordinator*>(free_coordinators_.back());
    free_coordinators_.pop_back();
  } else {
    if (created_coordinators_.size() == UINT16_MAX) {
      return nullptr;
    }
    verify(created_coordinators_.size() <= UINT16_MAX);
    coo = CreateCoordinator(created_coordinators_.size());
  }

  verify(!coo->_inuse_);
  coo->_inuse_ = true;
  return coo;
}

Coordinator* ClientWorker::CreateFailCtrlCoordinator() {

  cooid_t coo_id = cli_id_;
  uint64_t offset_id = 1000000 ; // TODO temp value
  coo_id = (coo_id << 16) + offset_id;
  auto coo = frame_->CreateCoordinator(coo_id,
                                       config_,
                                       benchmark,
                                       ccsi,
                                       id,
                                       txn_reg_);
  coo->loc_id_ = my_site_.locale_id;
  coo->commo_ = commo_;
  coo->forward_status_ = forward_requests_to_leader_ ? FORWARD_TO_LEADER : NONE;
  coo->offset_ = offset_id ;
  Log_debug("coordinator %d created at site %d: forward %d",
            coo->coo_id_,
            this->my_site_.id,
            coo->forward_status_);
  return coo ;
}


Coordinator* ClientWorker::CreateCoordinator(uint16_t offset_id) {

  cooid_t coo_id = cli_id_;
  coo_id = (coo_id << 16) + offset_id;
  auto coo = frame_->CreateCoordinator(coo_id,
                                       config_,
                                       benchmark,
                                       ccsi,
                                       id,
                                       txn_reg_);
  coo->loc_id_ = my_site_.locale_id;
  coo->commo_ = commo_;
  coo->forward_status_ = forward_requests_to_leader_ ? FORWARD_TO_LEADER : NONE;
  coo->offset_ = offset_id;
  Log_debug("coordinator %d created at site %d: forward %d",
            coo->coo_id_,
            this->my_site_.id,
            coo->forward_status_);
  created_coordinators_.push_back(coo);
  n_pause_concurrent_[coo_id] = false;
  return coo;
}

void ClientWorker::Work() {
  Log_debug("%s: %d", __FUNCTION__, this->cli_id_);
  txn_reg_ = std::make_shared<TxnRegistry>();
  verify(config_ != nullptr);
  Workload* workload = Workload::CreateWorkload(config_);
  workload->txn_reg_ = txn_reg_;
  workload->RegisterPrecedures();

  commo_->WaitConnectClientLeaders();
  if (ccsi) {
    ccsi->wait_for_start(id);
  }
  Log_debug("after wait for start");

  bool failover = Config::GetConfig()->get_failover();
  if (failover) {
    auto p_job = (Job*)new OneTimeJob([this]() {
      int run_int = Config::GetConfig()->get_failover_run_interval() * pow(10, 6);
      int stop_int = Config::GetConfig()->get_failover_stop_interval() * pow(10, 6);
      int wait_int = 50 * pow(10, 3);

      if (!fail_ctrl_coo_) {
        fail_ctrl_coo_ = CreateFailCtrlCoordinator();
      }
      locid_t idx = 0;
      while (!*failover_server_quit_) {
        auto r = Reactor::CreateSpEvent<NeverEvent>();
        r->Wait(run_int);
        *failover_trigger_ = true;
        while (*failover_trigger_) {
          auto e = Reactor::CreateSpEvent<NeverEvent>();
          e->Wait(wait_int);
          if (*failover_server_quit_) return;
        }
        Pause(idx);
        *failover_trigger_ = true;
        Log_info("server %d paused for failover test", idx);
        auto s = Reactor::CreateSpEvent<NeverEvent>();
        s->Wait(stop_int);
        while (*failover_trigger_) {
          auto e = Reactor::CreateSpEvent<NeverEvent>();
          e->Wait(wait_int);
          if (*failover_server_quit_) return;
        }
        Resume(idx);
        Log_info("server %d resumed for failover test", idx);
        // set the new leader
        idx = cur_leader_;
      }
    });
    shared_ptr<Job> sp_job(p_job);
    poll_mgr_->add(sp_job);
  }
  for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
    auto sp_job = std::make_shared<OneTimeJob>([this, n_tx] () {
      // this wait tries to avoid launching clients all at once, especially for open-loop clients.
      Reactor::CreateSpEvent<NeverEvent>()->Wait(RandomGenerator::rand(0, 1000000));
      auto beg_time = Time::now() ;
      auto end_time = beg_time + duration * pow(10, 6);
      while (true) {
        auto cur_time = Time::now(); // optimize: this call is not scalable.
        if (cur_time > end_time) {
          break;
        }
        n_tx_issued_++;
        while (true) {
          auto n_undone_tx = n_tx_issued_ - sp_n_tx_done_.value_;
          if (n_undone_tx % 1000 == 0) {
            Log_debug("unfinished tx %d", n_undone_tx);
          }
          if (config_->client_max_undone_ > 0
              && n_undone_tx > config_->client_max_undone_) {
            Reactor::CreateSpEvent<NeverEvent>()->Wait(pow(10, 4));
          } else {
            break;
          }
        }
        num_txn++;
        auto coo = FindOrCreateCoordinator();
        verify(!coo->sp_ev_commit_);
        verify(!coo->sp_ev_done_);
        coo->sp_ev_commit_ = Reactor::CreateSpEvent<IntEvent>();
        coo->sp_ev_done_ = Reactor::CreateSpEvent<IntEvent>();

				Log_debug("Dispatching request for %d", n_tx);
				this->outbound++;
				
				bool first = true;
				while(coo->commo_->paused){
					if(first){
						coo->commo_->count_lock_.lock();
						coo->commo_->total_ = this->outbound;
						coo->commo_->qe->n_voted_yes_ = this->outbound;
						coo->commo_->count_lock_.unlock();
						Log_info("is it ready: %d", coo->commo_->qe->IsReady());
						coo->commo_->qe->Test();
						first = false;
					}
					Log_info("total: %d", coo->commo_->total_);
					auto t = Reactor::CreateSpEvent<TimeoutEvent>(0.1*1000*1000);
					t->Wait(0.1*1000*1000);
				}
        
				this->DispatchRequest(coo);
        if (config_->client_type_ == Config::Closed) {
          auto ev = coo->sp_ev_commit_;
          ev->Wait(600*1000*1000);
					this->outbound--;
          verify(ev->status_ != Event::TIMEOUT);
        } else {
          auto sp_event = Reactor::CreateSpEvent<NeverEvent>();
          sp_event->Wait(pow(10, 6));
        }
        Coroutine::CreateRun([this, coo](){
          verify(coo->_inuse_);
          auto ev = coo->sp_ev_done_;
          ev->Wait();
          verify(coo->coo_id_ > 0);
//          ev->Wait(400*1000*1000);
          verify(coo->_inuse_);
          verify(coo->coo_id_ > 0);
          verify(ev->status_ != Event::TIMEOUT);
          if (coo->committed_) {
            success++;
          }
          sp_n_tx_done_.Set(sp_n_tx_done_.value_+1);
          num_try.fetch_add(coo->n_retry_);
          coo->sp_ev_done_.reset();
          coo->sp_ev_commit_.reset();
          free_coordinators_.push_back(coo);
          coo->_inuse_ = false;
          n_pause_concurrent_[coo->coo_id_] = true;
        });
      }
      n_ceased_client_.Set(n_ceased_client_.value_+1);
    });
    poll_mgr_->add(dynamic_pointer_cast<Job>(sp_job));
  }
  poll_mgr_->add(dynamic_pointer_cast<Job>(std::make_shared<OneTimeJob>([this](){
    Log_info("wait for all virtual clients to stop issuing new requests.");
    n_ceased_client_.WaitUntilGreaterOrEqualThan(n_concurrent_,
                                                 (duration+500)*1000000);
    Log_info("wait for all outstanding requests to finish.");
    // TODO uncomment this, otherwise many requests are still outstanding there.
    sp_n_tx_done_.WaitUntilGreaterOrEqualThan(n_tx_issued_);
    // for debug purpose
//    Reactor::CreateSpEvent<NeverEvent>()->Wait(5*1000*1000);
    *failover_server_quit_ = true;
    all_done_ = 1;
  })));

  while (all_done_ == 0) {
    // TODO: yidawu comment for test
    Log_info("wait for finish... n_ceased_cleints: %d,  "
              "n_issued: %d, n_done: %d, n_created_coordinator: %d",
              (int) n_ceased_client_.value_, (int) n_tx_issued_,
              (int) sp_n_tx_done_.value_, (int) created_coordinators_.size());
    sleep(1);
  }

  if (failover_server_quit_ && !*failover_server_quit_) {
    *failover_server_quit_ = true;
  }

  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u, Tput %d\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration(),
           num_txn.load() / Config::GetConfig()->get_duration());
  fflush(stderr);
  fflush(stdout);

  if (ccsi) {
    Log_info("%s: wait_for_shutdown at client %d", __FUNCTION__, cli_id_);
    ccsi->wait_for_shutdown();
  }
  delete timer_;
  return;
}

/*
void ClientWorker::Work() {
  Log_debug("%s: %d", __FUNCTION__, this->cli_id_);
  txn_reg_ = std::make_shared<TxnRegistry>();
  verify(config_ != nullptr);
  Workload* workload = Workload::CreateWorkload(config_);
  workload->txn_reg_ = txn_reg_;
  workload->RegisterPrecedures();

  commo_->WaitConnectClientLeaders();
  if (ccsi) {
    ccsi->wait_for_start(id);
  }
  Log_debug("after wait for start");

  timer_ = new Timer();
  timer_->start();

  if (config_->client_type_ == Config::Closed) {
    Log_info("closed loop clients.");
    verify(n_concurrent_ > 0);
    int n = n_concurrent_;
    auto sp_job = std::make_shared<OneTimeJob>([this] () {
      for (uint32_t n_tx = 0; n_tx < n_concurrent_; n_tx++) {
        auto coo = CreateCoordinator(n_tx);
        Log_debug("create coordinator %d", coo->coo_id_);
        this->DispatchRequest(coo);
      }
    });
    poll_mgr_->add(dynamic_pointer_cast<Job>(sp_job));
  } else {
    Log_info("open loop clients.");
    const std::chrono::nanoseconds wait_time
        ((int) (pow(10, 9) * 1.0 / (double) config_->client_rate_));
    double tps = 0;
    long txn_count = 0;
    auto start = std::chrono::steady_clock::now();
    std::chrono::nanoseconds elapsed;

    while (timer_->elapsed() < duration) {
      while (tps < config_->client_rate_ && timer_->elapsed() < duration) {
        auto coo = FindOrCreateCoordinator();
        if (coo != nullptr) {
          auto p_job = (Job*)new OneTimeJob([this, coo] () {
            this->DispatchRequest(coo);
          });
          shared_ptr<Job> sp_job(p_job);
          poll_mgr_->add(sp_job);
          txn_count++;
          elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>
              (std::chrono::steady_clock::now() - start);
          tps = (double) txn_count / elapsed.count() * pow(10, 9);
        }
      }
      auto next_time = std::chrono::steady_clock::now() + wait_time;
      std::this_thread::sleep_until(next_time);
      elapsed = std::chrono::duration_cast<std::chrono::nanoseconds>
          (std::chrono::steady_clock::now() - start);
      tps = (double) txn_count / elapsed.count() * pow(10, 9);
    }
    Log_debug("exit client dispatch loop...");
  }

//  finish_mutex.lock();
  while (n_concurrent_ > 0) {
    Log_debug("wait for finish... %d", n_concurrent_);
    sleep(1);
//    finish_cond.wait(finish_mutex);
  }
//  finish_mutex.unlock();

  if (failover_server_quit_ && !*failover_server_quit_)
  {
      *failover_server_quit_ = true ;
  }

  Log_info("Finish:\nTotal: %u, Commit: %u, Attempts: %u, Running for %u\n",
           num_txn.load(),
           success.load(),
           num_try.load(),
           Config::GetConfig()->get_duration());
  fflush(stderr);
  fflush(stdout);
  if (ccsi) {
    Log_info("%s: wait_for_shutdown at client %d", __FUNCTION__, cli_id_);
    ccsi->wait_for_shutdown();
  }
  delete timer_;
  return;
}
*/

void ClientWorker::AcceptForwardedRequest(TxRequest& request,
                                          TxReply* txn_reply,
                                          rrr::DeferredReply* defer) {
  const char* f = __FUNCTION__;

  // obtain free a coordinator first
  Coordinator* coo = nullptr;
  while (coo == nullptr) {
    coo = FindOrCreateCoordinator();
  }
  coo->forward_status_ = PROCESS_FORWARD_REQUEST;

  // run the task
  std::function<void()> task = [=]() {
    TxRequest req(request);
    req.callback_ = std::bind(&ClientWorker::ForwardRequestDone,
                              this,
                              coo,
                              txn_reply,
                              defer,
                              std::placeholders::_1);
    Log_debug("%s: running forwarded request at site %d", f, my_site_.id);
    coo->concurrent = n_concurrent_;
		coo->DoTxAsync(req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}

void ClientWorker::FailoverPreprocess(Coordinator* coo) {
  if (*failover_trigger_ || failover_trigger_loc) {
    if (coo->offset_ == 0) {
      failover_wait_leader_ = true;
    }
    n_pause_concurrent_[coo->coo_id_] = true;

    if (coo->offset_ == 0) {
      failover_pause_start = false;
      for (auto it = n_pause_concurrent_.begin(); it != n_pause_concurrent_.end(); it++) {
        while (!it->second) {
          auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(300 * 1000);
          sp_e->Wait(300 * 1000);
        }
      }
      failover_pause_start = true;
    } else {
      while (!failover_pause_start) {
        auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(300 * 1000);
        sp_e->Wait(300 * 1000);
      }
    }

    Log_debug("client worker start dispatch request pause: %d with cur leader %d",
        coo->coo_id_, cur_leader_);
    if (coo->offset_ == 0) {
      failover_trigger_loc = true;
      *failover_trigger_ = false;
    }
    while (!*failover_trigger_) {
      auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(300 * 1000);
      sp_e->Wait(300 * 1000);
      if (*failover_server_quit_) break;
    }
    if (coo->offset_ == 0) {
      SearchLeader(coo);
      *failover_trigger_ = false;
      failover_trigger_loc = false;
      failover_pause_start = false;
      failover_wait_leader_ = false;
    } else {
      while (failover_wait_leader_ && !*failover_server_quit_) {
        auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(500 * 1000);
        sp_e->Wait(500 * 1000);
      }
    }
    n_pause_concurrent_[coo->coo_id_] = false;
    Log_debug("client worker end dispatch request pause: %d with cur leader %d",
        coo->coo_id_, cur_leader_);
  }
}

void ClientWorker::DispatchRequest(Coordinator* coo) {
//  FailoverPreprocess(coo);
  const char* f = __FUNCTION__;
  std::function<void()> task = [=]() {
    Log_debug("%s: %d", f, cli_id_);
    // TODO don't use pointer here.
    TxRequest *req = new TxRequest;
    {
      std::lock_guard<std::mutex> lock(this->request_gen_mutex);
      tx_generator_->GetTxRequest(req, coo->coo_id_);
    }
//     req.callback_ = std::bind(&ClientWorker::RequestDone,
//                               this,
//                               coo,
//                               std::placeholders::_1);
    req->callback_ = [coo, req] (TxReply&) {
//      verify(coo->sp_ev_commit_->status_ != Event::WAIT);
      coo->sp_ev_commit_->Set(1);
      auto& status = coo->sp_ev_done_->status_;
      verify(status == Event::WAIT || status == Event::INIT);
      coo->sp_ev_done_->Set(1);
      delete req;
    };
    coo->DoTxAsync(*req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}

/*
void ClientWorker::DispatchRequest(Coordinator* coo) {
  FailoverPreprocess(coo);
  const char* f = __FUNCTION__;
  std::function<void()> task = [=]() {
    Log_debug("%s: %d", f, cli_id_);
    // TODO don't use pointer here.
    TxRequest *req = new TxRequest;
    {
      std::lock_guard<std::mutex> lock(this->request_gen_mutex);
      tx_generator_->GetTxRequest(req, coo->coo_id_);
    }
//     req.callback_ = std::bind(&ClientWorker::RequestDone,
//                               this,
//                               coo,
//                               std::placeholders::_1);
    req->callback_ = [coo, req] (TxReply&) {
//      verify(coo->sp_ev_commit_->status_ != Event::WAIT);
      coo->sp_ev_commit_->Set(1);
      auto& status = coo->sp_ev_done_->status_;
      verify(status == Event::WAIT || status == Event::INIT);
      coo->sp_ev_done_->Set(1);
      delete req;
    };
		coo->concurrent = n_concurrent_;
    coo->DoTxAsync(*req);
  };
  task();
//  dispatch_pool_->run_async(task); // this causes bug
}*/

void ClientWorker::SearchLeader(Coordinator* coo) {
  // TODO multiple par_id yidawu
  parid_t par_id = 0;
  coo->SetNewLeader(par_id, failover_server_idx_);
  cur_leader_ = *failover_server_idx_;
  Log_debug("client %d set cur_leader_ %d failover_server_idx_ %d", cli_id_, cur_leader_,
      *failover_server_idx_);
}

ClientWorker::ClientWorker(uint32_t id, Config::SiteInfo& site_info, Config* config,
    ClientControlServiceImpl* ccsi, PollMgr* poll_mgr, bool* volatile failover_trigger,
    volatile bool* failover_server_quit, volatile locid_t* failover_server_idx)
  : id(id),
    my_site_(site_info),
    config_(config),
    cli_id_(site_info.id),
    benchmark(config->benchmark()),
    mode(config->get_mode()),
    duration(config->get_duration()),
    ccsi(ccsi),
    n_concurrent_(config->get_concurrent_txn()),
    failover_trigger_(failover_trigger),
    failover_server_quit_(failover_server_quit),
    failover_server_idx_(failover_server_idx) {
  poll_mgr_ = poll_mgr == nullptr ? new PollMgr(1) : poll_mgr;
  frame_ = Frame::GetFrame(config->tx_proto_);
  tx_generator_ = frame_->CreateTxGenerator();
  config->get_all_site_addr(servers_);
  num_txn.store(0);
  success.store(0);
  num_try.store(0);
  commo_ = frame_->CreateCommo(poll_mgr_);
  commo_->loc_id_ = my_site_.locale_id;
  forward_requests_to_leader_ =
      (config->replica_proto_ == MODE_FPGA_RAFT && site_info.locale_id != 0) ? true :
                                                                               false;
  Log_debug("client %d created; forward %d",
            cli_id_,
            forward_requests_to_leader_);
}

void ClientWorker::Pause(locid_t locid) {
  // TODO modify it locid and parid
  fail_ctrl_coo_->SendFailOverTrig(0, locid, true);
}

void ClientWorker::Resume(locid_t locid) {
  // TODO modify it locid and parid
  fail_ctrl_coo_->SendFailOverTrig(0, locid, false);
}

} // namespace janus

