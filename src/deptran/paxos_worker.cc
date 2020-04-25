#include "paxos_worker.h"
#include "service.h"
#include "chrono"

namespace janus {


moodycamel::ConcurrentQueue<Coordinator*> PaxosWorker::coo_queue;

static int volatile xx =
    MarshallDeputy::RegInitializer(MarshallDeputy::CONTAINER_CMD,
                                   []() -> Marshallable* {
                                     return new LogEntry;
                                   });
static int volatile xxx =
      MarshallDeputy::RegInitializer(MarshallDeputy::CMD_BLK_PXS,
                                     []() -> Marshallable* {
                                       return new BulkPaxosCmd;
                                     });


Marshal& LogEntry::ToMarshal(Marshal& m) const {
  m << length;
  // m << std::string(operation_);
  m << log_entry;
  return m;
};

Marshal& LogEntry::FromMarshal(Marshal& m) {
  m >> length;
  // std::string str;
  // m >> str;
  // operation_ = new char[length];
  // strcpy(operation_, str.c_str());
  m >> log_entry;
  return m;
};

void PaxosWorker::SetupBase() {
  auto config = Config::GetConfig();
  rep_frame_ = Frame::GetFrame(config->replica_proto_);
  rep_frame_->site_info_ = site_info_;
  rep_sched_ = rep_frame_->CreateScheduler();
  rep_sched_->loc_id_ = site_info_->locale_id;
  rep_sched_->partition_id_ = site_info_->partition_id_;
  this->tot_num = config->get_tot_req();
}

void PaxosWorker::Next(Marshallable& cmd) {
  if (cmd.kind_ == MarshallDeputy::CONTAINER_CMD) {
    if (this->callback_ != nullptr) {
      auto& sp_log_entry = dynamic_cast<LogEntry&>(cmd);
      callback_(sp_log_entry.log_entry.c_str(), sp_log_entry.length);
    } else {
      verify(0);
    }
  } else {
    verify(0);
  }
  //if (n_current > n_tot) {
    n_current++;
    if (n_current >= n_tot) {
      //Log_info("Current pair id %d loc id %d n_current and n_tot and accept size is %d %d", site_info_->partition_id_, site_info_->locale_id, (int)n_current, (int)n_tot);
      finish_cond.bcast();
    }
  //}
}

void PaxosWorker::SetupService() {
  std::string bind_addr = site_info_->GetBindAddress();
  int n_io_threads = 1;
  svr_poll_mgr_ = new rrr::PollMgr(n_io_threads);
  if (rep_frame_ != nullptr) {
    services_ = rep_frame_->CreateRpcServices(site_info_->id,
                                              rep_sched_,
                                              svr_poll_mgr_,
                                              scsi_);
  }
  uint32_t num_threads = 1;
  thread_pool_g = new base::ThreadPool(num_threads);

  // init rrr::Server
  rpc_server_ = new rrr::Server(svr_poll_mgr_, thread_pool_g);

  // reg services
  for (auto service : services_) {
    rpc_server_->reg(service);
  }

  // start rpc server
  Log_debug("starting server at %s", bind_addr.c_str());
  int ret = rpc_server_->start(bind_addr.c_str());
  if (ret != 0) {
    Log_fatal("server launch failed.");
  }

  Log_info("Server %s ready at %s",
           site_info_->name.c_str(),
           bind_addr.c_str());
}

void PaxosWorker::SetupCommo() {
  if (rep_frame_) {
    rep_commo_ = rep_frame_->CreateCommo(svr_poll_mgr_);
    if (rep_commo_) {
      rep_commo_->loc_id_ = site_info_->locale_id;
    }
    rep_sched_->commo_ = rep_commo_;
  }
  if (IsLeader(site_info_->partition_id_))
    submit_pool = new SubmitPool();
}

void PaxosWorker::SetupHeartbeat() {
  bool hb = Config::GetConfig()->do_heart_beat();
  if (!hb) return;
  auto timeout = Config::GetConfig()->get_ctrl_timeout();
  scsi_ = new ServerControlServiceImpl(timeout);
  int n_io_threads = 1;
  svr_hb_poll_mgr_g = new rrr::PollMgr(n_io_threads);
  hb_thread_pool_g = new rrr::ThreadPool(1);
  hb_rpc_server_ = new rrr::Server(svr_hb_poll_mgr_g, hb_thread_pool_g);
  hb_rpc_server_->reg(scsi_);

  auto port = site_info_->port + CtrlPortDelta;
  std::string addr_port = std::string("0.0.0.0:") +
                          std::to_string(port);
  hb_rpc_server_->start(addr_port.c_str());
  if (hb_rpc_server_ != nullptr) {
    // Log_info("notify ready to control script for %s", bind_addr.c_str());
    scsi_->set_ready();
  }
  Log_info("heartbeat setup for %s on %s",
           site_info_->name.c_str(), addr_port.c_str());
}

void PaxosWorker::WaitForShutdown() {
  if (submit_pool != nullptr) {
    delete submit_pool;
    submit_pool = nullptr;
  }
  if (hb_rpc_server_ != nullptr) {
//    scsi_->server_heart_beat();
    scsi_->wait_for_shutdown();
    delete hb_rpc_server_;
    delete scsi_;
    svr_hb_poll_mgr_g->release();
    hb_thread_pool_g->release();

    for (auto service : services_) {
      if (DepTranServiceImpl* s = dynamic_cast<DepTranServiceImpl*>(service)) {
        auto& recorder = s->recorder_;
        if (recorder) {
          auto n_flush_avg_ = recorder->stat_cnt_.peek().avg_;
          auto sz_flush_avg_ = recorder->stat_sz_.peek().avg_;
          Log::info("Log to disk, average log per flush: %lld,"
                    " average size per flush: %lld",
                    n_flush_avg_, sz_flush_avg_);
        }
      }
    }
  }
}

void PaxosWorker::ShutDown() {
  Log_info("site %s deleting services, num: %d %d %d %d", site_info_->name.c_str(), services_.size(), 0, (int)n_current, (int)n_tot);
  verify(rpc_server_ != nullptr);
  delete rpc_server_;
  rpc_server_ = nullptr;
  for (auto service : services_) {
    delete service;
  }
  thread_pool_g->release();
  for (auto c : created_coordinators_) {
    delete c;
  }
  if (rep_sched_ != nullptr) {
    delete rep_sched_;
  }
}

void PaxosWorker::IncSubmit(){
    n_tot++;
}

void PaxosWorker::BulkSubmit(const vector<Coordinator*>& entries){
    //Log_debug("Obtaining bulk submit of size %d through coro", (int)entries.size());
    //Log_debug("Current n_submit and n_current is %d %d", (int)n_submit, (int)n_current);
    auto sp_cmd = make_shared<BulkPaxosCmd>();
    for(auto coo : entries){
        auto mpc = dynamic_cast<CoordinatorMultiPaxos*>(coo);
        sp_cmd->slots.push_back(mpc->slot_id_);
        sp_cmd->ballots.push_back(mpc->curr_ballot_);
        verify(mpc->cmd_ != nullptr);
        MarshallDeputy md(mpc->cmd_);
        sp_cmd->cmds.push_back(make_shared<MarshallDeputy>(md));
    }
    auto sp_m = dynamic_pointer_cast<Marshallable>(sp_cmd);
    //n_current += (int)entries.size();
    //n_submit -= (int)entries.size();
    //Log_info("Current pair id %d n_current and n_tot is %d %d", site_info_->partition_id_, (int)n_current, (int)n_tot);
    _BulkSubmit(sp_m);
}

inline void PaxosWorker::_BulkSubmit(shared_ptr<Marshallable> sp_m){
    Coordinator* coord = rep_frame_->CreateBulkCoordinator(Config::GetConfig(), 0);
    coord->par_id_ = site_info_->partition_id_;
    coord->loc_id_ = site_info_->locale_id;
    coord->BulkSubmit(sp_m);
}

void PaxosWorker::AddAccept(Coordinator* coord) {
  //Log_info("current batch cnt %d", cnt);
  PaxosWorker::coo_queue.enqueue(coord);
}

int PaxosWorker::deq_from_coo(vector<Coordinator*>& current){
	int qcnt = PaxosWorker::coo_queue.try_dequeue_bulk(&current[0], cnt);
	return qcnt;
}

void* PaxosWorker::StartReadAccept(void* arg){
  PaxosWorker* pw = (PaxosWorker*)arg;
  std::vector<Coordinator*> current(pw->cnt, nullptr);
  while (!pw->stop_flag) {
    int cnt = pw->deq_from_coo(current);
    if(cnt <= 0)continue;
    std::vector<Coordinator*> sub(current.begin(), current.begin() + cnt);
    //Log_debug("Pushing coordinators for bulk accept coordinators here having size %d %d %d %d", (int)sub.size(), pw->n_current.load(), pw->n_tot.load(),pw->site_info_->locale_id);
    auto sp_job = std::make_shared<OneTimeJob>([&pw, sub]() {
      pw->BulkSubmit(sub);
    });
    pw->GetPollMgr()->add(sp_job);
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  pthread_exit(nullptr);
  return nullptr;
}

void PaxosWorker::WaitForSubmit() {
  while (n_current < n_tot) {
    finish_mutex.lock();
    //Log_info("wait for task, amount: %d", (int)n_tot-(int)n_current);
    finish_cond.wait(finish_mutex);
    finish_mutex.unlock();
  }
  Log_debug("finish task.");
}

void PaxosWorker::InitQueueRead(){
  if(IsLeader(site_info_->partition_id_)){
    stop_flag = false;
    Pthread_create(&bulkops_th_, nullptr, PaxosWorker::StartReadAccept, this);
    pthread_detach(bulkops_th_);
  }
}

void PaxosWorker::AddReplayEntry(Marshallable& entry){
  Marshallable *p = &entry;
  replay_queue.enqueue(p);
}

void* PaxosWorker::StartReplayRead(void* arg){
  PaxosWorker* pw = (PaxosWorker*)arg;
  while(!pw->stop_replay_flag){
    Marshallable* p;
    auto res = pw->replay_queue.try_dequeue(p);
    if(!res)continue;
    pw->Next(*p);
  }
}

PaxosWorker::PaxosWorker() {
  stop_replay_flag = false;
  Pthread_create(&replay_th_, nullptr, PaxosWorker::StartReplayRead, this);
  pthread_detach(replay_th_);
}

PaxosWorker::~PaxosWorker() {
  Log_debug("Ending worker with n_tot %d and n_current %d", (int)n_tot, (int)n_current);
  stop_flag = true;
  stop_replay_flag = true;
}


void PaxosWorker::Submit(const char* log_entry, int length, uint32_t par_id) {
  if (!IsLeader(par_id)) return;
  auto sp_cmd = make_shared<LogEntry>();
  sp_cmd->log_entry = string(log_entry,length);
//  Log_info("PaxosWorker::Submit Log=%s",operation_);
  sp_cmd->length = length;
  auto sp_m = dynamic_pointer_cast<Marshallable>(sp_cmd);
  _Submit(sp_m);
  free((char*)log_entry);
}

inline void PaxosWorker::_Submit(shared_ptr<Marshallable> sp_m) {
  // finish_mutex.lock();
  //n_current++;
  //n_submit--;
  //n_tot++;
  // finish_mutex.unlock();
  static cooid_t cid = 1;
  static id_t id = 1;
  verify(rep_frame_ != nullptr);
  Coordinator* coord = rep_frame_->CreateCoordinator(cid++,
                                                     Config::GetConfig(),
                                                     0,
                                                     nullptr,
                                                     id++,
                                                     nullptr);
  coord->par_id_ = site_info_->partition_id_;
  coord->loc_id_ = site_info_->locale_id;
  //created_coordinators_.push_back(coord);
  coord->assignCmd(sp_m);
  if(stop_flag != true) {
    AddAccept(coord);
  } else{
    coord->Submit(sp_m);
  }
}

bool PaxosWorker::IsLeader(uint32_t par_id) {
  verify(rep_frame_ != nullptr);
  verify(rep_frame_->site_info_ != nullptr);
  return rep_frame_->site_info_->partition_id_ == par_id &&
         rep_frame_->site_info_->locale_id == 0;
}

bool PaxosWorker::IsPartition(uint32_t par_id) {
  verify(rep_frame_ != nullptr);
  verify(rep_frame_->site_info_ != nullptr);
  return rep_frame_->site_info_->partition_id_ == par_id;
}

void PaxosWorker::register_apply_callback(std::function<void(const char*, int)> cb) {
  this->callback_ = cb;
  verify(rep_sched_ != nullptr);
  rep_sched_->RegLearnerAction(std::bind(&PaxosWorker::AddReplayEntry,
                                         this,
                                         std::placeholders::_1));
}

} // namespace janus
