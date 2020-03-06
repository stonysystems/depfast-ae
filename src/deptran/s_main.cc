
#include "__dep__.h"
#include "frame.h"
#include "client_worker.h"
#include "procedure.h"
#include "command_marshaler.h"
#include "benchmark_control_rpc.h"
#include "server_worker.h"

#ifdef CPU_PROFILE
# include <gperftools/profiler.h>
#endif // ifdef CPU_PROFILE

using namespace janus;

static ClientControlServiceImpl *ccsi_g = nullptr;
static rrr::PollMgr *cli_poll_mgr_g = nullptr;
static rrr::Server *cli_hb_server_g = nullptr;

static vector<ServerWorker> svr_workers_g = {};
vector<unique_ptr<ClientWorker>> client_workers_g = {};
static std::vector<std::thread> client_threads_g = {}; // TODO remove this?
static std::vector<std::thread> failover_threads_g = {};

void client_setup_heartbeat(int num_clients) {
  Log_info("%s", __FUNCTION__);
  std::map<int32_t, std::string> txn_types;
  Frame* f = Frame::GetFrame(Config::GetConfig()->tx_proto_);
  f->GetTxTypes(txn_types);
  delete f;
  bool hb = Config::GetConfig()->do_heart_beat();
  if (hb) {
    // setup controller rpc server
    ccsi_g = new ClientControlServiceImpl(num_clients, txn_types);
    int n_io_threads = 1;
    cli_poll_mgr_g = new rrr::PollMgr(n_io_threads);
    base::ThreadPool *thread_pool = new base::ThreadPool(1);
    cli_hb_server_g = new rrr::Server(cli_poll_mgr_g, thread_pool);
    cli_hb_server_g->reg(ccsi_g);
    auto ctrl_port = std::to_string(Config::GetConfig()->get_ctrl_port());
    std::string server_address = std::string("0.0.0.0:").append(ctrl_port);
    Log_info("Start control server on port %s", ctrl_port.c_str());
    cli_hb_server_g->start(server_address.c_str());
  }
}

void client_launch_workers(vector<Config::SiteInfo> &client_sites) {
  // load some common configuration
  // start client workers in new threads.
  Log_info("client enabled, number of sites: %d", client_sites.size());
  vector<ClientWorker*> workers;

  for (uint32_t client_id = 0; client_id < client_sites.size(); client_id++) {
    ClientWorker* worker = new ClientWorker(client_id,
                                            client_sites[client_id],
                                            Config::GetConfig(),
                                            ccsi_g, nullptr);
    workers.push_back(worker);
    client_threads_g.push_back(std::thread(&ClientWorker::Work, worker));
    client_workers_g.push_back(std::unique_ptr<ClientWorker>(worker));
  }

}

void server_launch_worker(vector<Config::SiteInfo>& server_sites) {
  auto config = Config::GetConfig();
  Log_info("server enabled, number of sites: %d", server_sites.size());
  svr_workers_g.resize(server_sites.size(), ServerWorker());
  int i=0;
  vector<std::thread> setup_ths;
  for (auto& site_info : server_sites) {
    setup_ths.push_back(std::thread([&site_info, &i, &config] () {
      Log_info("launching site: %x, bind address %s",
               site_info.id,
               site_info.GetBindAddress().c_str());
      auto& worker = svr_workers_g[i++];
      worker.site_info_ = const_cast<Config::SiteInfo*>(&config->SiteById(site_info.id));
      worker.SetupBase();
      // register txn piece logic
      worker.RegisterWorkload();
      // populate table according to benchmarks
      worker.PopTable();
      Log_info("table popped for site %d", (int)worker.site_info_->id);
#ifdef DB_CHECKSUM
      worker.DbChecksum();
#endif
      // start server service
      worker.SetupService();
      Log_info("start communication for site %d", (int)worker.site_info_->id);
      worker.SetupCommo();
      Log_info("site %d launched!", (int)site_info.id);
      worker.launched_ = true;
    }));
  }

  for (auto& worker : svr_workers_g) {
    while (!worker.launched_) {
      sleep(1);
    }
  }

  Log_info("waiting for server setup threads.");
  for (auto& th: setup_ths) {
    th.join();
  }
  Log_info("done waiting for server setup threads.");

  for (ServerWorker& worker : svr_workers_g) {
    // start communicator after all servers are running
    // setup communication between controller script
    worker.SetupHeartbeat();
  }
  Log_info("server workers' communicators setup");
}

void client_shutdown() {
  client_workers_g.clear();
}

void server_shutdown() {
  for (auto &worker : svr_workers_g) {
    worker.ShutDown();
  }
}

void check_current_path() {
  auto path = boost::filesystem::current_path();
  Log_info("PWD : ", path.string().c_str());
}

void wait_for_clients() {
  Log_info("%s: wait for client threads to exit.", __FUNCTION__);
  for (auto &th: client_threads_g) {
    th.join();
  }
}

void server_failover_thread(bool random, bool leader, int srv_idx, bool *quit)
{
    int idx = -1 ;
    int expected_idx = -1 ;
    int run_int = Config::GetConfig()->get_failover_run_interval() ;
    int stop_int = Config::GetConfig()->get_failover_stop_interval() ;

    if (srv_idx != -1)
    {
        expected_idx = srv_idx ;
    }
    else if(!random)
    {
        // temporary solution, assign id 0 as leader, id 1 as follower
        if(leader)
        {
            expected_idx = 0 ;
        }
        else
        {
            expected_idx = 1 ;
        }
    }
    else
    {
        // do nothing
    }

    for(int i=0;i<svr_workers_g.size();i++)
    {
        if(svr_workers_g[i].site_info_->locale_id == expected_idx )
        {
            idx = i ;
            break ;
        }
    }    
    
    while(!(*quit))
    {
        if(random)
        {
            idx = rand() % svr_workers_g.size() ;
        }
        if(idx == -1) break ;   // quit if no idx has beeen assigned
        sleep(run_int) ;
        if(*quit)
        {
            break ;
        }
        svr_workers_g[idx].Pause() ;
        Log_debug("server %d paused for failover test", idx);
        sleep(stop_int) ;    
        svr_workers_g[idx].Resume() ;
        Log_debug("server %d resumed for failover test", idx);
    }

}

void server_failover(bool *quit)
{
    bool failover = Config::GetConfig()->get_failover();
    bool random = Config::GetConfig()->get_failover_random() ;
    bool leader = Config::GetConfig()->get_failover_leader() ;
    int idx = Config::GetConfig()->get_failover_srv_idx() ;
    if(failover)
    {
        failover_threads_g.push_back(
            std::thread(&server_failover_thread, random, leader, idx, quit)) ;
    }
}

void setup_ulimit() {
  struct rlimit limit;
  /* Get max number of files. */
  if (getrlimit(RLIMIT_NOFILE, &limit) != 0) {
    Log_fatal("getrlimit() failed with errno=%d", errno);
  }
  Log_info("ulimit -n is %d", (int)limit.rlim_cur);
}

int main(int argc, char *argv[]) {
  check_current_path();
  Log_info("starting process %ld", getpid());
  setup_ulimit();

  // read configuration
  int ret = Config::CreateConfig(argc, argv);
  if (ret != SUCCESS) {
    Log_fatal("Read config failed");
    return ret;
  }

  auto client_infos = Config::GetConfig()->GetMyClients();
  if (client_infos.size() > 0) {
    client_setup_heartbeat(client_infos.size());
  }

#ifdef CPU_PROFILE
  char prof_file[1024];
  Config::GetConfig()->GetProfilePath(prof_file);
  // start to profile
  ProfilerStart(prof_file);
  Log_info("started to profile cpu");
#endif // ifdef CPU_PROFILE

  
  auto server_infos = Config::GetConfig()->GetMyServers();
  if (!server_infos.empty()) {
    server_launch_worker(server_infos);
  }

  if (!client_infos.empty()) {
    //client_setup_heartbeat(client_infos.size());
    client_launch_workers(client_infos);
    bool quit = false ;
    server_failover(&quit) ;
    sleep(Config::GetConfig()->duration_);
    wait_for_clients();
    quit = true ;
    Log_info("all clients have shut down.");
  }

#ifdef DB_CHECKSUM
  sleep(5); // hopefully servers can finish hanging RPCs in 5 seconds.
#endif

  for (auto& worker : svr_workers_g) {
    worker.WaitForShutdown();
  }
  
  for (auto& ft : failover_threads_g)
  {
    ft.join() ;
  }
    
#ifdef DB_CHECKSUM
  for (auto& worker : svr_workers_g) {
    worker.DbChecksum();
  }
#endif
#ifdef CPU_PROFILE
  // stop profiling
  ProfilerStop();
#endif // ifdef CPU_PROFILE
  fflush(stderr);
  fflush(stdout);
  return 0;
  // TODO, FIXME pending_future in rpc cause error.
  client_shutdown();
  server_shutdown();
  Log_info("all server workers have shut down.");

  RandomGenerator::destroy();
  Config::DestroyConfig();

  Log_debug("exit process.");

  return 0;
}
