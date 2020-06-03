#pragma once

#include "__dep__.h"
#include "config.h"
#include "communicator.h"
#include "procedure.h"

namespace janus {

class ClientControlServiceImpl;
class Workload;
class CoordinatorBase;
class Frame;
class Coordinator;
class TxnRegistry;
class TxReply;

class ClientWorker {
 public:
  PollMgr* poll_mgr_{nullptr};
  Frame* frame_{nullptr};
  Communicator* commo_{nullptr};
  cliid_t cli_id_;
  int32_t benchmark;
  int32_t mode;
  bool batch_start;
  uint32_t id;
  uint32_t duration;
  ClientControlServiceImpl *ccsi{nullptr};
  int32_t n_concurrent_;
  map<cooid_t, bool> n_pause_concurrent_{};
  rrr::Mutex finish_mutex{};
  rrr::CondVar finish_cond{};
  bool forward_requests_to_leader_ = false;

  // coordinators_{mutex, cond} synchronization currently only used for open clients
  std::mutex request_gen_mutex{};
  std::mutex coordinator_mutex{};
  vector<Coordinator*> free_coordinators_{};
  vector<Coordinator*> created_coordinators_{};
  Coordinator* fail_ctrl_coo_{nullptr} ;
//  rrr::ThreadPool* dispatch_pool_ = new rrr::ThreadPool();

  std::shared_ptr<TimeoutEvent> timeout_event;
  std::shared_ptr<NEvent> n_event;
  std::shared_ptr<AndEvent> and_event;

  std::atomic<uint32_t> num_txn, success, num_try;
  Workload * tx_generator_{nullptr};
  Timer *timer_{nullptr};
  shared_ptr<TxnRegistry> txn_reg_{nullptr};
  Config* config_{nullptr};
  Config::SiteInfo& my_site_;
  vector<string> servers_;
  bool* volatile failover_trigger_ ;
  volatile bool* failover_server_quit_ ;
  volatile locid_t* failover_server_idx_ ;
  locid_t cur_leader_{0} ; // init leader is 0
  bool failover_wait_leader_{false} ;
  bool failover_trigger_loc{false} ;
  bool failover_pause_start{false} ;
 public:
  ClientWorker(uint32_t id,
               Config::SiteInfo &site_info,
               Config *config,
               ClientControlServiceImpl *ccsi,
               PollMgr* mgr,
               bool* volatile failover,
               volatile bool* failover_server_quit,
               volatile locid_t* failover_server_idx );
  ClientWorker() = delete;
  ~ClientWorker();
  // This is called from a different thread.
  void Work();
  Coordinator* FindOrCreateCoordinator();
  void DispatchRequest(Coordinator *coo);
  void SearchLeader(Coordinator* coo);
  void Pause(locid_t locid) ;
  void Resume(locid_t locid) ;
  Coordinator* CreateFailCtrlCoordinator() ;
  void AcceptForwardedRequest(TxRequest &request, TxReply* txn_reply, rrr::DeferredReply* defer);

 protected:
  Coordinator* CreateCoordinator(uint16_t offset_id);
  void RequestDone(Coordinator* coo, TxReply &txn_reply);
  void ForwardRequestDone(Coordinator* coo, TxReply* output, rrr::DeferredReply* defer, TxReply &txn_reply);
};
} // namespace janus


