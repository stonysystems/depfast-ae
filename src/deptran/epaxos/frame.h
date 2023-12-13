#pragma once

#include "../communicator.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"
#include "coordinator.h"

namespace janus {

class EpaxosFrame : public Frame {
 private:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_SERVER_METRICS_COLLECTION)
  static std::mutex epaxos_test_mutex_;
  static uint16_t n_replicas_;
  #endif

 public:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_SERVER_METRICS_COLLECTION)
  static EpaxosFrame *replicas_[NSERVERS];
  #endif
  EpaxosCommo *commo_ = nullptr;
  EpaxosServer *svr_ = nullptr;

  EpaxosFrame(int mode);
  virtual ~EpaxosFrame();

  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg);

  TxLogServer *CreateScheduler() override;
  
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
  
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;

  void setupCoordinator(EpaxosCoordinator *coord, Config *config);
};

} // namespace janus
