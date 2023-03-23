#pragma once

#include "../communicator.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"

namespace janus {

class EpaxosFrame : public Frame {
 private:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  static std::mutex epaxos_test_mutex_;
  static uint16_t n_replicas_;
  #endif

 public:
  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)
  static EpaxosFrame *replicas_[NSERVERS];
  #endif
  EpaxosCommo *commo_ = nullptr;
  EpaxosServer *svr_ = nullptr;

  EpaxosFrame(int mode);
  virtual ~EpaxosFrame();

  TxLogServer *CreateScheduler() override;
  
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
  
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;
};

} // namespace janus
