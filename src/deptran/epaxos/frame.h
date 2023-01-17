#pragma once

#include "../communicator.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"

namespace janus {

class EpaxosFrame : public Frame {
 private:
  EpaxosCommo *commo_ = nullptr;
  EpaxosServer *sch_ = nullptr;
  
  #ifdef EPAXOS_TEST_CORO
  static std::mutex epaxos_test_mutex_;
  static std::shared_ptr<Coroutine> epaxos_test_coro_;
  static uint16_t n_replicas_;
  static EpaxosFrame *replicas_[5];
  static uint16_t n_commo_;
  static bool tests_done_;
  #endif

 public:
  EpaxosFrame(int mode);
  virtual ~EpaxosFrame();

  TxLogServer *CreateScheduler() override;
  
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
  
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;
  EpaxosServer *server() {
    return sch_;
  }

  EpaxosCommo *commo() {
    return commo_;
  }
};

} // namespace janus
