#pragma once

#include "../communicator.h"
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"

namespace janus {

class CoordinatorCopilot;
class CopilotFrame : public Frame {
  CopilotCommo *commo_ = nullptr;
  CopilotServer *sch_ = nullptr;

  slotid_t slot_hint_ = 1;

  void setupCoordinator(CoordinatorCopilot *coord, Config *config);

 public:
  CopilotFrame(int mode);

  Coordinator *CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  
  TxLogServer *CreateScheduler() override;
  
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
  
  vector<rrr::Service *> CreateRpcServices(uint32_t site_id,
                                           TxLogServer *dtxn_sched,
                                           rrr::PollMgr *poll_mgr,
                                           ServerControlServiceImpl *scsi) override;

};

} // namespace janus