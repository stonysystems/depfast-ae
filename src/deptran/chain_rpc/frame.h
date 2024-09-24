#pragma once

#include <deptran/communicator.h>
#include "../frame.h"
#include "../constants.h"
#include "commo.h"
#include "server.h"

namespace janus {

class ChainRPCFrame : public Frame {
 private:
  slotid_t slot_hint_ = 1;
 public:
  ChainRPCFrame(int mode);
  ChainRPCCommo *commo_ = nullptr;
  /* TODO: have another class for common data */
  ChainRPCServer *sch_ = nullptr;
  Executor *CreateExecutor(cmdid_t cmd_id, TxLogServer *sched) override;
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
