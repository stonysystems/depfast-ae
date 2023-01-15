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
  slotid_t slot_hint_ = 1;
  
 public:
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
