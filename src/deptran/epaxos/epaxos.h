//
// Created by shuai on 08/04/21.
//

#ifndef JANUS_SRC_DEPTRAN_EPAXOS_EPAXOS_H_
#define JANUS_SRC_DEPTRAN_EPAXOS_EPAXOS_H_

#pragma once

#include "../troad/troad.h"

namespace janus {

class EPaxosFrame : public TroadFrame {
 public:
  EPaxosFrame(int mode = MODE_EPAXOS) : TroadFrame(mode) {}
  TxLogServer *CreateScheduler() override;
  Coordinator* CreateCoordinator(cooid_t coo_id,
                                 Config *config,
                                 int benchmark,
                                 ClientControlServiceImpl *ccsi,
                                 uint32_t id,
                                 shared_ptr<TxnRegistry> txn_reg) override;
  Communicator *CreateCommo(PollMgr *poll = nullptr) override;
};


} // namespace janus
#endif //JANUS_SRC_DEPTRAN_EPAXOS_EPAXOS_H_
