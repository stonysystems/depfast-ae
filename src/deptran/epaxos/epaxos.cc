
#include "epaxos.h"
#include "server.h"
#include "coord.h"
#include "commo.h"

namespace janus {
REG_FRAME(MODE_EPAXOS, vector<string>({"epaxos"}), EPaxosFrame);

TxLogServer *EPaxosFrame::CreateScheduler() {
  TxLogServer *sched = new EPaxosServer();
  sched->frame_ = this;
  return sched;
}

Coordinator *EPaxosFrame::CreateCoordinator(cooid_t coo_id,
                                            Config *config,
                                            int benchmark,
                                            ClientControlServiceImpl *ccsi,
                                            uint32_t id,
                                            shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  auto *coord = new EPaxosCoord(coo_id, benchmark, ccsi, id);
  coord->mocking_janus_ = true;
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Communicator *EPaxosFrame::CreateCommo(PollMgr *poll) {
  return new EPaxosCommo(poll);
}
} // namespane janus
