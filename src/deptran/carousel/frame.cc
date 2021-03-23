#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "coordinator.h"
#include "scheduler.h"
#include "tx.h"
#include "commo.h"
#include "config.h"

namespace janus {

REG_FRAME(MODE_CAROUSEL, vector<string>({"carousel"}), FrameCarousel);

Coordinator *FrameCarousel::CreateCoordinator(cooid_t coo_id,
                                           Config *config,
                                           int benchmark,
                                           ClientControlServiceImpl *ccsi,
                                           uint32_t id,
                                           shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  CoordinatorCarousel *coord = new CoordinatorCarousel(coo_id,
                                                 benchmark,
                                                 ccsi,
                                                 id);
  coord->txn_reg_ = txn_reg;
  coord->frame_ = this;
  return coord;
}

Communicator *FrameCarousel::CreateCommo(PollMgr *pollmgr) {
  // Default: return null;
  commo_ = new CarouselCommo(pollmgr);
  return commo_;
}

TxLogServer *FrameCarousel::CreateScheduler() {
  TxLogServer *sched = new SchedulerCarousel();
  sched->frame_ = this;
  return sched;
}

mdb::Row *FrameCarousel::CreateRow(const mdb::Schema *schema,
                                vector<Value> &row_data) {

  mdb::Row *r = mdb::VersionedRow::create(schema, row_data);
  return r;
}

shared_ptr<Tx> FrameCarousel::CreateTx(epoch_t epoch, txnid_t tid,
                                    bool ro, TxLogServer *mgr) {
  shared_ptr<Tx> sp_tx(new TxCarousel(epoch, tid, mgr));
  return sp_tx;
}

vector<rrr::Service *>
FrameCarousel::CreateRpcServices(uint32_t site_id,
                              TxLogServer *sched,
                              rrr::PollMgr *poll_mgr,
                              ServerControlServiceImpl *scsi) {
  return Frame::CreateRpcServices(site_id, sched, poll_mgr, scsi);
}

} // namespace janus
