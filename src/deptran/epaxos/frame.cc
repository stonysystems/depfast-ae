#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "server.h"
#include "service.h"
#include "coordinator.h"

namespace janus {

REG_FRAME(MODE_EPAXOS, vector<string>({"epaxos"}), EpaxosFrame);

EpaxosFrame::EpaxosFrame(int mode) : Frame(mode) {}

EpaxosFrame::~EpaxosFrame() {}

TxLogServer *EpaxosFrame::CreateScheduler() {
  if (sch_ == nullptr) {
    sch_ = new EpaxosServer(this);
  } else {
    verify(0);
  }
  Log_debug("create epaxos sched loc: %d", this->site_info_->locale_id);
  return sch_;
}

Communicator *EpaxosFrame::CreateCommo(PollMgr *poll) {
  if (commo_ == nullptr) {
    commo_ = new EpaxosCommo(poll);
  }
  return commo_;
}

vector<rrr::Service *>
EpaxosFrame::CreateRpcServices(uint32_t site_id,
                                TxLogServer *rep_sched,
                                rrr::PollMgr *poll_mgr,
                                ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = vector<Service *>();
  switch (config->replica_proto_) {
    case MODE_EPAXOS:
      result.push_back(new EpaxosServiceImpl(rep_sched));
      break;
    default:
      break;
  }
  return result;
}

} // namespace janus

