#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "server.h"
#include "service.h"
#include "coordinator.h"

namespace janus {

REG_FRAME(MODE_COPILOT, vector<string>({"copilot"}), CopilotFrame);

CopilotFrame::CopilotFrame(int mode) : Frame(mode) {}

CopilotFrame::~CopilotFrame() {
  Log_info(
      "server %d, "
      "[FAST_ACCEPT] %u "
      "[ACCEPT] %u "
      "[COMMIT] %u "
      "[PREPARE] %u",
      site_info_->id, n_fast_accept_, n_accept_, n_commit_, n_prepare_);
}

Coordinator *CopilotFrame::CreateCoordinator(cooid_t coo_id,
                                            Config *config,
                                            int benchmark,
                                            ClientControlServiceImpl *ccsi,
                                            uint32_t id,
                                            shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  // TODO: pool used coordinator to avoid creating every time
  auto coord = new CoordinatorCopilot(coo_id, benchmark, ccsi, id);

  setupCoordinator(coord, config);  

  Log_debug("create new copilot coord, coo_id: %d", (int)coord->coo_id_);
  return coord;
}

TxLogServer *CopilotFrame::CreateScheduler() {
  if (sch_ == nullptr) {
    sch_ = new CopilotServer(this);
  } else {
    verify(0);
  }

  Log_debug("create copilot sched loc: %d", this->site_info_->locale_id);
  return sch_;
}

Communicator *CopilotFrame::CreateCommo(PollMgr *poll) {
  if (commo_ == nullptr) {
    commo_ = new CopilotCommo(poll);
  }

  return commo_;
}

vector<rrr::Service *>
CopilotFrame::CreateRpcServices(uint32_t site_id,
                                TxLogServer *rep_sched,
                                rrr::PollMgr *poll_mgr,
                                ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = vector<Service *>();
  switch (config->replica_proto_) {
    case MODE_COPILOT:
      result.push_back(new CopilotServiceImpl(rep_sched));
      break;
    default:
      break;
  }

  return result;
}

void CopilotFrame::setupCoordinator(CoordinatorCopilot *coord, Config *config) {
  coord->frame_ = this;
  
  verify(commo_ != nullptr);
  coord->commo_ = commo_;

  verify(sch_ != nullptr);
  coord->sch_ = sch_;
  coord->slot_hint_ = &slot_hint_;
  // coord->slot_id_ = slot_hint_++;
  coord->n_replica_ = config->GetPartitionSize(site_info_->partition_id_);
  coord->loc_id_ = site_info_->locale_id;
  verify(coord->n_replica_ != 0);
}

} // namespace janus

