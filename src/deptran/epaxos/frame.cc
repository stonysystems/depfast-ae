#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "server.h"
#include "service.h"

namespace janus {

REG_FRAME(MODE_EPAXOS, vector<string>({"epaxos"}), EpaxosFrame);

#if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_SERVER_METRICS_COLLECTION)
std::mutex EpaxosFrame::epaxos_test_mutex_;
uint16_t EpaxosFrame::n_replicas_ = 0;
EpaxosFrame *EpaxosFrame::replicas_[NSERVERS];
#endif

EpaxosFrame::EpaxosFrame(int mode) : Frame(mode) {}

EpaxosFrame::~EpaxosFrame() {}

Coordinator *EpaxosFrame::CreateCoordinator(cooid_t coo_id,
                                            Config *config,
                                            int benchmark,
                                            ClientControlServiceImpl *ccsi,
                                            uint32_t id,
                                            shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  // TODO: pool used coordinator to avoid creating every time
  auto coord = new EpaxosCoordinator(coo_id, benchmark, ccsi, id);
  setupCoordinator(coord, config);  
  Log_debug("create new coord, coo_id: %d", (int)coord->coo_id_);
  return coord;
}

TxLogServer *EpaxosFrame::CreateScheduler() {
  if (svr_ == nullptr) {
    svr_ = new EpaxosServer(this);
  } else {
    verify(0);
  }
  Log_debug("create epaxos sched loc: %d", this->site_info_->locale_id);

  #if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_SERVER_METRICS_COLLECTION)
  epaxos_test_mutex_.lock();
  verify(n_replicas_ < NSERVERS);
  replicas_[this->site_info_->id] = this;
  n_replicas_++;
  epaxos_test_mutex_.unlock();
  #endif
  return svr_;
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

void EpaxosFrame::setupCoordinator(EpaxosCoordinator *coord, Config *config) {
  coord->frame_ = this;
  verify(commo_ != nullptr);
  coord->commo_ = commo_;
  verify(svr_ != nullptr);
  coord->sch_ = svr_;
  coord->n_replica_ = config->GetPartitionSize(site_info_->partition_id_);
  coord->loc_id_ = site_info_->locale_id;
  verify(coord->n_replica_ != 0);
}

} // namespace janus

