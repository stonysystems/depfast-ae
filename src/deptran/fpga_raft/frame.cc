#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "exec.h"
#include "coordinator.h"
#include "server.h"
#include "service.h"
#include "commo.h"
#include "config.h"

namespace janus {

REG_FRAME(MODE_FPGA_RAFT, vector<string>({"fpga_raft"}), FpgaRaftFrame);

/*
template<typename D>
struct automatic_register {
 private:
  struct exec_register {
    exec_register() {
      D::do_it();
    }
  };
  // will force instantiation of definition of static member
  template<exec_register&> struct ref_it { };

  static exec_register register_object;
  static ref_it<register_object> referrer;
};

template<typename D> typename automatic_register<D>::exec_register
    automatic_register<D>::register_object;

struct foo : automatic_register<foo> {
  static void do_it() {
    REG_FRAME(MODE_FPGA_RAFT, vector<string>({"fpga_raft"}), FpgaRaftFrame);
  }
};*/

FpgaRaftFrame::FpgaRaftFrame(int mode) : Frame(mode) {

}

Executor *FpgaRaftFrame::CreateExecutor(cmdid_t cmd_id, TxLogServer *sched) {
  Executor *exec = new FpgaRaftExecutor(cmd_id, sched);
  return exec;
}

Coordinator *FpgaRaftFrame::CreateCoordinator(cooid_t coo_id,
                                                Config *config,
                                                int benchmark,
                                                ClientControlServiceImpl *ccsi,
                                                uint32_t id,
                                                shared_ptr<TxnRegistry> txn_reg) {
  verify(config != nullptr);
  CoordinatorFpgaRaft *coo;
  coo = new CoordinatorFpgaRaft(coo_id,
                                  benchmark,
                                  ccsi,
                                  id);
  coo->frame_ = this;
  verify(commo_ != nullptr);
  coo->commo_ = commo_;
  /* TODO: remove when have a class for common data */
  verify(sch_ != nullptr);
  coo->sch_ = this->sch_;
  coo->slot_hint_ = &slot_hint_;
  coo->slot_id_ = slot_hint_++;
  coo->n_replica_ = config->GetPartitionSize(site_info_->partition_id_);
  coo->loc_id_ = this->site_info_->locale_id;
  verify(coo->n_replica_ != 0); // TODO
  Log_debug("create new fpga raft coord, coo_id: %d", (int) coo->coo_id_);
  return coo;
}

TxLogServer *FpgaRaftFrame::CreateScheduler() {
  if(sch_ == nullptr)
  {
    sch_ = new FpgaRaftServer(this);
  }
  else
  {
    verify(0) ;
  }
  Log_debug("create new fpga raft sched loc: %d", this->site_info_->locale_id);
  return sch_ ;
}

Communicator *FpgaRaftFrame::CreateCommo(PollMgr *poll) {
  // We only have 1 instance of FpgaRaftFrame object that is returned from
  // GetFrame method. FpgaRaftCommo currently seems ok to share among the
  // clients of this method.
  if (commo_ == nullptr) {
    commo_ = new FpgaRaftCommo(poll);
  }
  return commo_;
}

vector<rrr::Service *>
FpgaRaftFrame::CreateRpcServices(uint32_t site_id,
                                   TxLogServer *rep_sched,
                                   rrr::PollMgr *poll_mgr,
                                   ServerControlServiceImpl *scsi) {
  auto config = Config::GetConfig();
  auto result = std::vector<Service *>();
  switch (config->replica_proto_) {
    case MODE_FPGA_RAFT:result.push_back(new FpgaRaftServiceImpl(rep_sched));
    default:break;
  }
  return result;
}

} // namespace janus;
