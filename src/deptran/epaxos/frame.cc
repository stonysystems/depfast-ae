#include "../__dep__.h"
#include "../constants.h"
#include "frame.h"
#include "server.h"
#include "service.h"
#include "test.h"

namespace janus {

REG_FRAME(MODE_EPAXOS, vector<string>({"epaxos"}), EpaxosFrame);

#ifdef EPAXOS_TEST_CORO
std::mutex EpaxosFrame::epaxos_test_mutex_;
std::shared_ptr<Coroutine> EpaxosFrame::epaxos_test_coro_ = nullptr;
uint16_t EpaxosFrame::n_replicas_ = 0;
EpaxosFrame *EpaxosFrame::replicas_[5];
uint16_t EpaxosFrame::n_commo_ = 0;
bool EpaxosFrame::tests_done_ = false;
#endif

EpaxosFrame::EpaxosFrame(int mode) : Frame(mode) {}

EpaxosFrame::~EpaxosFrame() {}

TxLogServer *EpaxosFrame::CreateScheduler() {
  if (sch_ == nullptr) {
    sch_ = new EpaxosServer(this);
  } else {
    verify(0);
  }
  Log_debug("create epaxos sched loc: %d", this->site_info_->locale_id);

  #ifdef EPAXOS_TEST_CORO
  epaxos_test_mutex_.lock();
  verify(n_replicas_ < 5);
  replicas_[n_replicas_++] = this;
  epaxos_test_mutex_.unlock();
  #endif

  return sch_;
}

Communicator *EpaxosFrame::CreateCommo(PollMgr *poll) {
  if (commo_ == nullptr) {
    commo_ = new EpaxosCommo(poll);
  }

  #ifdef EPAXOS_TEST_CORO
  epaxos_test_mutex_.lock();
  verify(n_replicas_ == 5);
  n_commo_++;
  epaxos_test_mutex_.unlock();
  if (site_info_->locale_id == 0) {
    verify(epaxos_test_coro_.get() == nullptr);
    Log_debug("Creating Epaxos test coroutine");
    epaxos_test_coro_ = Coroutine::CreateRun([this] () {
      // Yield until all 5 communicators are initialized
      Coroutine::CurrentCoroutine()->Yield();
      // Run tests
      verify(n_replicas_ == 5);
      auto testconfig = new EpaxosTestConfig(replicas_);
      EpaxosLabTest test(testconfig);
      test.Run();
      test.Cleanup();
      // Turn off Reactor loop
      Reactor::GetReactor()->looping_ = false;
      return;
    });
    Log_info("epaxos_test_coro_ id=%d", epaxos_test_coro_->id);
    // wait until n_commo_ == 5, then resume the coroutine
    epaxos_test_mutex_.lock();
    while (n_commo_ < 5) {
      epaxos_test_mutex_.unlock();
      sleep(0.1);
      epaxos_test_mutex_.lock();
    }
    epaxos_test_mutex_.unlock();
    Reactor::GetReactor()->ContinueCoro(epaxos_test_coro_);
  }
  #endif

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

