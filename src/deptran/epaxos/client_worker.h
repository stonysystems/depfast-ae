#pragma once

#include "../client_worker.h"
#include "perf_test.h"
#include "test.h"

namespace janus {

class EpaxosClientWorker : public ClientWorker {

 public:
  using ClientWorker::ClientWorker;
  
  // This is called from a different thread.
  void Work() override {
    Coroutine::CreateRun([this] () {
      auto testconfig = new EpaxosTestConfig(EpaxosFrame::replicas_);
      #ifdef EPAXOS_TEST_CORO
      EpaxosTest test(testconfig);
      test.Run();
      #endif
      #ifdef EPAXOS_PERF_TEST_CORO
      EpaxosPerfTest perf_test(testconfig);
      perf_test.Run();
      #endif
      Coroutine::Sleep(10);
      testconfig->Shutdown();
      Reactor::GetReactor()->looping_ = false;
    });
    Reactor::GetReactor()->Loop(true, true);
  }
};

} // namespace janus
