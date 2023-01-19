#include "testconf.h"
#include "marshallable.h"
#include "../classic/tpc_command.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

int _test_id_g = 0;

EpaxosFrame **EpaxosTestConfig::replicas = nullptr;
std::function<void(Marshallable &)> EpaxosTestConfig::commit_callbacks[NSERVERS];
std::vector<int> EpaxosTestConfig::committed_cmds[NSERVERS];
uint64_t EpaxosTestConfig::rpc_count_last[NSERVERS];

EpaxosTestConfig::EpaxosTestConfig(EpaxosFrame **replicas) {
  verify(EpaxosTestConfig::replicas == nullptr);
  EpaxosTestConfig::replicas = replicas;
  for (int i = 0; i < NSERVERS; i++) {
    EpaxosTestConfig::replicas[i]->server()->rep_frame_ = EpaxosTestConfig::replicas[i]->server()->frame_;
    EpaxosTestConfig::committed_cmds[i].push_back(-1);
    EpaxosTestConfig::rpc_count_last[i] = 0;
    disconnected_[i] = false;
  }
  th_ = std::thread([this](){ netctlLoop(); });
}

void EpaxosTestConfig::SetLearnerAction(void) {
  for (int i = 0; i < NSERVERS; i++) {
    EpaxosTestConfig::commit_callbacks[i] = [i](Marshallable& cmd) {
      verify(cmd.kind_ == MarshallDeputy::CMD_TPC_COMMIT);
      auto& command = dynamic_cast<TpcCommitCommand&>(cmd);
      Log_debug("server %d committed value %d", i, command.tx_id_);
      EpaxosTestConfig::committed_cmds[i].push_back(command.tx_id_);
    };
    EpaxosTestConfig::replicas[i]->server()->RegLearnerAction(EpaxosTestConfig::commit_callbacks[i]);
  }
}

// bool EpaxosTestConfig::TermMovedOn(uint64_t term) {
//   for (int i = 0; i < NSERVERS; i++) {
//     uint64_t curTerm;
//     bool isLeader;
//     EpaxosTestConfig::replicas[i]->server()->GetState(&isLeader, &curTerm);
//     if (curTerm > term) {
//       return true;
//     }
//   }
//   return false;
// }

// uint64_t EpaxosTestConfig::OneTerm(void) {
//   uint64_t term, curTerm;
//   bool isLeader;
//   EpaxosTestConfig::replicas[0]->server()->GetState(&isLeader, &term);
//   for (int i = 1; i < NSERVERS; i++) {
//     EpaxosTestConfig::replicas[i]->server()->GetState(&isLeader, &curTerm);
//     if (curTerm != term) {
//       return -1;
//     }
//   }
//   return term;
// }

int EpaxosTestConfig::NCommitted(uint64_t tx_id) {
  int cmd, n = 0;
  for (int i = 0; i < NSERVERS; i++) {
    auto cmd = std::find(EpaxosTestConfig::committed_cmds[i].begin(), EpaxosTestConfig::committed_cmds[i].end(), tx_id);
    if (cmd != EpaxosTestConfig::committed_cmds[i].end()) {
      n++;
    }
  }
  return n;
}

void EpaxosTestConfig::Start(int svr, int cmd, string dkey) {
  // Construct an empty TpcCommitCommand containing cmd as its tx_id_
  auto cmdptr = std::make_shared<TpcCommitCommand>();
  auto vpd_p = std::make_shared<VecPieceData>();
  vpd_p->sp_vec_piece_data_ = std::make_shared<vector<shared_ptr<SimpleCommand>>>();
  cmdptr->tx_id_ = cmd;
  cmdptr->cmd_ = vpd_p;
  auto cmdptr_m = dynamic_pointer_cast<Marshallable>(cmdptr);
  // call Start()
  Log_debug("Starting agreement on svr %d for cmd id %d", svr, cmdptr->tx_id_);
  return EpaxosTestConfig::replicas[svr]->server()->Start(cmdptr_m, dkey);
}

// int EpaxosTestConfig::Wait(uint64_t index, int n, uint64_t term) {
//   int nc = 0, i;
//   auto to = 10000; // 10 milliseconds
//   for (i = 0; i < 30; i++) {
//     nc = NCommitted(index);
//     if (nc < 0) {
//       return -3; // values differ
//     } else if (nc >= n) {
//       break;
//     }
//     Reactor::CreateSpEvent<TimeoutEvent>(to)->Wait();
//     if (to < 1000000) {
//       to *= 2;
//     }
//     if (TermMovedOn(term)) {
//       return -2; // term changed
//     }
//   }
//   if (i == 30) {
//     return -1; // timeout
//   }
//   for (int i = 0; i < NSERVERS; i++) {
//     if (EpaxosTestConfig::committed_cmds[i].size() > index) {
//       return EpaxosTestConfig::committed_cmds[i][index];
//     }
//   }
//   verify(0);
// }

bool EpaxosTestConfig::DoAgreement(int cmd, string dkey, int n, bool retry) {
  Log_debug("Doing 1 round of Epaxos agreement");
  auto start = chrono::steady_clock::now();
  while ((chrono::steady_clock::now() - start) < chrono::seconds{10}) {
    usleep(50000);
    // Call Start() to all servers until alive command leader is found
    for (int i = 0; i < NSERVERS; i++) {
      // skip disconnected servers
      if (EpaxosTestConfig::replicas[i]->server()->IsDisconnected())
        continue;
      Start(i, cmd, dkey);
      Log_debug("starting cmd ldr=%d cmd=%d", EpaxosTestConfig::replicas[i]->server()->loc_id_, cmd); // TODO: Print instance and ballot
      break;
    }
    // If Start() successfully called, wait for agreement
    auto start2 = chrono::steady_clock::now();
    int nc;
    while ((chrono::steady_clock::now() - start2) < chrono::seconds{10}) {
      nc = NCommitted(cmd);
      verify(nc >= 0);
      if (nc >= n) {
        for (int i = 0; i < NSERVERS; i++) {
          auto cmd2 = std::find(EpaxosTestConfig::committed_cmds[i].begin(), EpaxosTestConfig::committed_cmds[i].end(), cmd);
          if (cmd2 != EpaxosTestConfig::committed_cmds[i].end()) {
            Log_debug("found commit log");
            return true;
            break;
          }
        }
        break;
      }
      usleep(20000);
    }
    Log_debug("%d committed server", nc);
    if (!retry) {
      Log_debug("failed to reach agreement");
      return false;
    }
  }
  Log_debug("Failed to reach agreement end");
  return false;
}

void EpaxosTestConfig::Disconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(!disconnected_[svr]);
  disconnect(svr, true);
  disconnected_[svr] = true;
}

void EpaxosTestConfig::Reconnect(int svr) {
  verify(svr >= 0 && svr < NSERVERS);
  std::lock_guard<std::mutex> lk(disconnect_mtx_);
  verify(disconnected_[svr]);
  reconnect(svr);
  disconnected_[svr] = false;
}

int EpaxosTestConfig::NDisconnected(void) {
  int count = 0;
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i])
      count++;
  }
  return count;
}

void EpaxosTestConfig::SetUnreliable(bool unreliable) {
  std::unique_lock<std::mutex> lk(cv_m_);
  verify(!finished_);
  if (unreliable) {
    verify(!unreliable_);
    // lk acquired cv_m_ in state 1 or 0
    unreliable_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  } else {
    verify(unreliable_);
    // lk acquired cv_m_ in state 2 or 0
    unreliable_ = false;
    // wait until netctlLoop moves cv_m_ from state 2 (or 0) to state 1,
    // restoring the network to reliable state in the process.
    lk.unlock();
    lk.lock();
  }
}

bool EpaxosTestConfig::IsUnreliable(void) {
  return unreliable_;
}

void EpaxosTestConfig::Shutdown(void) {
  // trigger netctlLoop shutdown
  {
    std::unique_lock<std::mutex> lk(cv_m_);
    verify(!finished_);
    // lk acquired cv_m_ in state 0, 1, or 2
    finished_ = true;
    // if cv_m_ was in state 1, must signal cv_ to wake up netctlLoop
    lk.unlock();
    cv_.notify_one();
  }
  // wait for netctlLoop thread to exit
  th_.join();
  // Reconnect() all Deconnect()ed servers
  for (int i = 0; i < NSERVERS; i++) {
    if (disconnected_[i]) {
      Reconnect(i);
    }
  }
}

uint64_t EpaxosTestConfig::RpcCount(int svr, bool reset) {
  std::lock_guard<std::recursive_mutex> lk(
    EpaxosTestConfig::replicas[svr]->commo()->rpc_mtx_);
  uint64_t count = EpaxosTestConfig::replicas[svr]->commo()->rpc_count_;
  uint64_t count_last = EpaxosTestConfig::rpc_count_last[svr];
  if (reset) {
    EpaxosTestConfig::rpc_count_last[svr] = count;
  }
  verify(count >= count_last);
  return count - count_last;
}

uint64_t EpaxosTestConfig::RpcTotal(void) {
  uint64_t total = 0;
  for (int i = 0; i < NSERVERS; i++) {
    total += EpaxosTestConfig::replicas[i]->commo()->rpc_count_;
  }
  return total;
}

bool EpaxosTestConfig::ServerCommitted(int svr, uint64_t index, int cmd) {
  if (EpaxosTestConfig::committed_cmds[svr].size() <= index)
    return false;
  return EpaxosTestConfig::committed_cmds[svr][index] == cmd;
}

void EpaxosTestConfig::netctlLoop(void) {
  int i;
  bool isdown;
  // cv_m_ unlocked state 0 (finished_ == false)
  std::unique_lock<std::mutex> lk(cv_m_);
  while (!finished_) {
    if (!unreliable_) {
      {
        std::lock_guard<std::mutex> prlk(disconnect_mtx_);
        // unset all unreliable-related disconnects and slows
        for (i = 0; i < NSERVERS; i++) {
          if (!disconnected_[i]) {
            reconnect(i, true);
            slow(i, 0);
          }
        }
      }
      // sleep until unreliable_ or finished_ is set
      // cv_m_ unlocked state 1 (unreliable_ == false && finished_ == false)
      cv_.wait(lk, [this](){ return unreliable_ || finished_; });
      continue;
    }
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      for (i = 0; i < NSERVERS; i++) {
        // skip server if it was disconnected using Disconnect()
        if (disconnected_[i]) {
          continue;
        }
        // server has DOWNRATE_N / DOWNRATE_D chance of being down
        if ((rand() % DOWNRATE_D) < DOWNRATE_N) {
          // disconnect server if not already disconnected in the previous period
          disconnect(i, true);
        } else {
          // Server not down: random slow timeout
          // Reconnect server if it was disconnected in the previous period
          reconnect(i, true);
          // server's slow timeout should be btwn 0-(MAXSLOW-1) ms
          slow(i, rand() % MAXSLOW);
        }
      }
    }
    // change unreliable state every 0.1s
    usleep(100000);
    // Coroutine::Sleep(100000);
    lk.unlock();
    // cv_m_ unlocked state 2 (unreliable_ == true && finished_ == false)
    lk.lock();
  }
  // If network is still unreliable, unset it
  if (unreliable_) {
    unreliable_ = false;
    {
      std::lock_guard<std::mutex> prlk(disconnect_mtx_);
      // unset all unreliable-related disconnects and slows
      for (i = 0; i < NSERVERS; i++) {
        if (!disconnected_[i]) {
          reconnect(i, true);
          slow(i, 0);
        }
      }
    }
  }
  // cv_m_ unlocked state 3 (unreliable_ == false && finished_ == true)
}

bool EpaxosTestConfig::isDisconnected(int svr) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  return EpaxosTestConfig::replicas[svr]->server()->IsDisconnected();
}

void EpaxosTestConfig::disconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (!isDisconnected(svr)) {
    // simulate disconnected server
    EpaxosTestConfig::replicas[svr]->server()->Disconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void EpaxosTestConfig::reconnect(int svr, bool ignore) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  if (isDisconnected(svr)) {
    // simulate reconnected server
    EpaxosTestConfig::replicas[svr]->server()->Reconnect();
  } else if (!ignore) {
    verify(0);
  }
}

void EpaxosTestConfig::slow(int svr, uint32_t msec) {
  std::lock_guard<std::recursive_mutex> lk(connection_m_);
  verify(!isDisconnected(svr));
  EpaxosTestConfig::replicas[svr]->commo()->rpc_poll_->slow(msec * 1000);
}

EpaxosServer *EpaxosTestConfig::GetServer(int svr) {
  return EpaxosTestConfig::replicas[svr]->server();
}

#endif

}
