#include "test.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

int EpaxosLabTest::Run(void) {
  config_->SetLearnerAction();
  uint64_t start_rpc = config_->RpcTotal();
  if (testBasicAgree()
      || testFastPathIndependentAgree()
      || testFastPathDependentAgree()
      || testSlowPathIndependentAgree()
      || testSlowPathDependentAgree()
      || testFailNoQuorum()
      || testPrepareCommittedCommand()
      || testConcurrentAgree()
      || testConcurrentUnreliableAgree()
    ) {
    Print("TESTS FAILED");
    return 1;
  }
  Print("ALL TESTS PASSED");
  Print("Total RPC count: %ld", config_->RpcTotal() - start_rpc);
  return 0;
}

void EpaxosLabTest::Cleanup(void) {
  config_->Shutdown();
}

#define Init2(test_id, description) \
  Init(test_id, description); \
  verify(config_->NDisconnected() == 0 && !config_->IsUnreliable())

#define InitSub2(sub_test_id, description) \
  InitSub(sub_test_id, description); \
  verify(config_->NDisconnected() == 0 && !config_->IsUnreliable())

#define Passed2() Passed(); return 0

#define Assert(expr) if (!(expr)) { \
  return 1; \
}

#define Assert2(expr, msg, ...) if (!(expr)) { \
  Failed(msg, ##__VA_ARGS__); \
  return 1; \
}

#define AssertNoneExecuted(cmd) { \
        auto nc = config_->NExecuted(cmd); \
        Assert2(nc == 0, "%d servers unexpectedly executed command %d", nc, cmd); \
      }

#define AssertNExecuted(cmd, expected) { \
        auto nc = config_->NExecuted(cmd); \
        Assert2(nc == expected, "%d servers executed command %d (%d expected)", nc, cmd, expected) \
      }

#define AssertValidCommitStatus(replica_id, instance_no, n, status) { \
          Assert2(r != -1, "failed to reach agreement for instance R%d.%d among %d servers, committed different commands", replica_id, instance_no, n); \
          Assert2(r != -2, "failed to reach agreement for instance R%d.%d among %d servers, committed different dkey", replica_id, instance_no, n); \
          Assert2(r != -3, "failed to reach agreement for instance R%d.%d among %d servers, committed different seq", replica_id, instance_no, n); \
          Assert2(r != -4, "failed to reach agreement for instance R%d.%d among %d servers, committed different deps", replica_id, instance_no, n); \
        }

#define AssertSuccessCommitStatus(replica_id, instance_no, n, status) { \
        Assert2(status != 0, "failed to reach agreement for instance R%d.%d among %d servers", replica_id, instance_no, n); \
        AssertValidCommitStatus(replica_id, instance_no, n, status); \
      }

#define AssertNCommitted(replica_id, instance_no, n) { \
        auto r = config_->NCommitted(replica_id, instance_no, n); \
        Assert2(r != 0, "failed to reach agreement for instance R%d.%d among %d servers", replica_id, instance_no, n); \
        AssertSuccessCommitStatus(replica_id, instance_no, n, r); \
      }

#define DoAgreeAndAssertNCommitted(cmd, dkey, n, no_op, exp_dkey, exp_seq, exp_deps) { \
        bool cno_op; \
        string cdkey; \
        uint64_t cseq; \
        unordered_map<uint64_t, uint64_t> cdeps; \
        auto r = config_->DoAgreement(cmd, dkey, n, false, &cno_op, &cdkey, &cseq, &cdeps); \
        Assert2(r != 0, "failed to reach agreement for command %d among %d servers", cmd, n); \
        Assert2(r != -1, "failed to reach agreement for command %d among %d servers, committed different commands", cmd, n); \
        Assert2(r != -2, "failed to reach agreement for command %d among %d servers, committed different dkey", cmd, n); \
        Assert2(r != -3, "failed to reach agreement for command %d among %d servers, committed different seq", cmd, n); \
        Assert2(r != -4, "failed to reach agreement for command %d among %d servers, committed different deps", cmd, n); \
        Assert2(cno_op == no_op || no_op, "failed to reach agreement for command %d among %d servers, expected no-op, got command", cmd, n); \
        Assert2(cno_op == no_op || !no_op, "failed to reach agreement for command %d among %d servers, expected command, got co-op", cmd, n); \
        Assert2(cdkey == exp_dkey, "failed to reach agreement for command %d among %d servers, expected dkey %s, got dkey %s", cmd, n, exp_dkey.c_str(), cdkey.c_str()); \
        Assert2(cseq == exp_seq, "failed to reach agreement for command %d among %d servers, expected seq %d, got seq %d", cmd, n, exp_seq, cseq); \
        Assert2(cdeps == exp_deps, "failed to reach agreement for command %d among %d servers, expected deps different from committed deps", cmd, n); \
      }
      
#define DoAgreeAndAssertNoneCommitted(cmd, dkey) { \
        bool cno_op; \
        string cdkey; \
        uint64_t cseq; \
        unordered_map<uint64_t, uint64_t> cdeps; \
        auto r = config_->DoAgreement(cmd, dkey, 1, false, &cno_op, &cdkey, &cseq, &cdeps); \
        Assert2(r == 0, "committed command %d without majority", cmd); \
      }

int EpaxosLabTest::testBasicAgree(void) {
  Init2(1, "Basic agreement");
  for (int i = 1; i <= 3; i++) {
    // complete 1 agreement and make sure its index is as expected
    int cmd = 100 + i;
    string dkey = to_string(cmd);
    // make sure no commits exist before any agreements are started
    AssertNoneExecuted(cmd);
    unordered_map<uint64_t, uint64_t> deps;
    DoAgreeAndAssertNCommitted(cmd, dkey, NSERVERS, false, dkey, 0, deps);
  }
  Passed2();
}

int EpaxosLabTest::testFastPathIndependentAgree(void) {
  Init2(2, "Fast path agreement of independent commands");
  config_->Disconnect(0);
  for (int i = 1; i <= 3; i++) {
    // complete 1 agreement and make sure its index is as expected
    int cmd = 200 + i;
    string dkey = to_string(cmd);
    // make sure no commits exist before any agreements are started
    AssertNoneExecuted(cmd);
    unordered_map<uint64_t, uint64_t> deps;
    DoAgreeAndAssertNCommitted(cmd, dkey, FAST_PATH_QUORUM, false, dkey, 0, deps);
  }
  // Reconnect all
  config_->Reconnect(0);
  // TODO: Check if committed through prepare
  Passed2();
}

int EpaxosLabTest::testFastPathDependentAgree(void) {
  Init2(3, "Fast path agreement of dependent commands");
  config_->Disconnect(0);
  // Round 1
  int cmd = 301;
  string dkey = "300";
  uint64_t seq = 0;
  unordered_map<uint64_t, uint64_t> deps;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  // Round 2
  cmd++;
  seq++;
  deps[1] = 3;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  // Round 3
  cmd++;
  seq++;
  deps[1] = 4;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  // Reconnect all
  config_->Reconnect(0);
  // TODO: Check if committed through prepare
  Passed2();
}

int EpaxosLabTest::testSlowPathIndependentAgree(void) {
  Init2(4, "Slow path agreement of independent commands");
  config_->Disconnect(0);
  config_->Disconnect(1);
  for (int i = 1; i <= 3; i++) {
    // complete 1 agreement and make sure its index is as expected
    int cmd = 400 + i;
    string dkey = to_string(cmd);
    // make sure no commits exist before any agreements are started
    AssertNoneExecuted(cmd);
    unordered_map<uint64_t, uint64_t> deps;
    DoAgreeAndAssertNCommitted(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, 0, deps);
  }
  // Reconnect all
  config_->Reconnect(0);
  config_->Reconnect(1);
  // TODO: Check if committed through prepare
  Passed2();
}

int EpaxosLabTest::testSlowPathDependentAgree(void) {
  Init2(5, "Slow path agreement of dependent commands");
  config_->Disconnect(0);
  config_->Disconnect(1);
  // Round 1
  int cmd = 501;
  string dkey = "500";
  uint64_t seq = 0;
  unordered_map<uint64_t, uint64_t> deps;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  // Round 2
  cmd++;
  seq++;
  deps[2] = 3;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  // Round 3
  cmd++;
  seq++;
  deps[2] = 4;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommitted(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  // Reconnect all
  config_->Reconnect(0);
  config_->Reconnect(1);
  // TODO: Check if committed through prepare
  Passed2();
}

int EpaxosLabTest::testFailNoQuorum(void) {
  Init2(6, "No agreement if too many servers disconnect");
  config_->Disconnect(0);
  config_->Disconnect(1);
  config_->Disconnect(2);
  for (int i = 1; i <= 3; i++) {
    // complete 1 agreement and make sure its index is as expected
    int cmd = 600 + i;
    string dkey = to_string(cmd);
    // make sure no commits exist before any agreements are started
    AssertNoneExecuted(cmd);
    DoAgreeAndAssertNoneCommitted(cmd, dkey);
  }
  // Reconnect all
  config_->Reconnect(0);
  config_->Reconnect(1);
  config_->Reconnect(2);
  // TODO: Check if committed through prepare
  Passed2();
}

int EpaxosLabTest::testPrepareCommittedCommand(void) {
  Init2(7, "Commit through prepare - committed command");
  InitSub2(1, "Committed (via fast path) in 1 server (leader)");
  int cmd = 701;
  string dkey = to_string(cmd);
  uint64_t replica_id, instance_no;
  int time_to_sleep = 1000, diff = 200;
  config_->Disconnect(0);
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(1, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect(4);
    config_->Disconnect(3);
    config_->Disconnect(2);
    config_->Disconnect(1);
    auto nc = config_->NCommitted(replica_id, instance_no, 1);
    verify(nc <= FAST_PATH_QUORUM);
    if (nc == 1) {
      break;
    }
    if (nc < 1) {
      time_to_sleep += diff;
    } else {
      time_to_sleep -= diff;
    }
    cmd++;
    dkey = to_string(cmd);
    config_->Reconnect(1);
    config_->Reconnect(2);
    config_->Reconnect(3);
    config_->Reconnect(4);
  }
  config_->Reconnect(0);
  config_->Reconnect(2);
  config_->Reconnect(3);
  config_->Reconnect(4);
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS);
  config_->Reconnect(1);

  InitSub2(2, "Committed (via fast path) in 2 servers (leader and one replica)");
  cmd++;
  dkey = to_string(cmd);
  config_->Disconnect(0);
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(1, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect(4);
    config_->Disconnect(3);
    config_->Disconnect(2);
    config_->Disconnect(1);
    auto nc = config_->NCommitted(replica_id, instance_no, 2);
    verify(nc <= FAST_PATH_QUORUM);
    if (nc == 2) {
      break;
    }
    if (nc < 2) {
      time_to_sleep += diff;
    } else {
      time_to_sleep -= diff;
    }
    cmd++;
    dkey = to_string(cmd);
    config_->Reconnect(1);
    config_->Reconnect(2);
    config_->Reconnect(3);
    config_->Reconnect(4);
  }
  config_->Reconnect(0);
  config_->Reconnect(2);
  config_->Reconnect(3);
  config_->Reconnect(4);
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS);
  config_->Reconnect(1);

  InitSub2(3, "Committed (via slow path) in 1 server (leader)");
  config_->Disconnect(0);
  config_->Disconnect(1);
  cmd++;
  dkey = to_string(cmd);
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(2, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect(4);
    config_->Disconnect(3);
    config_->Disconnect(2);
    auto nc = config_->NCommitted(replica_id, instance_no, 1);
    verify(nc <= SLOW_PATH_QUORUM);
    if (nc == 1) {
      break;
    }
    if (nc < 1) {
      time_to_sleep += diff;
    } else {
      time_to_sleep -= diff;
    }
    cmd++;
    dkey = to_string(cmd);
    config_->Reconnect(2);
    config_->Reconnect(3);
    config_->Reconnect(4);
  }
  config_->Reconnect(0);
  config_->Reconnect(1);
  config_->Reconnect(3);
  config_->Reconnect(4);
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS);
  config_->Reconnect(2);

  InitSub2(4, "Committed (via slow path) in 2 servers (leader and one replica)");
  config_->Disconnect(0);
  config_->Disconnect(1);
  cmd++;
  dkey = to_string(cmd);
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(2, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect(4);
    config_->Disconnect(3);
    config_->Disconnect(2);
    auto nc = config_->NCommitted(replica_id, instance_no, 2);
    verify(nc <= SLOW_PATH_QUORUM);
    if (nc == 2) {
      break;
    }
    if (nc < 2) {
      time_to_sleep += diff;
    } else {
      time_to_sleep -= diff;
    }
    cmd++;
    dkey = to_string(cmd);
    config_->Reconnect(2);
    config_->Reconnect(3);
    config_->Reconnect(4);
  }
  config_->Reconnect(0);
  config_->Reconnect(3);
  config_->Reconnect(4);
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS-1);
  config_->Reconnect(1);
  config_->Prepare(1, replica_id, instance_no);
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS);
  config_->Reconnect(2);
  Passed2();
}

int EpaxosLabTest::testPrepareAcceptedCommand(void) {
  Passed2();
}

int EpaxosLabTest::testPrepareIdenticallyPreAcceptedCommand(void) {
  Passed2();
}

int EpaxosLabTest::testPreparePreAcceptedCommand(void) {
  Passed2();
}

int EpaxosLabTest::testPrepareNoopCommand(void) {
  Passed2();
}

class CAArgs {
 public:
  int cmd;
  int svr;
  string dkey;
  std::mutex *mtx;
  std::vector<std::pair<uint64_t, uint64_t>> *retvals;
  EpaxosTestConfig *config;
};

static void *doConcurrentAgreement(void *args) {
  CAArgs *caargs = (CAArgs *)args;
  uint64_t replica_id, instance_no;
  caargs->config->Start(caargs->svr, caargs->cmd, caargs->dkey, &replica_id, &instance_no);
  std::lock_guard<std::mutex> lock(*(caargs->mtx));
  caargs->retvals->push_back(make_pair(replica_id, instance_no));
  return nullptr;
}

int EpaxosLabTest::testConcurrentAgree(void) {
  Init2(7, "Concurrent agreements");
  std::vector<pthread_t> threads{};
  std::vector<std::pair<uint64_t, uint64_t>> retvals{};
  std::mutex mtx{};
  for (int iter = 1; iter <= 50; iter++) {
    for (int svr = 0; svr < NSERVERS; svr++) {
      CAArgs *args = new CAArgs{};
      args->cmd = svr * 1000 + iter;
      args->svr = svr;
      args->dkey = "1000";
      args->mtx = &mtx;
      args->retvals = &retvals;
      args->config = config_;
      pthread_t thread;
      verify(pthread_create(&thread,
                            nullptr,
                            doConcurrentAgreement,
                            (void*)args) == 0);
      threads.push_back(thread);
    }
  }
  // join all threads
  for (auto thread : threads) {
    verify(pthread_join(thread, nullptr) == 0);
  }
  Assert2(retvals.size() == 250, "Failed to reach agreement");
  for (auto retval : retvals) {
    AssertNCommitted(retval.first, retval.second, NSERVERS);
  }
  Passed2();
}

int EpaxosLabTest::testConcurrentUnreliableAgree(void) {
  Init2(8, "Unreliable concurrent agreement (takes a few minutes)");
  config_->SetUnreliable(true);
  std::vector<pthread_t> threads{};
  std::vector<std::pair<uint64_t, uint64_t>> retvals{};
  std::mutex mtx{};
  for (int iter = 1; iter <= 50; iter++) {
    for (int svr = 0; svr < NSERVERS; svr++) {
      CAArgs *args = new CAArgs{};
      args->cmd = 5000 + (svr * 1000) + iter;
      args->svr = svr;
      args->dkey = "5000";
      args->mtx = &mtx;
      args->retvals = &retvals;
      args->config = config_;
      pthread_t thread;
      verify(pthread_create(&thread,
                            nullptr,
                            doConcurrentAgreement,
                            (void*)args) == 0);
      threads.push_back(thread);
    }
  }
  // join all threads
  for (auto thread : threads) {
    verify(pthread_join(thread, nullptr) == 0);
  }
  Coroutine::Sleep(5000000);
  config_->SetUnreliable(false);
  Coroutine::Sleep(5000000);
  config_->PrepareAllUncommitted();
  Coroutine::Sleep(10000000);
  for (auto retval : retvals) {
    AssertNCommitted(retval.first, retval.second, NSERVERS);
  }
  Passed2();
}

// class CSArgs {
//  public:
//   std::vector<uint64_t> *indices;
//   std::mutex *mtx;
//   int i;
//   int leader;
//   uint64_t term;
//   EpaxosTestConfig *config;
// };

// static void *doConcurrentStarts(void *args) {
//   CSArgs *csargs = (CSArgs *)args;
//   uint64_t idx, tm;
//   auto ok = csargs->config->Start(csargs->leader, 701 + csargs->i, &idx, &tm);
//   if (!ok || tm != csargs->term) {
//     return nullptr;
//   }
//   {
//     std::lock_guard<std::mutex> lock(*(csargs->mtx));
//     csargs->indices->push_back(idx);
//   }
//   return nullptr;
// }

// int EpaxosLabTest::testConcurrentStarts(void) {
//   Init2(7, "Concurrently started agreements");
//   int nconcurrent = 5;
//   bool success = false;
//   for (int again = 0; again < 5; again++) {
//     if (again > 0) {
//       wait(3000000);
//     }
//     auto leader = config_->OneLeader();
//     AssertOneLeader(leader);
//     uint64_t index, term;
//     auto ok = config_->Start(leader, 701, &index, &term);
//     if (!ok) {
//       continue; // retry (up to 5 times)
//     }
//     // create 5 threads that each Start a command to leader
//     std::vector<uint64_t> indices{};
//     std::vector<int> cmds{};
//     std::mutex mtx{};
//     pthread_t threads[nconcurrent];
//     for (int i = 0; i < nconcurrent; i++) {
//       CSArgs *args = new CSArgs{};
//       args->indices = &indices;
//       args->mtx = &mtx;
//       args->i = i;
//       args->leader = leader;
//       args->term = term;
//       args->config = config_;
//       verify(pthread_create(&threads[i], nullptr, doConcurrentStarts, (void*)args) == 0);
//     }
//     // join all threads
//     for (int i = 0; i < nconcurrent; i++) {
//       verify(pthread_join(threads[i], nullptr) == 0);
//     }
//     if (config_->TermMovedOn(term)) {
//       goto skip; // if leader's term is expiring, start over
//     }
//     // wait for all indices to commit
//     for (auto index : indices) {
//       int cmd = config_->Wait(index, NSERVERS, term);
//       if (cmd < 0) {
//         AssertWaitNoError(cmd, index);
//         goto skip; // on timeout and term changes, try again
//       }
//       cmds.push_back(cmd);
//     }
//     // make sure all the commits are there with the correct values
//     for (int i = 0; i < nconcurrent; i++) {
//       auto val = 701 + i;
//       int j;
//       for (j = 0; j < cmds.size(); j++) {
//         if (cmds[j] == val) {
//           break;
//         }
//       }
//       Assert2(j < cmds.size(), "cmd %d missing", val);
//     }
//     success = true;
//     break;
//     skip: ;
//   }
//   Assert2(success, "too many term changes and/or delayed responses");
//   index_ += nconcurrent + 1;
//   Passed2();
// }

// int EpaxosLabTest::testBackup(void) {
//   Init2(8, "Leader backs up quickly over incorrect follower logs");
//   // disconnect 3 servers that are not the leader
//   int leader1 = config_->OneLeader();
//   AssertOneLeader(leader1);
//   Log_debug("disconnect 3 followers");
//   config_->Disconnect((leader1 + 2) % NSERVERS);
//   config_->Disconnect((leader1 + 3) % NSERVERS);
//   config_->Disconnect((leader1 + 4) % NSERVERS);
//   // Start() a bunch of commands that won't be committed
//   uint64_t index, term;
//   for (int i = 0; i < 50; i++) {
//     AssertStartOk(config_->Start(leader1, 800 + i, &index, &term));
//   }
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   // disconnect the leader and its 1 follower, then reconnect the 3 servers
//   Log_debug("disconnect the leader and its 1 follower, reconnect the 3 followers");
//   config_->Disconnect((leader1 + 1) % NSERVERS);
//   config_->Disconnect(leader1);
//   config_->Reconnect((leader1 + 2) % NSERVERS);
//   config_->Reconnect((leader1 + 3) % NSERVERS);
//   config_->Reconnect((leader1 + 4) % NSERVERS);
//   // do a bunch of agreements among the new quorum
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   Log_debug("try to commit a lot of commands");
//   for (int i = 1; i <= 50; i++) {
//     DoAgreeAndAssertIndex(800 + i, NSERVERS - 2, index_++);
//   }
//   // reconnect the old leader and its follower
//   Log_debug("reconnect the old leader and the follower");
//   config_->Reconnect((leader1 + 1) % NSERVERS);
//   config_->Reconnect(leader1);
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   // do an agreement all together to check the old leader's incorrect
//   // entries are replaced in a timely manner
//   int leader2 = config_->OneLeader();
//   AssertOneLeader(leader2);
//   AssertStartOk(config_->Start(leader2, 851, &index, &term));
//   index_++;
//   // 10 seconds should be enough to back up 50 incorrect logs
//   Coroutine::Sleep(2*ELECTIONTIMEOUT);
//   Log_debug("check if the old leader has enough committed");
//   AssertNCommitted(index, NSERVERS);
//   Passed2();
// }

// int EpaxosLabTest::testCount(void) {
//   Init2(9, "RPC counts aren't too high");

//   // reset RPC counts before starting
//   for (int i = 0; i < NSERVERS; i++) {
//     config_->RpcCount(i, true);
//   }

//   auto rpcs = [this]() {
//     uint64_t total = 0;
//     for (int i = 0; i < NSERVERS; i++) {
//       total += config_->RpcCount(i);
//     }
//     return total;
//   };

//   // initial election RPC count
//   Assert2(init_rpcs_ > 1 && init_rpcs_ <= 30,
//           "too many or too few RPCs (%ld) to elect initial leader",
//           init_rpcs_);

//   // agreement RPC count
//   int iters = 10;
//   uint64_t total = -1;
//   bool success = false;
//   for (int again = 0; again < 5; again++) {
//     if (again > 0) {
//       wait(3000000);
//     }
//     auto leader = config_->OneLeader();
//     AssertOneLeader(leader);
//     rpcs();
//     uint64_t index, term, startindex, startterm;
//     auto ok = config_->Start(leader, 900, &startindex, &startterm);
//     if (!ok) {
//       // leader moved on quickly: start over
//       continue;
//     }
//     for (int i = 1; i <= iters; i++) {
//       ok = config_->Start(leader, 900 + i, &index, &term);
//       if (!ok || term != startterm) {
//         // no longer the leader and/or term changed: start over
//         goto loop;
//       }
//       Assert2(index == (startindex + i), "Start() failed");
//     }
//     for (int i = 1; i <= iters; i++) {
//       auto r = config_->Wait(startindex + i, NSERVERS, startterm);
//       AssertWaitNoError(r, startindex + i);
//       if (r < 0) {
//         // timeout or term change: start over
//         goto loop;
//       }
//       Assert2(r == (900 + i), "wrong value %d committed for index %ld: expected %d", r, startindex + i, 900 + i);
//     }
//     if (config_->TermMovedOn(startterm)) {
//       // term changed -- can't expect low RPC counts: start over
//       continue;
//     }
//     total = rpcs();
//     Assert2(total <= COMMITRPCS(iters),
//             "too many RPCs (%ld) for %d entries",
//             total, iters);
//     success = true;
//     break;
//     loop: ;
//   }
//   Assert2(success, "term changed too often");

//   // idle RPC count
//   wait(1000000);
//   total = rpcs();
//   Assert2(total <= 60,
//           "too many RPCs (%ld) for 1 second of idleness",
//           total);
//   Passed2();
// }

// int EpaxosLabTest::testUnreliableAgree(void) {
//   Init2(10, "Unreliable agreement (takes a few minutes)");
//   config_->SetUnreliable(true);
//   std::vector<pthread_t> threads{};
//   std::vector<uint64_t> retvals{};
//   std::mutex mtx{};
//   for (int iter = 1; iter < 50; iter++) {
//     for (int i = 0; i < 4; i++) {
//       CAArgs *args = new CAArgs{};
//       args->iter = iter;
//       args->i = i;
//       args->mtx = &mtx;
//       args->retvals = &retvals;
//       args->config = config_;
//       pthread_t thread;
//       verify(pthread_create(&thread,
//                             nullptr,
//                             doConcurrentAgreement,
//                             (void*)args) == 0);
//       threads.push_back(thread);
//     }
//     if (retvals.size() > 0)
//       break;
//     if (config_->DoAgreement(1000 + iter, 1, true) == 0) {
//       std::lock_guard<std::mutex> lock(mtx);
//       retvals.push_back(0);
//       break;
//     }
//   }
//   config_->SetUnreliable(false);
//   // join all threads
//   for (auto thread : threads) {
//     verify(pthread_join(thread, nullptr) == 0);
//   }
//   Assert2(retvals.size() == 0, "Failed to reach agreement");
//   index_ += 50 * 5;
//   DoAgreeAndAssertWaitSuccess(1060, NSERVERS);
//   Passed2();
// }

// int EpaxosLabTest::testFigure8(void) {
//   Init2(11, "Figure 8");
//   bool success = false;
//   // Leader should not determine commitment using log entries from previous terms
//   for (int again = 0; again < 10; again++) {
//     // find out initial leader (S1) and term
//     auto leader1 = config_->OneLeader();
//     AssertOneLeader(leader1);
//     uint64_t index1, term1, index2, term2;
//     auto ok = config_->Start(leader1, 1100, &index1, &term1);
//     if (!ok) {
//       continue; // term moved on too quickly: start over
//     }
//     auto r = config_->Wait(index1, NSERVERS, term1);
//     AssertWaitNoError(r, index1);
//     AssertWaitNoTimeout(r, index1, NSERVERS);
//     index_ = index1;
//     // Start() a command (C1) and only let it get replicated to 1 follower (S2)
//     config_->Disconnect((leader1 + 1) % NSERVERS);
//     config_->Disconnect((leader1 + 2) % NSERVERS);
//     config_->Disconnect((leader1 + 3) % NSERVERS);
//     ok = config_->Start(leader1, 1101, &index1, &term1);
//     if (!ok) {
//       config_->Reconnect((leader1 + 1) % NSERVERS);
//       config_->Reconnect((leader1 + 2) % NSERVERS);
//       config_->Reconnect((leader1 + 3) % NSERVERS);
//       continue;
//     }
//     Coroutine::Sleep(ELECTIONTIMEOUT);
//     // C1 is at index i1 for S1 and S2
//     AssertNoneCommitted(index1);
//     // Elect new leader (S3) among other 3 servers
//     config_->Disconnect((leader1 + 4) % NSERVERS);
//     config_->Disconnect(leader1);
//     config_->Reconnect((leader1 + 1) % NSERVERS);
//     config_->Reconnect((leader1 + 2) % NSERVERS);
//     config_->Reconnect((leader1 + 3) % NSERVERS);
//     auto leader2 = config_->OneLeader();
//     AssertOneLeader(leader2);
//     // let old leader (S1) and follower (S2) become a follower in the new term
//     config_->Reconnect((leader1 + 4) % NSERVERS);
//     config_->Reconnect(leader1);
//     Coroutine::Sleep(ELECTIONTIMEOUT);
//     AssertOneLeader(config_->OneLeader(leader2));
//     Log_debug("disconnect all followers and Start() a cmd (C2) to isolated new leader");
//     for (int i = 0; i < NSERVERS; i++) {
//       if (i != leader2) {
//         config_->Disconnect(i);
//       }
//     }
//     ok = config_->Start(leader2, 1102, &index2, &term2);
//     if (!ok) {
//       for (int i = 1; i < 5; i++) {
//         config_->Reconnect((leader2 + i) % NSERVERS);
//       }
//       continue;
//     }
//     // C2 is at index i1 for S3, C1 still at index i1 for S1 & S2
//     Assert2(index2 == index1, "Start() returned index %ld (%ld expected)", index2, index1);
//     Assert2(term2 > term1, "Start() returned term %ld (%ld expected)", term2, term1);
//     Coroutine::Sleep(ELECTIONTIMEOUT);
//     AssertNoneCommitted(index1);
//     // Let first leader (S1) or its initial follower (S2) become next leader
//     config_->Disconnect(leader2);
//     config_->Reconnect(leader1);
//     verify((leader1 + 4) % NSERVERS != leader2);
//     config_->Reconnect((leader1 + 4) % NSERVERS);
//     if (leader2 == leader1 + 1)
//       config_->Reconnect((leader1 + 2) % NSERVERS);
//     else
//       config_->Reconnect((leader1 + 1) % NSERVERS);
//     auto leader3 = config_->OneLeader();
//     AssertOneLeader(leader3);
//     if (leader3 != leader1 && leader3 != ((leader1 + 4) % NSERVERS)) {
//       continue; // failed this step with a 1/3 chance. just start over until success.
//     }
//     // give leader3 more than enough time to replicate index1 to a third server
//     Coroutine::Sleep(ELECTIONTIMEOUT);
//     // Make sure initial Start() value isn't getting committed at this point
//     AssertNoneCommitted(index1);
//     // Commit a new index in the current term
//     Assert2(config_->DoAgreement(1103, NSERVERS - 2, false) > index1,
//             "failed to reach agreement");
//     // Make sure that C1 is committed for index i1 now
//     AssertNCommitted(index1, NSERVERS - 2);
//     Assert2(config_->ServerCommitted(leader3, index1, 1101),
//             "value 1101 is not committed at index %ld when it should be", index1);
//     success = true;
//     // Reconnect all servers
//     config_->Reconnect((leader1 + 3) % NSERVERS);
//     if (leader2 == leader1 + 1)
//       config_->Reconnect((leader1 + 1) % NSERVERS);
//     else
//       config_->Reconnect((leader1 + 2) % NSERVERS);
//     break;
//   }
//   Assert2(success, "Failed to test figure 8");
//   Passed2();
// }

// void EpaxosLabTest::wait(uint64_t microseconds) {
//   Reactor::CreateSpEvent<TimeoutEvent>(microseconds)->Wait();
// }

#endif

}
