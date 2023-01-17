#include "test.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

// #define TEST_EXPAND(x) x || x || x || x || x 
#define TEST_EXPAND(x) x 

int EpaxosLabTest::Run(void) {
  config_->SetLearnerAction();
  uint64_t start_rpc = config_->RpcTotal();
  if (testBasicAgree()
//       || TEST_EXPAND(testFailAgree())
//       || TEST_EXPAND(testFailNoAgree())
//       || TEST_EXPAND(testRejoin())
//       || TEST_EXPAND(testConcurrentStarts())
//       || TEST_EXPAND(testBackup())
//       || TEST_EXPAND(testCount())
//       || TEST_EXPAND(testUnreliableAgree())
//       || TEST_EXPAND(testFigure8())
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

#define Passed2() Passed(); return 0

#define Assert(expr) if (!(expr)) { \
  return 1; \
}

#define Assert2(expr, msg, ...) if (!(expr)) { \
  Failed(msg, ##__VA_ARGS__); \
  return 1; \
}

#define AssertNoneCommitted(index) { \
        auto nc = config_->NCommitted(index); \
        Assert2(nc == 0, \
                "%d servers unexpectedly committed index %ld", \
                nc, index) \
      }

// #define AssertNCommitted(index, expected) { \
//         auto nc = config_->NCommitted(index); \
//         Assert2(nc == expected, \
//                 "%d servers committed index %ld (%d expected)", \
//                 nc, index, expected) \
//       }
// #define AssertStartOk(ok) Assert2(ok, "unexpected leader change during Start()")
// #define AssertWaitNoError(ret, index) \
//         Assert2(ret != -3, "committed values differ for index %ld", index)
// #define AssertWaitNoTimeout(ret, index, n) \
//         Assert2(ret != -1, "waited too long for %d server(s) to commit index %ld", n, index); \
//         Assert2(ret != -2, "term moved on before index %ld committed by %d server(s)", index, n)

#define DoAgreeAndAssertIndex(cmd, dkey, n, index) { \
        auto r = config_->DoAgreement(cmd, dkey, n, false); \
        auto ind = index; \
        Assert2(r, "failed to reach agreement for command %d among %d servers" PRId64, cmd, n); \
      }
      /* Assert2(r > 0, "failed to reach agreement for command %d among %d servers, expected commit index>0, got %" PRId64, cmd, n, r); \
      Assert2(r == ind, "agreement index incorrect. got %ld, expected %ld", r, ind); \ */

// #define DoAgreeAndAssertWaitSuccess(cmd, n) { \
//         auto r = config_->DoAgreement(cmd, n, true); \
//         Assert2(r > 0, "failed to reach agreement for command %d among %d servers", cmd, n); \
//         index_ = r + 1; \
//       }

int EpaxosLabTest::testBasicAgree(void) {
  Init2(3, "Basic agreement");
  for (int i = 1; i <= 3; i++) {
    // make sure no commits exist before any agreements are started
    AssertNoneCommitted(index_);
    // complete 1 agreement and make sure its index is as expected
    int cmd = index_ + 100;
    DoAgreeAndAssertIndex(cmd, to_string(cmd), NSERVERS, index_++);
  }
  Passed2();
}

// int EpaxosLabTest::testFailAgree(void) {
//   Init2(4, "Agreement despite follower disconnection");
//   // disconnect 2 followers
//   auto leader = config_->OneLeader();
//   AssertOneLeader(leader);
//   Log_debug("disconnecting two followers leader");
//   config_->Disconnect((leader + 1) % NSERVERS);
//   config_->Disconnect((leader + 2) % NSERVERS);
//   // Agreement despite 2 disconnected servers
//   Log_debug("try commit a few commands after disconnect");
//   DoAgreeAndAssertIndex(401, NSERVERS - 2, index_++);
//   DoAgreeAndAssertIndex(402, NSERVERS - 2, index_++);
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   DoAgreeAndAssertIndex(403, NSERVERS - 2, index_++);
//   DoAgreeAndAssertIndex(404, NSERVERS - 2, index_++);
//   // reconnect followers
//   Log_debug("reconnect servers");
//   config_->Reconnect((leader + 1) % NSERVERS);
//   config_->Reconnect((leader + 2) % NSERVERS);
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   Log_debug("try commit a few commands after reconnect");
//   DoAgreeAndAssertWaitSuccess(405, NSERVERS);
//   DoAgreeAndAssertWaitSuccess(406, NSERVERS);
//   Passed2();
// }

// int EpaxosLabTest::testFailNoAgree(void) {
//   Init2(5, "No agreement if too many followers disconnect");
//   // disconnect 3 followers
//   auto leader = config_->OneLeader();
//   AssertOneLeader(leader);
//   config_->Disconnect((leader + 1) % NSERVERS);
//   config_->Disconnect((leader + 2) % NSERVERS);
//   config_->Disconnect((leader + 3) % NSERVERS);
//   // attempt to do an agreement
//   uint64_t index, term;
//   AssertStartOk(config_->Start(leader, 501, &index, &term));
//   Assert2(index == index_++ && term > 0,
//           "Start() returned unexpected index (%ld, expected %ld) and/or term (%ld, expected >0)",
//           index, index_-1, term);
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   AssertNoneCommitted(index);
//   // reconnect followers
//   config_->Reconnect((leader + 1) % NSERVERS);
//   config_->Reconnect((leader + 2) % NSERVERS);
//   config_->Reconnect((leader + 3) % NSERVERS);
//   // do agreement in restored quorum
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   DoAgreeAndAssertWaitSuccess(502, NSERVERS);
//   Passed2();
// }

// int EpaxosLabTest::testRejoin(void) {
//   Init2(6, "Rejoin of disconnected leader");
//   DoAgreeAndAssertIndex(601, NSERVERS, index_++);
//   // disconnect leader
//   auto leader1 = config_->OneLeader();
//   AssertOneLeader(leader1);
//   config_->Disconnect(leader1);
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   // Make old leader try to agree on some entries (these should not commit)
//   uint64_t index, term;
//   AssertStartOk(config_->Start(leader1, 602, &index, &term));
//   AssertStartOk(config_->Start(leader1, 603, &index, &term));
//   AssertStartOk(config_->Start(leader1, 604, &index, &term));
//   // New leader commits, successfully
//   DoAgreeAndAssertWaitSuccess(605, NSERVERS - 1);
//   DoAgreeAndAssertWaitSuccess(606, NSERVERS - 1);
//   // Disconnect new leader
//   auto leader2 = config_->OneLeader();
//   AssertOneLeader(leader2);
//   AssertReElection(leader2, leader1);
//   config_->Disconnect(leader2);
//   // reconnect old leader
//   config_->Reconnect(leader1);
//   // wait for new election
//   Coroutine::Sleep(ELECTIONTIMEOUT);
//   auto leader3 = config_->OneLeader();
//   AssertOneLeader(leader3);
//   AssertReElection(leader3, leader2);
//   // More commits
//   DoAgreeAndAssertWaitSuccess(607, NSERVERS - 1);
//   DoAgreeAndAssertWaitSuccess(608, NSERVERS - 1);
//   // Reconnect all
//   config_->Reconnect(leader2);
//   DoAgreeAndAssertWaitSuccess(609, NSERVERS);
//   Passed2();
// }

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

// class CAArgs {
//  public:
//   int iter;
//   int i;
//   std::mutex *mtx;
//   std::vector<uint64_t> *retvals;
//   EpaxosTestConfig *config;
// };

// static void *doConcurrentAgreement(void *args) {
//   CAArgs *caargs = (CAArgs *)args;
//   uint64_t retval = caargs->config->DoAgreement(1000 + caargs->iter, 1, true);
//   if (retval == 0) {
//     std::lock_guard<std::mutex> lock(*(caargs->mtx));
//     caargs->retvals->push_back(retval);
//   }
//   return nullptr;
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
