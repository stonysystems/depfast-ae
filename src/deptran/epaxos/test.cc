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
      || testPrepareAcceptedCommand()
      || testPreparePreAcceptedCommand()
      || testPrepareNoopCommand()
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

#define Init2(test_id, description) { \
        Init(test_id, description); \
        verify(config_->NDisconnected() == 0 && !config_->IsUnreliable() && !config_->AnySlow()); \
      }

#define InitSub2(sub_test_id, description) { \
        InitSub(sub_test_id, description); \
        verify(config_->NDisconnected() == 0 && !config_->IsUnreliable() && !config_->AnySlow()); \
      }

#define Passed2() { \
        Passed(); \
        return 0; \
      }

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

#define AssertValidCommitStatus(replica_id, instance_no, r) { \
          Assert2(r != -1, "failed to reach agreement for replica: %d instance: %d, committed different commands", replica_id, instance_no); \
          Assert2(r != -2, "failed to reach agreement for replica: %d instance: %d, committed different dkey", replica_id, instance_no); \
          Assert2(r != -3, "failed to reach agreement for replica: %d instance: %d, committed different seq", replica_id, instance_no); \
          Assert2(r != -4, "failed to reach agreement for replica: %d instance: %d, committed different deps", replica_id, instance_no); \
        }

#define AssertNCommitted(replica_id, instance_no, n) { \
        auto r = config_->NCommitted(replica_id, instance_no, n); \
        Assert2(r >= n, "failed to reach agreement for replica: %d instance: %d among %d servers", replica_id, instance_no, n); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
      }

#define AssertNCommittedAndVerifyNoop(replica_id, instance_no, n, noop) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        unordered_map<uint64_t, uint64_t> cdeps; \
        auto r = config_->NCommitted(replica_id, instance_no, n, &cnoop, &cdkey, &cseq, &cdeps); \
        Assert2(r >= n, "failed to reach agreement for replica: %d instance: %d among %d servers", replica_id, instance_no, n); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
        Assert2(cnoop == noop || !noop, "failed to reach agreement for replica: %d instance: %d, expected noop, got command", replica_id, instance_no); \
        Assert2(cnoop == noop || noop, "failed to reach agreement for replica: %d instance: %d, expected command, got noop", replica_id, instance_no); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
      }
      
#define DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, n, noop, exp_dkey, exp_seq, exp_deps) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        unordered_map<uint64_t, uint64_t> cdeps; \
        auto r = config_->DoAgreement(cmd, dkey, n, false, &cnoop, &cdkey, &cseq, &cdeps); \
        Assert2(r >= 0, "failed to reach agreement for command %d among %d servers", cmd, n); \
        Assert2(r != -1, "failed to reach agreement for command %d, committed different commands", cmd); \
        Assert2(r != -2, "failed to reach agreement for command %d, committed different dkey", cmd); \
        Assert2(r != -3, "failed to reach agreement for command %d, committed different seq", cmd); \
        Assert2(r != -4, "failed to reach agreement for command %d, committed different deps", cmd); \
        Assert2(cnoop == noop || !noop, "failed to reach agreement for command %d, expected noop, got command", cmd); \
        Assert2(cnoop == noop || noop, "failed to reach agreement for command %d, expected command, got noop", cmd); \
        Assert2(cdkey == exp_dkey, "failed to reach agreement for command %d, expected dkey %s, got dkey %s", cmd, exp_dkey.c_str(), cdkey.c_str()); \
        Assert2(cseq == exp_seq, "failed to reach agreement for command %d, expected seq %d, got seq %d", cmd, exp_seq, cseq); \
        Assert2(cdeps == exp_deps, "failed to reach agreement for command %d, expected deps different from committed deps", cmd); \
      }
      
#define DoAgreeAndAssertNoneCommitted(cmd, dkey) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        unordered_map<uint64_t, uint64_t> cdeps; \
        auto r = config_->DoAgreement(cmd, dkey, 1, false, &cnoop, &cdkey, &cseq, &cdeps); \
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
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, NSERVERS, false, dkey, 0, deps);
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
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, 0, deps);
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
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  // Round 2
  cmd++;
  seq++;
  deps[1] = 3;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  // Round 3
  cmd++;
  seq++;
  deps[1] = 4;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
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
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, 0, deps);
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
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  // Round 2
  cmd++;
  seq++;
  deps[2] = 3;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  // Round 3
  cmd++;
  seq++;
  deps[2] = 4;
  // make sure no commits exist before any agreements are started
  AssertNoneExecuted(cmd);
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
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
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Committed (via fast path) in 2 servers (leader and one replica). Prepare returns 1 committed reply.");
  int cmd = 701;
  string dkey = "700";
  uint64_t replica_id, instance_no;
  int time_to_sleep = 1100, diff = 100;
  int CMD_LEADER = 0;
  // Keep only fast-path quorum of servers alive
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  // Repeat till only 2 servers (leader and one replica) have committed the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    verify(nc <= FAST_PATH_QUORUM);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (nc == 2) break;
    // Retry if more than 2 servers committed
    time_to_sleep = (nc < 2) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  }
  // Reconnect all except command leader and commit in all via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);

  /*********** Sub Test 2 ***********/
  InitSub2(2, "Committed (via slow path) in 1 server (leader). Prepare returns 1 accepted reply.");
  cmd++;
  time_to_sleep = 1800;
  // Keep only slow-path quorum of servers alive
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    verify(nc <= SLOW_PATH_QUORUM);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (nc == 1) break;
    // Retry if more than 1 server committed
    time_to_sleep = (nc < 1) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  }
  // Reconnect all disconnected servers and one accepted server and commit in those via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS-1, false);
  // Reconnect other accepted server and commit in all via prepare
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 4) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);

  /*********** Sub Test 3 ***********/
  InitSub2(3, "Committed (via fast path) in 1 server (leader). Prepare returns N/2 identical pre-accepted replies.");
  cmd++;
  time_to_sleep = 1200;
  // Keep only fast-path quorum of servers alive
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    verify(nc <= FAST_PATH_QUORUM);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (nc == 1) break;
    // Retry if more than 1 server committed
    time_to_sleep = (nc < 1) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  }
  // Reconnect all except command leader and commit in all via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);

  /*********** Sub Test 4 ***********/
  InitSub2(4, "Committed (via slow path) in 2 servers (leader and one replica). Prepare returns 1 committed reply.");
  cmd++;
  time_to_sleep = 1900;
  // Keep only slow-path quorum of servers alive
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  // Repeat till only 2 servers (leader and one replica) have committed the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    verify(nc <= SLOW_PATH_QUORUM);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (nc == 2) break;
    // Retry if more than 2 servers committed
    time_to_sleep = (nc < 2) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  }
  // Reconnect one disconnected server and commit in those via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS-1);
  // Reconnect other disconnected server and prepare again in it
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Prepare((CMD_LEADER + 2) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);
  Passed2();
}

int EpaxosLabTest::testPrepareAcceptedCommand(void) {
  Init2(8, "Commit through prepare - accepted but not committed command");
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Accepted in 1 server (leader). Prepare returns 1 pre-accepted reply.");
  int cmd = 801;
  string dkey = "800";
  uint64_t replica_id, instance_no;
  int time_to_sleep = 1000, diff = 100;
  int CMD_LEADER = 1;
  // Keep only slow-path quorum of servers alive
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  // Repeat till only 1 server (leader) have accepted the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    verify(na <= SLOW_PATH_QUORUM);
    if (na == 1) break;
    // Retry if more than 1 server accepted
    time_to_sleep = (na < 1 && na >= 0) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  }
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Reconnect all disconnected servers and one pre-accepted server and commit in those via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS-2, false);
  // Reconnect leader and other pre-accepted server and commit in those via prepare
  config_->Reconnect(CMD_LEADER);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 2) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 2 ***********/
  InitSub2(2, "Accepted in 2 servers (leader and one replica). Prepare returns 1 accepted reply.");
  cmd++;
  time_to_sleep = 1300;
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  // Repeat till only 2 servers (leader and one replica) have accepted the command
  while (true) {
    AssertNoneExecuted(cmd);
    config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
    Coroutine::Sleep(time_to_sleep);
    config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Disconnect(CMD_LEADER);
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    verify(na <= SLOW_PATH_QUORUM);
    if (na == 2) break;
    // Retry if more than 2 server accepted
    time_to_sleep = (na < 2 && na >= 0) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
    config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
    config_->Reconnect(CMD_LEADER);
  }
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Reconnect all disconnected servers and commit via prepare
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS-1, false);
  // Reconnect leader and commit in it via prepare
  config_->Reconnect(CMD_LEADER);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  Passed2();
}

int EpaxosLabTest::testPreparePreAcceptedCommand(void) {
  Init2(9, "Commit through prepare - pre-accepted but not in majority");
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Pre-accepted in 1 server (leader). Prepare return 1 pre-accepted reply from leader (avoid fast-path).");
  int cmd = 901;
  string dkey = "900";
  int CMD_LEADER = 2;
  uint64_t replica_id, instance_no;
  // Disconnect leader
  config_->Disconnect(CMD_LEADER);
  AssertNoneExecuted(cmd);
  // Start agreement in leader - will not replicate as leader is disconnected
  config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
  auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  verify(np == 1);
  auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  verify(na == 0);
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Reconnect leader and commit via prepare
  config_->Reconnect(CMD_LEADER);
  // Set non-leaders slow to prevent from committing noop
  config_->SetSlow((CMD_LEADER + 1) % NSERVERS, true);
  config_->SetSlow((CMD_LEADER + 2) % NSERVERS, true);
  config_->SetSlow((CMD_LEADER + 3) % NSERVERS, true);
  config_->SetSlow((CMD_LEADER + 4) % NSERVERS, true);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  config_->SetSlow((CMD_LEADER + 1) % NSERVERS, false);
  config_->SetSlow((CMD_LEADER + 2) % NSERVERS, false);
  config_->SetSlow((CMD_LEADER + 3) % NSERVERS, false);
  config_->SetSlow((CMD_LEADER + 4) % NSERVERS, false);
  
  /*********** Sub Test 2 ***********/
  InitSub2(2, "Pre-accepted in 2 server (leader and replica). Prepare returns 1 pre-accepted reply from another replica (slow-path).");
  cmd++;
  // Disconnect all servers except leader and 1 replica
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
  AssertNoneExecuted(cmd);
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  verify(np == 2);
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  verify(na == 0);
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Disconnect leader and reconnect majority-1 non pre-accepted servers to commit pre-accepted command via prepare
  config_->Disconnect(CMD_LEADER);
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS-2, false);
  config_->Reconnect(CMD_LEADER);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  Passed2();
}

int EpaxosLabTest::testPrepareNoopCommand(void) {
  Init2(10, "Commit through prepare - commit noop");
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Pre-accepted in 1 server (leader). Prepare returns no replies (avoid fast-path).");
  int cmd = 1001;
  string dkey = "1000";
  int CMD_LEADER = 3;
  uint64_t replica_id, instance_no;
  // Disconnect leader
  config_->Disconnect(CMD_LEADER);
  AssertNoneExecuted(cmd);
  // Start agreement in leader - will not replicate as leader is disconnected
  config_->Start(CMD_LEADER, cmd, dkey, &replica_id, &instance_no);
  auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  verify(np == 1);
  auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  verify(na == 0);
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Commit noop in others via prepare
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS-1, true);
  // Reconnect leader and see if noop is committed in all via prepare
  config_->Reconnect(CMD_LEADER);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, true);
  
  /*********** Sub Test 2 ***********/
  InitSub2(2, "Pre-accepted in 2 server (leader and replica). Prepare returns no replies (slow path).");
  cmd++;
  // Disconnect all servers except leader and 1 replica
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Disconnect((CMD_LEADER + 3) % NSERVERS);
  AssertNoneExecuted(cmd);
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  config_->Start(3, cmd, dkey, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  verify(np == 2);
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  verify(na == 0);
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  verify(nc == 0);
  // Disconnect pre-accepted servers and commit noop in others via prepare
  config_->Disconnect(CMD_LEADER);
  config_->Disconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 2) % NSERVERS);
  config_->Reconnect((CMD_LEADER + 3) % NSERVERS);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommitted(replica_id, instance_no, NSERVERS-2);
  // Reconnect leader and other server and see if noop is committed in all
  config_->Reconnect(CMD_LEADER);
  config_->Reconnect((CMD_LEADER + 4) % NSERVERS);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, true);
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
  Init2(11, "Concurrent agreements");
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
  Init2(12, "Unreliable concurrent agreement (takes a few minutes)");
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

#endif

}
