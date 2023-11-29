#include "test.h"

namespace janus {


#ifdef EPAXOS_TEST_CORO

int EpaxosTest::Run(void) {
  Print("START WHOLISTIC TESTS");
  config_->SetLearnerAction();
  uint64_t start_rpc = config_->RpcTotal();
  if (testBasicAgree()
      || testFastPathIndependentAgree()
      || testFastPathDependentAgree()
      || testSlowPathIndependentAgree()
      || testSlowPathDependentAgree()
      || testFailNoQuorum()
      // || testNonIdenticalAttrsAgree()
      || testPrepareCommittedCommandAgree()
      || testPrepareAcceptedCommandAgree()
      || testPreparePreAcceptedCommandAgree()
      || testPrepareNoopCommandAgree()
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

#define Init2(test_id, description) { \
        Init(test_id, description); \
        cmd = ((cmd / 100) + 1) * 100; \
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
        auto ne = config_->NExecuted(cmd, NSERVERS); \
        Assert2(ne == 0, "%d servers unexpectedly executed command %d", ne, cmd); \
      }

#define AssertNExecuted(cmd, expected) { \
        auto ne = config_->NExecuted(cmd, expected); \
        Assert2(ne == expected, "%d servers executed command %d (%d expected)", ne, cmd, expected) \
      }

#define AssertExecutedPairsInOrder(expected_pairs) { \
        auto r = config_->ExecutedPairsInOrder(expected_pairs); \
        Assert2(r, "unexpected execution order of commands"); \
      }

#define AssertSameExecutedOrder(dependent_cmds) { \
        auto r = config_->ExecutedInSameOrder(dependent_cmds); \
        Assert2(r, "execution order of dependent commands is different in servers"); \
      }

#define AssertValidCommitStatus(replica_id, instance_no, r) { \
          Assert2(r != -1, "failed to reach agreement for replica: %d instance: %d, committed different commands", replica_id, instance_no); \
          Assert2(r != -2, "failed to reach agreement for replica: %d instance: %d, committed different dkey", replica_id, instance_no); \
          Assert2(r != -3, "failed to reach agreement for replica: %d instance: %d, committed different seq", replica_id, instance_no); \
          Assert2(r != -4, "failed to reach agreement for replica: %d instance: %d, committed different deps", replica_id, instance_no); \
        }

#define AssertNCommitted(replica_id, instance_no, n) { \
        auto r = config_->NCommitted(replica_id, instance_no, n); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
        Assert2(r >= n, "failed to reach agreement for replica: %d instance: %d among %d servers", replica_id, instance_no, n); \
      }

#define AssertNCommittedAndVerifyNoop(replica_id, instance_no, n, noop) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        map<uint64_t, uint64_t> cdeps; \
        auto r = config_->NCommitted(replica_id, instance_no, n, &cnoop, &cdkey, &cseq, &cdeps); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
        Assert2(r >= n, "failed to reach agreement for replica: %d instance: %d among %d servers", replica_id, instance_no, n); \
        Assert2(cnoop == noop || !noop, "failed to reach agreement for replica: %d instance: %d, expected noop, got command", replica_id, instance_no); \
        Assert2(cnoop == noop || noop, "failed to reach agreement for replica: %d instance: %d, expected command, got noop", replica_id, instance_no); \
      }
      
#define AssertNCommittedAndVerifyAttrs(replica_id, instance_no, n, noop, exp_dkey, exp_seq, exp_deps) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        map<uint64_t, uint64_t> cdeps; \
        auto r = config_->NCommitted(replica_id, instance_no, n, &cnoop, &cdkey, &cseq, &cdeps); \
        Assert2(r >= n, "failed to reach agreement for replica: %d instance: %d among %d servers", replica_id, instance_no, n); \
        AssertValidCommitStatus(replica_id, instance_no, r); \
        Assert2(cnoop == noop || !noop, "failed to reach agreement for replica: %d instance: %d, expected noop, got command", replica_id, instance_no); \
        Assert2(cnoop == noop || noop, "failed to reach agreement for replica: %d instance: %d, expected command, got noop", replica_id, instance_no); \
        Assert2(cdkey == exp_dkey, "failed to reach agreement for replica: %d instance: %d, expected dkey %s, got dkey %s", replica_id, instance_no, exp_dkey.c_str(), cdkey.c_str()); \
        Assert2(cseq == exp_seq, "failed to reach agreement for replica: %d instance: %d, expected seq %d, got seq %d", replica_id, instance_no, exp_seq, cseq); \
        Assert2(cdeps == exp_deps, "failed to reach agreement for replica: %d instance: %d, expected deps %s different from committed deps %s", replica_id, instance_no, map_to_string(exp_deps).c_str(), map_to_string(cdeps).c_str()); \
      }

#define DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, n, noop, exp_dkey, exp_seq, exp_deps) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        map<uint64_t, uint64_t> cdeps; \
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
        Assert2(cdeps == exp_deps, "failed to reach agreement for command %d, expected deps %s different from committed deps %s", cmd, map_to_string(exp_deps).c_str(), map_to_string(cdeps).c_str()); \
      }
      
#define DoAgreeAndAssertNoneCommitted(cmd, dkey) { \
        bool cnoop; \
        string cdkey; \
        uint64_t cseq; \
        map<uint64_t, uint64_t> cdeps; \
        auto r = config_->DoAgreement(cmd, dkey, 1, false, &cnoop, &cdkey, &cseq, &cdeps); \
        Assert2(r == 0, "committed command %d without majority", cmd); \
      }

#define DisconnectNServers(n) { \
        for (int i = 0; i < n; i++) { \
          config_->Disconnect(i); \
        } \
      }

#define ReconnectNServers(n) { \
        for (int i = 0; i < n; i++) { \
          config_->Reconnect(i); \
        } \
      }

int EpaxosTest::testBasicAgree(void) {
  Init2(1, "Basic agreement");
  config_->PauseExecution(false);
  for (int i = 1; i <= 3; i++) {
    // complete agreement and make sure its attributes are as expected
    cmd++;
    string dkey = to_string(cmd);
    map<uint64_t, uint64_t> deps;
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, NSERVERS, false, dkey, 1, deps);
    AssertNExecuted(cmd, NSERVERS);
  }
  Passed2();
}

int EpaxosTest::testFastPathIndependentAgree(void) {
  Init2(2, "Fast path agreement of independent commands");
  config_->PauseExecution(false);
  DisconnectNServers(NSERVERS - FAST_PATH_QUORUM);
  map<uint64_t, uint64_t> deps;
  for (int i = 1; i <= 3; i++) {
    // complete agreement and make sure its attributes are as expected
    cmd++;
    string dkey = to_string(cmd);
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, 1, deps);
    AssertNExecuted(cmd, FAST_PATH_QUORUM);
  }
  // Reconnect all
  ReconnectNServers(NSERVERS - FAST_PATH_QUORUM);
  Passed2();
}

int EpaxosTest::testFastPathDependentAgree(void) {
  Init2(3, "Fast path agreement of dependent commands");
  config_->PauseExecution(false);
  DisconnectNServers(NSERVERS - FAST_PATH_QUORUM);
  int next_active_server = NSERVERS - FAST_PATH_QUORUM;
  // complete 1st agreement and make sure its attributes are as expected
  uint64_t cmd1 = ++cmd;
  string dkey = "1";
  uint64_t seq = 1;
  map<uint64_t, uint64_t> deps;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, FAST_PATH_QUORUM);
  // complete 2nd agreement and make sure its attributes are as expected
  uint64_t cmd2 = ++cmd;
  seq++;
  deps[next_active_server] = 3;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, FAST_PATH_QUORUM);
  // complete 3rd agreement and make sure its attributes are as expected
  uint64_t cmd3 = ++cmd;
  seq++;
  deps[next_active_server] = 4;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, FAST_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, FAST_PATH_QUORUM);
  // Verify order of execution
  vector<pair<uint64_t, uint64_t>> exp_exec_order = {{cmd1, cmd2}, {cmd2, cmd3}};
  AssertExecutedPairsInOrder(exp_exec_order)
  // Reconnect all
  ReconnectNServers(NSERVERS - FAST_PATH_QUORUM);
  Passed2();
}

int EpaxosTest::testSlowPathIndependentAgree(void) {
  Init2(4, "Slow path agreement of independent commands");
  config_->PauseExecution(false);
  DisconnectNServers(NSERVERS - SLOW_PATH_QUORUM);
  for (int i = 1; i <= 3; i++) {
    // complete agreement and make sure its attributes are as expected
    cmd++;
    string dkey = to_string(cmd);
    map<uint64_t, uint64_t> deps;
    DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, 1, deps);
    AssertNExecuted(cmd, SLOW_PATH_QUORUM);
  }
  // Reconnect all
  ReconnectNServers(NSERVERS - SLOW_PATH_QUORUM);
  Passed2();
}

int EpaxosTest::testSlowPathDependentAgree(void) {
  Init2(5, "Slow path agreement of dependent commands");
  config_->PauseExecution(false);
  DisconnectNServers(NSERVERS - SLOW_PATH_QUORUM);
  int next_active_server = NSERVERS - SLOW_PATH_QUORUM;
  // complete 1st agreement and make sure its attributes are as expected
  uint64_t cmd1 = ++cmd;
  string dkey = "2";
  uint64_t seq = 1;
  map<uint64_t, uint64_t> deps;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, SLOW_PATH_QUORUM);
  // complete 2nd agreement and make sure its attributes are as expected
  uint64_t cmd2 = ++cmd;
  seq++;
  deps[next_active_server] = 3;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, SLOW_PATH_QUORUM);
  // complete 3rd agreement and make sure its attributes are as expected
  uint64_t cmd3 = ++cmd;
  seq++;
  deps[next_active_server] = 4;
  DoAgreeAndAssertNCommittedAndVerifyAttrs(cmd, dkey, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  AssertNExecuted(cmd, SLOW_PATH_QUORUM);
  // Verify order of execution
  vector<pair<uint64_t, uint64_t>> exp_exec_order = {{cmd1, cmd2}, {cmd1, cmd3}};
  AssertExecutedPairsInOrder(exp_exec_order)
  // Reconnect all
  ReconnectNServers(NSERVERS - SLOW_PATH_QUORUM);
  Passed2();
}

int EpaxosTest::testFailNoQuorum(void) {
  Init2(6, "No agreement if too many servers disconnect");
  config_->PauseExecution(false);
  DisconnectNServers(NSERVERS + 1 - SLOW_PATH_QUORUM);
  for (int i = 1; i <= 3; i++) {
    // complete 1 agreement and make sure its not committed
    cmd++;
    string dkey = to_string(cmd);
    DoAgreeAndAssertNoneCommitted(cmd, dkey);
    AssertNoneExecuted(cmd);
  }
  // Reconnect all
  ReconnectNServers(NSERVERS + 1 - SLOW_PATH_QUORUM);
  Passed2();
}

int EpaxosTest::testNonIdenticalAttrsAgree(void) {
  Init2(7, "Leader and replicas have different dependencies");
  config_->PauseExecution(true);
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Leader have more dependencies than replicas");
  cmd++;
  string dkey = "3";
  uint64_t replica_id, instance_no;
  map<uint64_t, uint64_t> init_deps;
  unordered_set<uint64_t> dependent_cmds;
  // Pre-accept different commands in each server
  for (int i=0; i<NSERVERS; i++) {
    config_->Disconnect(i);
    dependent_cmds.insert(cmd);
    config_->Start(i, cmd, dkey);
    config_->GetInstance(i, cmd, &replica_id, &instance_no);
    auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
    Assert2(np == 1, "unexpected number of pre-accepted servers");
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    Assert2(na == 0, "unexpected number of accepted servers");
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc == 0, "unexpected number of committed servers");
    init_deps[replica_id] = instance_no;
    cmd++;
    config_->Reconnect(i);
  }
  int CMD_LEADER = NSERVERS/2;
  // Commit in one majority
  int seq = 2;
  map<uint64_t, uint64_t> deps;
  for (int i = 0; i < NSERVERS/2; i++) {
    config_->Disconnect(i);
  }
  for (int i = CMD_LEADER; i < NSERVERS; i++) {
    deps[i] = init_deps[i];
  }
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  for (int i = 0; i < NSERVERS/2; i++) {
    config_->Reconnect(i);
  }
  // Commit in another majority with same command leader
  cmd++;
  seq = 3;
  for (int i = CMD_LEADER + 1; i < NSERVERS; i++) {
    config_->Disconnect(i);
  }
  deps[CMD_LEADER] = instance_no;
  for (int i = 0; i < NSERVERS/2; i++) {
    deps[i] = init_deps[i];
  }
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, SLOW_PATH_QUORUM, false, dkey, seq, deps)
  for (int i = CMD_LEADER + 1; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  // Commit in all
  cmd++;
  seq = 4;
  deps[CMD_LEADER] = instance_no;
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, NSERVERS, false, dkey, seq, deps);


  /*********** Sub Test 2 ***********/
  InitSub2(2, "Replicas have more dependencies than leader");
  cmd++;
  // Pre-accept different commands in each server
  for (int i=0; i<NSERVERS; i++) {
    config_->Disconnect(i);
    dependent_cmds.insert(cmd);
    config_->Start(i, cmd, dkey);
    config_->GetInstance(i, cmd, &replica_id, &instance_no);
    auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
    Assert2(np == 1, "unexpected number of pre-accepted servers");
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    Assert2(na == 0, "unexpected number of accepted servers");
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc == 0, "unexpected number of committed servers");
    init_deps[replica_id] = instance_no;
    cmd++;
    config_->Reconnect(i);
  }
  // Commit in majority
  seq = 6;
  for (int i = 0; i < NSERVERS/2; i++) {
    config_->Disconnect(i);
  }
  for (int i = CMD_LEADER; i < NSERVERS; i++) {
    deps[i] = init_deps[i];
  }
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, SLOW_PATH_QUORUM, false, dkey, seq, deps);
  for (int i = 0; i < NSERVERS/2; i++) {
    config_->Reconnect(i);
  }
  // Commit in another majority
  cmd++;
  seq = 7;
  for (int i = CMD_LEADER + 1; i < NSERVERS; i++) {
    config_->Disconnect(i);
  }
  for (int i = 0; i < NSERVERS/2; i++) {
    deps[i] = init_deps[i];
  }
  deps[CMD_LEADER] = instance_no;
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER - 1, cmd, dkey);
  config_->GetInstance(CMD_LEADER - 1, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, SLOW_PATH_QUORUM, false, dkey, seq, deps)
  for (int i = CMD_LEADER + 1; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  // Commit in all
  cmd++;
  seq = 8;
  deps[CMD_LEADER - 1] = instance_no;
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER - 2, cmd, dkey);
  config_->GetInstance(CMD_LEADER - 2, cmd, &replica_id, &instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, NSERVERS, false, dkey, seq, deps);

  /*********** Execution order ***********/
  InitSub2(3, "Execution order");
  config_->PauseExecution(false);
  Coroutine::Sleep(5000000);
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

int EpaxosTest::testPrepareCommittedCommandAgree(void) {
  Init2(8, "Commit through prepare - committed command");
  config_->PauseExecution(true);
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Committed (via fast path). Prepare returns committed replies.");
  cmd++;
  string dkey = "4";
  unordered_set<uint64_t> dependent_cmds;
  uint64_t replica_id, instance_no;
  int time_to_sleep = 1100, diff = 100;
  int CMD_LEADER = 0;
  // Keep only fast-path quorum of servers alive
  for (int i = 1 ; i <= NSERVERS - FAST_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  auto nc = config_->NCommitted(replica_id, instance_no, FAST_PATH_QUORUM);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == FAST_PATH_QUORUM, "unexpected number of committed servers");
  // Reconnect all
  for (int i = 1; i <= NSERVERS - FAST_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 2 ***********/
  InitSub2(2, "Committed (via slow path). Prepare returns committed replies.");
  cmd++;
  time_to_sleep = 1900;
  // Keep only slow-path quorum of servers alive
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  nc = config_->NCommitted(replica_id, instance_no, SLOW_PATH_QUORUM);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == SLOW_PATH_QUORUM, "unexpected number of committed servers");
  // Reconnect all
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 3 ***********/
  InitSub2(3, "Committed (via slow path) in 1 server (leader). Prepare returns 1 accepted reply.");
  cmd++;
  time_to_sleep = 1800;
  // Keep only slow-path quorum of servers alive
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc <= SLOW_PATH_QUORUM, "unexpected number of committed servers");
    if (nc == 1) break;
    // Retry if more than 1 server committed
    time_to_sleep = (nc < 1) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  // Reconnect all disconnected servers and one accepted server and commit in those via prepare
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Reconnect(NSERVERS - SLOW_PATH_QUORUM + 1);
  config_->Prepare(CMD_LEADER + 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM + 1, false);
  // Reconnect other accepted server and commit in all via prepare
  for (int i = NSERVERS - SLOW_PATH_QUORUM + 2; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(NSERVERS - SLOW_PATH_QUORUM + 2, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);

  /*********** Sub Test 4 ***********/
  InitSub2(4, "Committed (via fast path) in 1 server (leader). Prepare returns N/2 identical pre-accepted replies.");
  cmd++;
  time_to_sleep = 1200;
  // Keep only fast-path quorum of servers alive
  for (int i = 1; i <= NSERVERS - FAST_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - FAST_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc <= FAST_PATH_QUORUM, "unexpected number of committed servers");
    if (nc == 1) break;
    // Retry if more than 1 server committed
    time_to_sleep = (nc < 1) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - FAST_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  // Reconnect all except command leader and commit in all via prepare
  for (int i = 1; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(CMD_LEADER + 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  // Reconnect leader
  config_->Reconnect(CMD_LEADER);

  /*********** Sub Test 5 ***********/
  InitSub2(5, "Committed (via fast path) in 1 server (leader). Prepare returns (F+1)/2 identical pre-accepted replies.");
  cmd++;
  time_to_sleep = 1200;
  // Keep only fast-path quorum of servers alive
  for (int i = 1; i <= NSERVERS - FAST_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - FAST_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc <= FAST_PATH_QUORUM, "unexpected number of committed servers");
    if (nc == 1) break;
    // Retry if more than 1 server accepted
    time_to_sleep = (nc < 1) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - FAST_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  // Reconnect (F+1)/2 identically preaccepted servers, remaining not pre-accepted servers (to meet majority) and commit in all via prepare
  int F = NSERVERS/2;
  for (int i = NSERVERS - FAST_PATH_QUORUM + 1; i < NSERVERS - FAST_PATH_QUORUM + 1 + (F+1)/2; i++) {
    config_->Reconnect(i);
  }
  for (int i = 1; i <= SLOW_PATH_QUORUM - ((F+1)/2); i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(CMD_LEADER + 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  // Reconnect all and commit via prepare
  for (int i = 0; i < NSERVERS; i++) {
    if (config_->IsDisconnected(i)) {
      config_->Reconnect(i);
    }
  }
  config_->Prepare(NSERVERS - 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Execution order ***********/
  InitSub2(5, "Execution order");
  config_->PauseExecution(false);
  Coroutine::Sleep(5000000);
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

int EpaxosTest::testPrepareAcceptedCommandAgree(void) {
  Init2(9, "Commit through prepare - accepted but not committed command");
  config_->PauseExecution(true);
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Accepted in 1 server (leader). Prepare returns 1 pre-accepted reply.");
  cmd++;
  string dkey = "5";
  unordered_set<uint64_t> dependent_cmds;
  uint64_t replica_id, instance_no;
  int time_to_sleep = 1000, diff = 100;
  int CMD_LEADER = NSERVERS - SLOW_PATH_QUORUM;
  // Keep only slow-path quorum of servers alive
  for (int i = 0; i < NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have accepted the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    Assert2(na <= SLOW_PATH_QUORUM, "unexpected number of accepted servers");
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (na == 1 && nc == 0) break;
    // Retry if more than 1 server accepted
    time_to_sleep = (na < 1 && nc == 0) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Reconnect all disconnected servers and one pre-accepted server and commit in those via prepare
  for (int i = 0; i < NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Reconnect(NSERVERS - SLOW_PATH_QUORUM + 1);
  config_->Prepare(0, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  // Reconnect leader and other pre-accepted servers and commit in those via prepare
  config_->Reconnect(CMD_LEADER);
  for (int i = NSERVERS - SLOW_PATH_QUORUM + 2; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(NSERVERS - SLOW_PATH_QUORUM + 2, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 2 ***********/
  InitSub2(2, "Accepted in 1 server (leader). Prepare returns 1 accepted reply.");
  cmd++;
  time_to_sleep = 1300;
  CMD_LEADER = 0;
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have accepted the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    Assert2(na <= SLOW_PATH_QUORUM, "unexpected number of accepted servers");
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    if (na == 1 && nc == 0) break;
    // Retry if more than 1 server accepted
    time_to_sleep = (na < 1 && nc == 0) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Reconnect all disconnected servers and commit in those via prepare
  for (int i = 0; i < NSERVERS; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 3 ***********/
  InitSub2(3, "Accepted in 1 server (leader). Prepare returns (F+1)/2 identical pre-accepted replies.");
  cmd++;
  time_to_sleep = 1200;
  // Keep only slow-path quorum of servers alive
  for (int i = 1; i <= NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Disconnect(i);
  }
  // Repeat till only 1 server (leader) have committed the command
  while (true) {
    dependent_cmds.insert(cmd);
    config_->Start(CMD_LEADER, cmd, dkey);
    Coroutine::Sleep(time_to_sleep);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Disconnect(i);
    }
    config_->Disconnect(CMD_LEADER);
    config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
    auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
    Assert2(na <= SLOW_PATH_QUORUM, "unexpected number of accepted servers");
    auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
    AssertValidCommitStatus(replica_id, instance_no, nc);
    Assert2(nc <= SLOW_PATH_QUORUM, "unexpected number of committed servers");
    if (na == 1 && nc == 0) break;
    // Retry if more than 1 server accepted
    time_to_sleep = (na < 1 && nc == 0) ? (time_to_sleep + diff) : (time_to_sleep - diff);
    cmd++;
    config_->Reconnect(CMD_LEADER);
    for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS; i++) {
      config_->Reconnect(i);
    }
  }
  // Reconnect (F+1)/2 identically preaccepted servers, remaining not pre-accepted servers (to meet majority) and commit in all via prepare
  int F = NSERVERS/2;
  for (int i = NSERVERS - SLOW_PATH_QUORUM + 1; i < NSERVERS - SLOW_PATH_QUORUM + 1 + (F+1)/2; i++) {
    config_->Reconnect(i);
  }
  for (int i = 1; i <= SLOW_PATH_QUORUM - ((F+1)/2); i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(CMD_LEADER + 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  // Reconnect all and commit via prepare
  for (int i = 0; i < NSERVERS; i++) {
    if (config_->IsDisconnected(i)) {
      config_->Reconnect(i);
    }
  }
  config_->Prepare(NSERVERS - 1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Execution order ***********/
  InitSub2(3, "Execution order");
  config_->PauseExecution(false);
  Coroutine::Sleep(5000000);
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

int EpaxosTest::testPreparePreAcceptedCommandAgree(void) {
  Init2(10, "Commit through prepare - pre-accepted but not in majority");
  config_->PauseExecution(true);
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Pre-accepted in 1 server (leader). Prepare return 1 pre-accepted reply from leader (avoid fast-path).");
  cmd++;
  string dkey = "6";
  unordered_set<uint64_t> dependent_cmds;
  int CMD_LEADER = 2;
  uint64_t replica_id, instance_no;
  // Disconnect leader
  config_->Disconnect(CMD_LEADER);
  // Start agreement in leader - will not replicate as leader is disconnected
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == 1, "unexpected number of pre-accepted servers");
  auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Reconnect leader and commit via prepare
  config_->Reconnect(CMD_LEADER);
  // Set non-leaders slow to prevent from committing noop
  config_->SetSlow((CMD_LEADER + 1) % NSERVERS, 100);
  config_->SetSlow((CMD_LEADER + 2) % NSERVERS, 100);
  config_->SetSlow((CMD_LEADER + 3) % NSERVERS, 100);
  config_->SetSlow((CMD_LEADER + 4) % NSERVERS, 100);
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);
  config_->ResetSlow((CMD_LEADER + 1) % NSERVERS);
  config_->ResetSlow((CMD_LEADER + 2) % NSERVERS);
  config_->ResetSlow((CMD_LEADER + 3) % NSERVERS);
  config_->ResetSlow((CMD_LEADER + 4) % NSERVERS);
  
  /*********** Sub Test 2 ***********/
  InitSub2(2, "Pre-accepted in 2 server (leader and replica). Prepare returns 1 pre-accepted reply from another replica (slow-path).");
  cmd++;
  // Disconnect all servers except leader and 1 replica
  for (int i = 2; i < NSERVERS; i++) {
    config_->Disconnect((CMD_LEADER + i) % NSERVERS);
  }
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == 2, "unexpected number of pre-accepted servers");
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Disconnect leader and reconnect majority-1 non pre-accepted servers to commit pre-accepted command via prepare
  config_->Disconnect(CMD_LEADER);
  for (int i = 2; i < (NSERVERS/2)+2; i++) {
    config_->Reconnect((CMD_LEADER + i) % NSERVERS);
  }
  config_->Prepare((CMD_LEADER + 2) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  config_->Reconnect(CMD_LEADER);
  for (int i = (NSERVERS/2)+2; i < NSERVERS; i++) {
    config_->Reconnect((CMD_LEADER + i) % NSERVERS);
  }
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Sub Test 3 ***********/
  InitSub2(3, "Pre-accepted in (F+1)/2 servers. Prepare returns (F+1)/2 identical pre-accepted replies but includes leader.");
  cmd++;
  CMD_LEADER = 0;
  // Keep only (F+1)/2 quorum of servers alive
  int X = ((NSERVERS/2) + 1)/2 - 1;
  for (int i = 1; i < NSERVERS - X; i++) {
    config_->Disconnect(i);
  }
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == X+1, "unexpected number of pre-accepted servers");
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Reconnect majority servers to commit pre-accepted command via prepare
  for (int i = NSERVERS - SLOW_PATH_QUORUM; i < NSERVERS - X; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(NSERVERS - SLOW_PATH_QUORUM, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  // Reconnect all and commit via prepare
  for (int i = 1; i < NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

 /*********** Sub Test 4 ***********/
  InitSub2(4, "Pre-accepted in leader and (F+1)/2 servers. Prepare returns (F+1)/2 identical pre-accepted replies.");
  cmd++;
  CMD_LEADER = 0;
  // Keep only leader + (F+1)/2 quorum of servers alive
  X = ((NSERVERS/2) + 1)/2;
  for (int i = 1; i < NSERVERS - X; i++) {
    config_->Disconnect(i);
  }
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  dependent_cmds.insert(cmd);
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == X+1, "unexpected number of pre-accepted servers");
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Reconnect majority servers to commit pre-accepted command via prepare
  config_->Disconnect(CMD_LEADER);
  for (int i = NSERVERS - SLOW_PATH_QUORUM ; i < NSERVERS - X; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(NSERVERS - SLOW_PATH_QUORUM, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, SLOW_PATH_QUORUM, false);
  // Reconnect all and commit via prepare
  config_->Reconnect(CMD_LEADER);
  for (int i = 1; i < NSERVERS - SLOW_PATH_QUORUM; i++) {
    config_->Reconnect(i);
  }
  config_->Prepare(1, replica_id, instance_no);
  AssertNCommittedAndVerifyNoop(replica_id, instance_no, NSERVERS, false);

  /*********** Execution order ***********/
  InitSub2(3, "Execution order");
  config_->PauseExecution(false);
  Coroutine::Sleep(5000000);
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

int EpaxosTest::testPrepareNoopCommandAgree(void) {
  Init2(11, "Commit through prepare - commit noop");
  config_->PauseExecution(true);
  /*********** Sub Test 1 ***********/
  InitSub2(1, "Pre-accepted in 1 server (leader). Prepare returns no replies (avoid fast-path).");
  int cmd1 = ++cmd;
  string dkey = "7";
  int CMD_LEADER = 3;
  uint64_t replica_id, instance_no;
  auto deps = map<uint64_t, uint64_t>();
  int seq = 0;
  // Disconnect leader
  config_->Disconnect(CMD_LEADER);
  // Start agreement in leader - will not replicate as leader is disconnected
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  auto np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == 1, "unexpected number of pre-accepted servers");
  auto na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  auto nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Commit noop in others via prepare
  config_->Prepare((CMD_LEADER + 1) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, NSERVERS-1, true, NOOP_DKEY, seq, deps);
  // Reconnect leader and see if noop is committed in all via prepare
  config_->Reconnect(CMD_LEADER);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, NSERVERS, true, NOOP_DKEY, seq, deps);
  
  /*********** Sub Test 2 ***********/
  InitSub2(2, "Pre-accepted in 2 server (leader and replica). Prepare returns no replies (slow path). (May fail sometimes)");
  int cmd2 = ++cmd;
  // Disconnect all servers except leader and 1 replica
  for (int i = 2; i < NSERVERS; i++) {
    config_->Disconnect((CMD_LEADER + i) % NSERVERS);
  }
  // Start agreement in leader - will replicate to only 1 replica as others are disconnected
  config_->Start(CMD_LEADER, cmd, dkey);
  config_->GetInstance(CMD_LEADER, cmd, &replica_id, &instance_no);
  np = config_->NPreAccepted(replica_id, instance_no, NSERVERS);
  Assert2(np == 2, "unexpected number of pre-accepted servers");
  na = config_->NAccepted(replica_id, instance_no, NSERVERS);
  Assert2(na == 0, "unexpected number of accepted servers");
  nc = config_->NCommitted(replica_id, instance_no, NSERVERS);
  AssertValidCommitStatus(replica_id, instance_no, nc);
  Assert2(nc == 0, "unexpected number of committed servers");
  // Disconnect pre-accepted servers and commit noop in others via prepare
  config_->Disconnect(CMD_LEADER);
  config_->Disconnect((CMD_LEADER + 1) % NSERVERS);
  for (int i = 2; i < NSERVERS; i++) {
    config_->Reconnect((CMD_LEADER + i) % NSERVERS);
  }
  config_->Prepare((CMD_LEADER + 2) % NSERVERS, replica_id, instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, SLOW_PATH_QUORUM, true, NOOP_DKEY, seq, deps);
  // Reconnect leader and other server and see if noop is committed in all
  config_->Reconnect(CMD_LEADER);
  config_->Reconnect((CMD_LEADER + 1) % NSERVERS);
  config_->Prepare(CMD_LEADER, replica_id, instance_no);
  AssertNCommittedAndVerifyAttrs(replica_id, instance_no, NSERVERS, true, NOOP_DKEY, seq, deps);

  /*********** Execution order ***********/
  InitSub2(3, "Execution order");
  config_->PauseExecution(false);
  Coroutine::Sleep(5000000);
  AssertNoneExecuted(cmd1);
  AssertNoneExecuted(cmd2);
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
  caargs->config->Start(caargs->svr, caargs->cmd, caargs->dkey);
  caargs->config->GetInstance(caargs->svr, caargs->cmd, &replica_id, &instance_no);
  std::lock_guard<std::mutex> lock(*(caargs->mtx));
  caargs->retvals->push_back(make_pair(replica_id, instance_no));
  return nullptr;
}

int EpaxosTest::testConcurrentAgree(void) {
  Init2(12, "Concurrent agreements");
  config_->PauseExecution(false);
  std::vector<pthread_t> threads{};
  std::vector<std::pair<uint64_t, uint64_t>> retvals{};
  std::mutex mtx{};
  unordered_set<uint64_t> dependent_cmds;
  for (int iter = 1; iter <= 50; iter++) {
    for (int svr = 0; svr < NSERVERS; svr++) {
      CAArgs *args = new CAArgs{};
      args->cmd = ++cmd;
      args->svr = svr;
      args->dkey = "8";
      args->mtx = &mtx;
      args->retvals = &retvals;
      args->config = config_;
      dependent_cmds.insert(cmd);
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
  Coroutine::Sleep(30000000);
  Assert2(retvals.size() == (50 * NSERVERS), "Failed to reach agreement");
  for (auto retval : retvals) {
    AssertNCommitted(retval.first, retval.second, NSERVERS);
  }
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

int EpaxosTest::testConcurrentUnreliableAgree(void) {
  Init2(13, "Unreliable concurrent agreement");
  config_->PauseExecution(false);
  config_->SetUnreliable(true);
  std::vector<pthread_t> threads{};
  std::vector<std::pair<uint64_t, uint64_t>> retvals{};
  std::mutex mtx{};
  unordered_set<uint64_t> dependent_cmds;
  for (int iter = 1; iter <= 50; iter++) {
    for (int svr = 0; svr < NSERVERS; svr++) {
      CAArgs *args = new CAArgs{};
      args->cmd = ++cmd;
      args->svr = svr;
      args->dkey = "9";
      args->mtx = &mtx;
      args->retvals = &retvals;
      args->config = config_;
      dependent_cmds.insert(cmd);
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
  config_->SetUnreliable(false);
  Coroutine::Sleep(30000000);
  Assert2(retvals.size() == (50 * NSERVERS), "Failed to reach agreement");
  for (auto retval : retvals) {
    AssertNCommitted(retval.first, retval.second, NSERVERS);
  }
  AssertSameExecutedOrder(dependent_cmds);
  Passed2();
}

#endif

}
