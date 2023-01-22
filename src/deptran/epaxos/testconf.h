#pragma once

#include "frame.h"
#include "coordinator.h"

namespace janus {

#ifdef EPAXOS_TEST_CORO

// 5 servers in test configuration
#define NSERVERS 5
#define FAST_PATH_QUORUM 4
#define SLOW_PATH_QUORUM 3
// slow network connections have latency up to 26 milliseconds
#define MAXSLOW 27
// servers have 1/10 chance of being disconnected to the network
#define DOWNRATE_N 1
#define DOWNRATE_D 10
// expected # of RPCs taken to commit n agreements
#define COMMITRPCS(n) ((n + 1) * NSERVERS)


#define Print(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)

extern int _test_id_g;
#define Init(test_id, description) \
  Print("TEST %d: " description, test_id); \
  _test_id_g = test_id
#define Failed(msg, ...) Print("TEST %d Failed: " msg, _test_id_g, ##__VA_ARGS__)
#define Passed() Print("TEST %d Passed", _test_id_g)

class CommitIndex {
 private:
  uint64_t val_;
 public:
  CommitIndex(uint64_t val) : val_(val) {}
  uint64_t getval(void) { return val_; }
  void setval(uint64_t val) { val_ = val; }
};

class EpaxosTestConfig {

 private:
  static EpaxosFrame **replicas;
  static std::function<void(Marshallable &)> commit_callbacks[NSERVERS];
  static std::vector<int> committed_cmds[NSERVERS];
  static uint64_t rpc_count_last[NSERVERS];

  // disconnected_[svr] true if svr is disconnected by Disconnect()/Reconnect()
  bool disconnected_[NSERVERS];
  // guards disconnected_ between Disconnect()/Reconnect() and netctlLoop
  std::mutex disconnect_mtx_;

 public:
  EpaxosTestConfig(EpaxosFrame **replicas);

  // sets up learner action functions for the servers
  // so that each committed command on each server is
  // logged to this test's data structures.
  void SetLearnerAction(void);

  // Returns number of servers that think log entry at index is committed.
  // Checks if the committed value for index is the same across servers.
  int NCommitted(uint64_t tx_id);

  // Calls Start() to specified server
  void Start(int svr, int cmd, string dkey, uint64_t *replica_id, uint64_t *instance_no);

  // Get state of the command at an instance replica_id.instance_no in a specified server
  void GetState(int svr, 
                uint64_t replica_id, 
                uint64_t instance_no, 
                shared_ptr<Marshallable> *cmd, 
                string *string,
                uint64_t *seq, 
                unordered_map<uint64_t, uint64_t> *deps, 
                bool *committed);

  // Waits for at least n servers to commit index
  // If commit takes too long, gives up after a while.
  // If term has moved on since the given start term, also gives up.
  // Returns the committed value on success.
  // -1 if it took too long for enough servers to commit
  // -2 if term changed
  // -3 if committed values for index differ
  // int Wait(uint64_t index, int n, uint64_t term);

  // Does one agreement.
  // Submits a command with value cmd to the leader
  // Waits at most 2 seconds until n servers commit the command.
  // Makes sure the value of the commits is the same as what was given.
  // If retry == true, Retries the agreement until at most 10 seconds pass.
  // Returns true on success, false on error.
  bool DoAgreement(int cmd, 
                   string dkey, 
                   int n, 
                   bool retry, 
                   bool *cno_op, 
                   string *cdkey, 
                   uint64_t *cseq, 
                   unordered_map<uint64_t, uint64_t> *cdeps);

  // Disconnects server from rest of servers
  void Disconnect(int svr);

  // Checks if server was disconnected from rest of servers
  bool IsDisconnected(int svr);

  // Reconnects disconnected server
  void Reconnect(int svr);

  // Returns number of disconnected servers
  int NDisconnected(void);

  // Sets/unsets network unreliable
  // Blocks until network successfully set to unreliable/reliable
  // If unreliable == true, previous call to SetUnreliable must have been false
  // and vice versa
  void SetUnreliable(bool unreliable = true);

  bool IsUnreliable(void);

  // Reconnects all disconnected servers
  // Waits on unreliable thread
  void Shutdown(void);

  // Resets RPC counts to zero
  void RpcCountReset(void);

  // Returns total RPC count for a server
  // if reset, the next time RpcCount called for
  // svr, the count will exclude all RPCs before this call
  uint64_t RpcCount(int svr, bool reset = true);

  // Returns total RPC count across all servers
  // since server setup.
  uint64_t RpcTotal(void);

  // Returns true if svr committed a log entry at index with value cmd
  bool ServerCommitted(int svr, uint64_t index, int cmd);

 private:
  // vars & subroutine for unreliable network setting
  std::thread th_;
  std::mutex cv_m_; // guards cv_, unreliable_, finished_
  std::condition_variable cv_;
  bool unreliable_ = false;
  bool finished_ = false;
  void netctlLoop(void);

  // internal disconnect/reconnect/slow functions
  std::recursive_mutex connection_m_;
  bool isDisconnected(int svr);
  void disconnect(int svr, bool ignore = false);
  void reconnect(int svr, bool ignore = false);
  void slow(int svr, uint32_t msec);

  // other internal helpers
  int waitOneLeader(bool want_leader, int expected);

};

#endif

}
