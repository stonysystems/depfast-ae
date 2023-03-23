#pragma once

#include "frame.h"

namespace janus {

#if defined(EPAXOS_TEST_CORO) || defined(EPAXOS_PERF_TEST_CORO)

// slow network connections have latency up to 26 milliseconds
#define MAXSLOW 27
// servers have 1/10 chance of being disconnected to the network
#define DOWNRATE_N 1
#define DOWNRATE_D 10
#define DKEY_NOOP ""

#define Print(format, ...) fprintf(stderr, format "\n", ##__VA_ARGS__)

extern int _test_id_g;
extern std::chrono::_V2::system_clock::time_point _test_starttime_g;
#define Init(test_id, description) \
        Print("TEST %d: " description, test_id); \
        _test_id_g = test_id; \
        _test_starttime_g = std::chrono::system_clock::now();

#define InitSub(sub_test_id, description) \
        Print("TEST %d.%d: " description, _test_id_g, sub_test_id);

#define Failed(msg, ...) Print("TEST %d Failed: " msg, _test_id_g, ##__VA_ARGS__);

#define Passed() \
        Print("TEST %d Passed (time taken: %d s)", _test_id_g, (std::chrono::system_clock::now() - _test_starttime_g)/1000000000);

extern string map_to_string(unordered_map<uint64_t, uint64_t> m);

class EpaxosTestConfig {

 private:
  static EpaxosFrame **replicas;
  static std::function<void(Marshallable &)> commit_callbacks[NSERVERS];
  static std::vector<int> committed_cmds[NSERVERS];
  static std::atomic<int> committed_count[NSERVERS];
  static uint64_t rpc_count_last[NSERVERS];

  // disconnected_[svr] true if svr is disconnected by Disconnect()/Reconnect()
  bool disconnected_[NSERVERS];
  // slow_[svr] true if svr is set as slow by SetSlow()
  bool slow_[NSERVERS];
  // guards disconnected_ between Disconnect()/Reconnect() and netctlLoop
  std::mutex disconnect_mtx_;

 public:
  EpaxosTestConfig(EpaxosFrame **replicas);

  // sets up learner action functions for the servers
  // so that each committed command on each server is
  // logged to this test's data structures.
  void SetLearnerAction(void);

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
                status_t *state);

  // Calls Prepare() to specified server
  void Prepare(int svr, uint64_t replica_id, uint64_t instance_no);

  // Calls PauseExecution() in all servers
  void PauseExecution(bool pause);

  // Return all executed commands in the executed order
  vector<int> GetExecutedCommands(int svr);

  // Return total number of executed commands
  int GetExecutedCount(int svr);

  // Returns 1 if n servers executed the command
  int NExecuted(uint64_t tx_id, int n);

  // Returns true if all servers executed first command in pair before second command (if executed)
  bool ExecutedPairsInOrder(vector<pair<uint64_t, uint64_t>> expected_pairs);

  // Check if the commands are executed in same order in all servers
  bool ExecutedInSameOrder(unordered_set<uint64_t> dependent_cmds);

  // Returns number of servers committed the command if atleast n servers committed and 
  // the committed cmd and attr are same across all commited servers.
  // Waits at most 2 seconds until n servers commit the command.
  // 0 if it took too long for enough servers to commit
  // -1 if committed values of command kind differ
  // -2 if committed values of dependency key kind differ
  // -3 if committed values of seq kind differ
  // -4 if committed values of deps kind differ
  int NCommitted(uint64_t replica_id, uint64_t instance_no, int n);

  // Returns number of servers committed the command if atleast n servers committed and 
  // the committed cmd and attr are same across all commited servers.
  // Waits at most 2 seconds until n servers commit the command.
  // 0 if it took too long for enough servers to commit
  // -1 if committed values of command kind differ
  // -2 if committed values of dependency key kind differ
  // -3 if committed values of seq kind differ
  // -4 if committed values of deps kind differ
  // Stores the committed command attributes in the args passed by pointer.
  int NCommitted(uint64_t replica_id, 
                 uint64_t instance_no, 
                 int n, 
                 bool *cno_op, 
                 string *cdkey, 
                 uint64_t *cseq, 
                 unordered_map<uint64_t, uint64_t> *cdeps);

  // Returns number of servers accepted the command 
  int NAccepted(uint64_t replica_id, uint64_t instance_no, int n);

  // Returns number of servers pre-accepted the command 
  int NPreAccepted(uint64_t replica_id, uint64_t instance_no, int n);

  // Does one agreement.
  // Submits a command with value cmd to the leader
  // Waits at most 2 seconds until n servers commit the command.
  // Makes sure the value of the commits is the same as what was given.
  // Stores the committed command attributes in the args passed by pointer.
  // Retries the agreement until at most 10 seconds pass.
  // Returns 1 if n servers committed the command and 
  // the committed cmd and attr are same across all commited servers.
  // 0 if it took too long for enough servers to commit
  // -1 if committed values of command kind differ
  // -2 if committed values of dependency key kind differ
  // -3 if committed values of seq kind differ
  // -4 if committed values of deps kind differ
  int DoAgreement(int cmd, 
                  string dkey, 
                  int n, 
                  bool retry, 
                  bool *cno_op, 
                  string *cdkey, 
                  uint64_t *cseq, 
                  unordered_map<uint64_t, uint64_t> *cdeps);

  // Disconnects server from rest of servers
  void Disconnect(int svr);

  // Reconnects disconnected server
  void Reconnect(int svr);

  // Returns number of disconnected servers
  int NDisconnected(void);

  // Checks if server was disconnected from rest of servers
  bool IsDisconnected(int svr);

  // Sets/unsets network unreliable
  // Blocks until network successfully set to unreliable/reliable
  // If unreliable == true, previous call to SetUnreliable must have been false
  // and vice versa
  void SetUnreliable(bool unreliable = true);

  bool IsUnreliable(void);

  // Slow down connections of a server
  void SetSlow(int svr, int msec);

  // Reset a slowed server
  void ResetSlow(int svr);

  // Returns if any server is set slow
  bool AnySlow(void);

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

};

#endif

}
