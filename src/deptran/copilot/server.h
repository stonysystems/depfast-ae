#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"
#include <stack>

#define REVERSE(p) (1 - (p))

namespace janus {

const status_t FLAG_TAKEOVER = 0x80000000;
const status_t CLR_FLAG_TAKEOVER = (~FLAG_TAKEOVER);
#define GET_STATUS(s) ((s) & CLR_FLAG_TAKEOVER)
#define GET_TAKEOVER(s) ((s) & FLAG_TAKEOVER)
#define SET_STATUS(s_old, s_new) ((s_old) = (s_new) | GET_TAKEOVER(s_old))

enum Status : status_t { NOT_ACCEPTED = 0, FAST_ACCEPTED, FAST_ACCEPTED_EQ, ACCEPTED, COMMITED, EXECUTED };
const size_t n_status = 5;

struct CopilotData {
  shared_ptr<Marshallable>  cmd{nullptr};  // command
  slotid_t                  dep_id;  // dependency
  uint8_t                   is_pilot;
  slotid_t                  slot_id;  // position
  ballot_t                  ballot;  // ballot
  status_t                  status;  // status
  int                       low, dfn;  //tarjan
  SharedIntEvent            cmit_evt{};
};

struct CopilotLogInfo {
  std::map<slotid_t, shared_ptr<CopilotData> > logs;
  slotid_t current_slot = 0;
  slotid_t min_active_slot = 1; // anything before (lt) this slot is freed
  slotid_t max_executed_slot = 0;
  slotid_t max_committed_slot = 0;
  slotid_t max_accepted_slot = 0;
  slotid_t max_dep = 0;
  SharedIntEvent max_cmit_evt{};
};

struct KeyValue {
  int key;
  i32 value;
};

class CopilotServer : public TxLogServer {
  using copilot_stack_t = std::stack<shared_ptr<CopilotData> >;
  using visited_map_t = std::map<shared_ptr<CopilotData>, bool>;
 private:
  uint16_t id_;
  bool isPilot_ = false;
  bool isCopilot_ = false;

  void setIsPilot(bool isPilot);
  void setIsCopilot(bool isCopilot);
  const char* toString(uint8_t is_pilot);

 private:
  std::vector<CopilotLogInfo> log_infos_;

  SharedIntEvent pingpong_event_{};
  bool pingpong_ok_;
  uint64_t last_ready_time_ = 0;
  uint64_t n_timeout = 0;

 public:
  CopilotServer(Frame *frame);
  ~CopilotServer() {}

  shared_ptr<CopilotData> GetInstance(slotid_t slot, uint8_t is_pilot);
  std::pair<slotid_t, uint64_t> PickInitSlotAndDep();
  slotid_t GetMaxCommittedSlot(uint8_t is_copilot);
  bool WaitMaxCommittedGT(uint8_t is_pilot, slotid_t slot, int timeout=0);
  
  /**
   * If the log entry has been executed (in another log), mark it as EXECUTED,
   * and update max_committed_slot and max_executed_slot accordingly
   * 
   * @param ins log entry to be check
   * @return true if executed, false otherwise
   */
  bool EliminateNullDep(shared_ptr<CopilotData>& ins);
  bool AllDepsEliminated(uint8_t is_pilot, slotid_t dep_id);

  void Setup();

  // Wait for Ping-Pong signal from another pilot, or timeout and proceed to FastAccept
  void WaitForPingPong();
  bool WillWait(int& time_to_wait) const;

  void OnForward(shared_ptr<Marshallable>& cmd,
                 const function<void()> &cb);

  void OnPrepare(const uint8_t& is_pilot,
                 const uint64_t& slot,
                 const ballot_t& ballot,
                 const struct DepId& dep_id,
                 MarshallDeputy* ret_cmd,
                 ballot_t* max_ballot,
                 uint64_t* dep,
                 status_t* status,
                 const function<void()> &cb);

  void OnFastAccept(const uint8_t& is_pilot,
                    const uint64_t& slot,
                    const ballot_t& ballot,
                    const uint64_t& dep,
                    shared_ptr<Marshallable>& cmd,
                    const struct DepId& dep_id,
                    ballot_t* max_ballot,
                    uint64_t* ret_dep,
                    const function<void()> &cb);

  void OnAccept(const uint8_t& is_pilot,
                const uint64_t& slot,
                const ballot_t& ballot,
                const uint64_t& dep,
                shared_ptr<Marshallable>& cmd,
                const struct DepId& dep_id,
                ballot_t* max_ballot,
                const function<void()> &cb);

  void OnCommit(const uint8_t& is_pilot,
                const uint64_t& slot,
                const uint64_t& dep,
                shared_ptr<Marshallable>& cmd);
 private:
  bool executeCmd(shared_ptr<CopilotData>& ins);
  bool executeCmds(shared_ptr<CopilotData>& ins);
  void waitAllPredCommit(shared_ptr<CopilotData>& ins);
  void waitPredCmds(shared_ptr<CopilotData>& ins, shared_ptr<visited_map_t> map);
  bool findSCC(shared_ptr<CopilotData>& root);
  bool strongConnect(shared_ptr<CopilotData>& ins, int* index);
  bool checkAllDepExecuted(uint8_t is_pilot, slotid_t start, slotid_t end);
  // void strongConnect(shared_ptr<CopilotData>& ins, int* index, copilot_stack_t *stack);
  void updateMaxExecSlot(shared_ptr<CopilotData>& ins);
  void updateMaxAcptSlot(CopilotLogInfo& log_info, slotid_t slot);
  void updateMaxCmtdSlot(CopilotLogInfo& log_info, slotid_t slot);
  void removeCmd(CopilotLogInfo& log_info, slotid_t slot);
  copilot_stack_t stack_;

  bool isExecuted(shared_ptr<CopilotData>& ins);
  bool allCmdComitted(shared_ptr<Marshallable> batch_cmd);
};

} //namespace janus