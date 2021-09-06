#pragma once

#include "../__dep__.h"
#include "../constants.h"
#include "../scheduler.h"
#include "../classic/tpc_command.h"

namespace janus {

enum Status : status_t { NOT_ACCEPTED = 0, FAST_ACCEPTED, ACCEPTED, COMMITED, EXECUTED, TAKEOVER };
const size_t n_status = 5;

struct CopilotData {
  shared_ptr<Marshallable>  cmd{nullptr};  // command
  slotid_t                  dep_id;  // dependency
  uint8_t                   is_pilot;
  slotid_t                  slot_id;  // position
  ballot_t                  ballot;  // ballot
  status_t                  status;  // status
  int                       low, dfn;  //torjan
};

struct CopilotLogInfo {
  map<slotid_t, shared_ptr<CopilotData> > logs;
  slotid_t current_slot = 0;
  slotid_t min_active_slot = 0; // anything before (lt) this slot is freed
  slotid_t max_executed_slot = 0;
  slotid_t max_committed_slot = 0;
  slotid_t max_accepted_slot = 0;
};

struct KeyValue {
  int key;
  i32 value;
};

class CopilotServer : public TxLogServer {
 private:
  uint16_t id_;
  bool isPilot_ = false;
  bool isCopilot_ = false;

  void setIsPilot(bool isPilot);
  void setIsCopilot(bool isCopilot);
  const char* toString(uint8_t is_pilot);

 private:
  std::vector<CopilotLogInfo> log_infos_;

 public:
  CopilotServer(Frame *frame);
  ~CopilotServer() {}

  shared_ptr<CopilotData> GetInstance(slotid_t slot, uint8_t is_pilot);
  std::pair<slotid_t, uint64_t> PickInitSlotAndDep();

  void Setup();

  void OnForward(shared_ptr<Marshallable>& cmd,
                 const function<void()> &cb);

  void OnPrepare(const uint8_t& is_pilot,
                 const uint64_t& slot,
                 const ballot_t& ballot,
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
                    ballot_t* max_ballot,
                    uint64_t* ret_dep,
                    const function<void()> &cb);

  void OnAccept(const uint8_t& is_pilot,
                const uint64_t& slot,
                const ballot_t& ballot,
                const uint64_t& dep,
                shared_ptr<Marshallable>& cmd,
                ballot_t* max_ballot,
                const function<void()> &cb);

  void OnCommit(const uint8_t& is_pilot,
                const uint64_t& slot,
                const uint64_t& dep,
                shared_ptr<Marshallable>& cmd);
 private:
  bool executeCmd(shared_ptr<CopilotData>& ins);
  bool findSCC(shared_ptr<CopilotData>& root);
  bool strongConnect(shared_ptr<CopilotData>& ins, int* index);
  void updateMaxExecSlot(shared_ptr<CopilotData>& ins);
  void updateMaxAcptSlot(CopilotLogInfo& log_info, slotid_t slot);
  void updateMaxCmtdSlot(CopilotLogInfo& log_info, slotid_t slot);
  std::stack<shared_ptr<CopilotData> > stack_;
};

} //namespace janus