#pragma once

#include <vector>
#include <functional>
#include <unordered_map>
#include <chrono>
#include "event.h"
#include "base/basetypes.hpp"

using rrr::Event;
using rrr::IntEvent;
using std::vector;
using std::function;
using std::shared_ptr;

namespace janus {

class QuorumEvent : public Event {
 public:
	static uint64_t count;
  int32_t n_voted_yes_{0};
  int32_t n_voted_no_{0};
  std::unordered_map<uint16_t, rrr::i64> xids_;
  uint64_t begin_timestamp_;

 public:
  int32_t n_total_ = -1;
  int32_t quorum_ = -1;
  int64_t highest_term_{0} ;
  bool timeouted_ = false;
  uint64_t cmt_idx_{0} ;
  uint32_t leader_id_{0} ;
  uint64_t coro_id_ = -1;
  int64_t par_id_ = -1;
  uint64_t id_ = -1;
	uint64_t server_id_ = -1;
  std::chrono::steady_clock::time_point ready_time;
  // fast vote result.
  vector<uint64_t> vec_timestamp_{};
  shared_ptr<IntEvent> finalize_event_;

  QuorumEvent() = delete;

  QuorumEvent(int n_total, int quorum);

  /**
   * Record the TXid of an issued RPC and which site it's issued to
   * in the dangling RPC list
   * 
   * @param site site id of the RPC issuing to
   * @param xid TXid of the RPC
   */
  void AddXid(uint16_t site, rrr::i64 xid);

  /**
   * Remove an replied RPC from the dangling RPC list
   * 
   * @param site site id of the reply coming from
   */
  void RemoveXid(uint16_t site);

  /**
   * call Finalize before/after Wait() to cleanup the side-effect of the quorun-event
   * (e.g. free dangling RPCs). However, Finalize should not block execution after Wait.
   * That is, Finalize should be a background task, with respect to the main coroutine (
   * the coroutine where Wait() is called)
   * TODO: find a proper way to achieve this
   *
   * @param timeout time to wait after event-ready to do finalize
   * @param finalize_func what to do in finalization, take a list of dangling RPC
   */
  void Finalize(uint64_t timeout, function<bool(vector<std::pair<uint16_t, rrr::i64> >&)> finalize_func);

  virtual bool Yes() {
    return n_voted_yes_ >= quorum_;
  }

  virtual bool No() {
    verify(n_total_ >= quorum_);
    return n_voted_no_ > (n_total_ - quorum_);
  }

  void VoteYes();

  void VoteNo();

  bool IsReady() override {
    if (timeouted_) {
      // TODO add time out support
      return true;
    }
    if (Yes()) {
//      Log_info("voted: %d is equal or greater than quorum: %d",
//                (int)n_voted_yes_, (int) quorum_);
      ready_time = std::chrono::steady_clock::now();
      return true;
    } else if (No()) {
      return true;
    }
//    Log_debug("voted: %d is smaller than quorum: %d",
//              (int)n_voted_, (int) quorum_);
    return false;
  }

  void Log();

};

} // namespace janus
