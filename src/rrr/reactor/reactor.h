#pragma once
#include <set>
#include <algorithm>
#include <unordered_map>
#include <list>
#include <thread>
#include "base/misc.hpp"
#include "event.h"
#include "quorum_event.h"
#include "coroutine.h"
#include "epoll_wrapper.h"

namespace rrr {

using std::make_unique;
using std::make_shared;

class Coroutine;
// TODO for now we depend on the rpc services, fix in the future.
class Reactor {
 public:
  static std::shared_ptr<Reactor> GetReactor();
  static std::shared_ptr<Reactor> GetDiskReactor();
  static thread_local std::shared_ptr<Reactor> sp_reactor_th_;
  static thread_local std::shared_ptr<Reactor> sp_disk_reactor_th_;
  static thread_local std::shared_ptr<Coroutine> sp_running_coro_th_;

  /*struct eventComp{
    bool operator()(const std::shared_ptr<Event>& lhs, const std::shared_ptf<Event>& rhs) const{
      return lhs->timeout
    }
  }*/

  /**
   * A reactor needs to keep reference to all coroutines created,
   * in case it is freed by the caller after a yield.
   */
  std::list<std::shared_ptr<Event>> all_events_{};
  std::list<std::shared_ptr<Event>> waiting_events_{};
  std::vector<std::shared_ptr<Event>> ready_events_{};
  std::list<std::shared_ptr<Event>> timeout_events_{};
  std::vector<std::shared_ptr<Event>> disk_events_{};
  std::list<std::shared_ptr<Event>> ready_disk_events_{};
  std::vector<std::shared_ptr<Event>> network_events_{};
  std::list<std::shared_ptr<Event>> ready_network_events_{};
  std::set<std::shared_ptr<Coroutine>> coros_{};
  std::vector<std::shared_ptr<Coroutine>> available_coros_{};
  std::unordered_map<uint64_t, std::function<void(Event&)>> processors_{};
  bool looping_{false};
	bool slow_{false};
	long disk_times[50];
	int disk_count{0};
	int disk_index{0};
	int slow_count{0};
  std::thread::id thread_id_{};
  static SpinLock disk_job_;
#ifdef REUSE_CORO
#define REUSING_CORO (true)
#else
#define REUSING_CORO (false)
#endif

  /**
   * @param ev. is usually allocated on coroutine stack. memory managed by user.
   */
  std::shared_ptr<Coroutine> CreateRunCoroutine(std::function<void()> func);
  void Loop(bool infinite = false);
  void DiskLoop();
	void NetworkLoop();
  void ContinueCoro(std::shared_ptr<Coroutine> sp_coro);

  ~Reactor() {
//    verify(0);
  }
  friend Event;

  
  template <typename Ev, typename... Args>
  static shared_ptr<Ev> CreateSpEvent(Args&&... args) {
    auto sp_ev = make_shared<Ev>(args...);
    sp_ev->__debug_creator = 1;
    // TODO push them into a wait queue when they actually wait.
    //events.push_back(sp_ev);
    //Log_info("ADDING %s %d", typeid(sp_ev).name(), events.size());
    return sp_ev;
  }

  template <typename Ev, typename... Args>
  static Ev& CreateEvent(Args&&... args) {
    auto sp_ev = CreateSpEvent<Ev>(args...);
    return *sp_ev;
  }
};

class PollMgr: public rrr::RefCounted {
 public:
    class PollThread;

    PollThread* poll_threads_;
    const int n_threads_;

protected:

    // RefCounted object uses protected dtor to prevent accidental deletion
    ~PollMgr();

public:

    PollMgr(int n_threads = 1);
    PollMgr(const PollMgr&) = delete;
    PollMgr& operator=(const PollMgr&) = delete;

    void add(shared_ptr<Pollable>);
    void remove(shared_ptr<Pollable>);
    void update_mode(shared_ptr<Pollable>, int new_mode);
    void pause();
    void resume();

    
    // Frequent Job
    void add(std::shared_ptr<Job> sp_job);
    void remove(std::shared_ptr<Job> sp_job);
};

} // namespace rrr
