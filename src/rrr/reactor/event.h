
#pragma once

#include <memory>
#include <algorithm>
#include <fstream>
#include <unordered_set>
#include <map>
//#include "../../deptran/client_worker.h"
#include "../base/all.hpp"

namespace rrr {
using std::shared_ptr;
using std::function;
using std::vector;

class Reactor;
class Coroutine;
class Event {
 public:
  int __debug_creator{0};
  enum EventStatus { INIT = 0, WAIT = 1, READY = 2, DONE = 3, DEBUG};
  EventStatus status_{INIT};
  void* _dbg_p_scheduler_{nullptr};
  uint64_t type_{0};
  function<bool(int)> test_{};

  // An event is usually allocated on a coroutine stack, thus it cannot own a
  //   shared_ptr to the coroutine it is.
  // In this case there is no shared pointer to the event.
  // When the stack that contains the event frees, the event frees.
  std::weak_ptr<Coroutine> wp_coro_{};

  virtual void Wait();
  virtual bool Test();
  virtual bool IsReady() {return false;}

  friend Reactor;
 protected:
  Event();
};

class IntEvent : public Event {

 public:
  IntEvent() {}
  int value_{0};
  int target_{1};


  bool TestTrigger();

  int get() {
    return value_;
  }

  int Set(int n) {
    int t = value_;
    value_ = n;
    TestTrigger();
    return t;
  };

  bool IsReady() override {
    if (test_) {
      return test_(value_);
    } else {
      return (value_ == target_);
    }
  }
};

class SharedIntEvent {
 public:
  int value_{};
  vector<shared_ptr<IntEvent>> events_;
  int Set(int& v) {
    auto ret = value_;
    value_ = v;
    for (auto sp_ev : events_) {
      if (sp_ev->status_ <= Event::WAIT)
      sp_ev->Set(v);
    }
    return ret;
  }

  void Wait(function<bool(int)> f);
};

class TimeoutEvent : public Event {
 public:
  uint64_t wakeup_time_{0};
  TimeoutEvent(uint64_t wait_us_): wakeup_time_{Time::now()+wait_us_} {}

  bool IsReady() override {
//    Log_debug("test timeout");
    return (Time::now() > wakeup_time_);
  }
};

class OrEvent : public Event {
 public:
  vector<shared_ptr<Event>> events_;

  void AddEvent() {
    // empty func for recursive variadic parameters
  }

  template<typename X, typename... Args>
  void AddEvent(X& x, Args&... rest) {
    events_.push_back(std::dynamic_pointer_cast<Event>(x));
    AddEvent(rest...);
  }

  template<typename... Args>
  OrEvent(Args&&... args) {
    AddEvent(args...);
  }

  bool IsReady() {
    return std::any_of(events_.begin(), events_.end(), [](shared_ptr<Event> e){return e->IsReady();});
  }
};

class AndEvent : public Event {
 public:
  vector<shared_ptr<Event>> events_;

  void AddEvent() {
    // empty func for recursive variadic parameters
  }

  template<typename X, typename... Args>
  void AddEvent(X& x, Args&... rest) {
    events_.push_back(std::dynamic_pointer_cast<Event>(x));
    AddEvent(rest...);
  }

  template<typename... Args>
  AndEvent(Args&&... args) {
    AddEvent(args...);
  }

  bool IsReady() {
    return std::all_of(events_.begin(), events_.end(), [](shared_ptr<Event> e){return e->IsReady();});
  }
};

class NEvent : public Event {
 public:
  vector<shared_ptr<Event>> events_;
  int number;

  void AddEvent() {
    // empty func for recursive variadic parameters
  }

  template<typename X, typename... Args>
  void AddEvent(X& x, Args&... rest) {
    events_.push_back(std::dynamic_pointer_cast<Event>(x));
    AddEvent(rest...);
  }

  template<typename... Args>
  NEvent(Args&&... args) {
    AddEvent(args...);
  }

  bool IsReady() {
    int count = 0;
    for(auto index = events_.begin(); index != events_.end(); index++){
      if((*index)->IsReady()){
        count++;
        if(count == number){
          return true;
        }
      }
    }
    return false;
    
    //return std::all_of(events_.begin(), events_.end(), [](shared_ptr<Event> e){return e->IsReady();});
  }
};

class DispatchEvent: public Event{
  public:
    uint32_t n_dispatch_;
    uint32_t n_dispatch_ack_ = 0;
    std::map<uint32_t, bool> dispatch_acks_ = {};
    bool aborted_ = false;
    bool more = false;

    DispatchEvent() : Event(){
    
    }
    bool IsReady() override{
      if(n_dispatch_ == n_dispatch_ack_){
        if(aborted_){
          Log_info("aborted");
          return true;
        }
        else{
          Log_info("DONE DONE: %p", this);
          return std::all_of(dispatch_acks_.begin(),
                             dispatch_acks_.end(),
                             [](std::pair<uint32_t, bool> pair) {
                             return pair.second;
                             });
        }
      }
      else if(more){
        Log_info("more");
        return true;
      }
      Log_info("FALSE");
      return false;
    }

};

}
