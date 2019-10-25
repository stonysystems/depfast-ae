
#pragma once

#include <memory>
#include <algorithm>
#include <fstream>
#include <unordered_set>
#include <map>
//#include "../../deptran/client_worker.h"
#include "../base/all.hpp"

#define SUCCESS (0)
#define REJECT (-10)

namespace rrr {
using std::shared_ptr;
using std::function;
using std::vector;

class Reactor;
class Coroutine;
class Event : public std::enable_shared_from_this<Event> {
//class Event {
 public:
  int __debug_creator{0};
  enum EventStatus { INIT = 0, WAIT = 1, READY = 2,
      DONE = 3, TIMEOUT = 4, DEBUG};
  EventStatus status_{INIT};
  void* _dbg_p_scheduler_{nullptr};
  uint64_t type_{0};
  function<bool(int)> test_{};
  uint64_t wakeup_time_; // calculated by timeout, unit: microsecond

  // An event is usually allocated on a coroutine stack, thus it cannot own a
  //   shared_ptr to the coroutine it is.
  // In this case there is no shared pointer to the event.
  // When the stack that contains the event frees, the event frees.
  std::weak_ptr<Coroutine> wp_coro_{};

  virtual void Wait(uint64_t timeout=0) final;

  void Wait(function<bool(int)> f) {
    test_ = f;
    Wait();
  }


  virtual void log(){return;}
  virtual bool Test();
  virtual bool IsReady() {
    verify(test_);
    return test_(0);
  }
  virtual uint64_t GetCoroId();

  friend Reactor;
// protected:
  Event();
};

template <class Type>
class BoxEvent : public Event {
 public:
  Type content_{};
  bool is_set_{false};
  Type& Get() {
    return content_;
  }
  void Set(const Type& c) {
    is_set_ = true;
    content_ = c;
    Test();
  }
  void Clear() {
    is_set_ = false;
    content_ = {};
  }
  virtual bool IsReady() override {
    return is_set_;
  }
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
//    TestTrigger();
    Test();
    return t;
  };

  virtual bool IsReady() override {
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
  vector<shared_ptr<IntEvent>> events_{};
  int Set(const int& v) {
    auto ret = value_;
    value_ = v;
    for (auto sp_ev : events_) {
      if (sp_ev->status_ <= Event::WAIT)
      sp_ev->Set(v);
    }
    return ret;
  }

  void Wait(function<bool(int)> f);
  void WaitUntilGreaterOrEqualThan(int x);
};


class NeverEvent: public Event {
 public:
  bool IsReady() override {
    return false;
  }
};

class SingleRPCEvent: public Event{
  public:
    uint32_t cli_id_;
    uint32_t coo_id_;
    std::string log_file = "logs.txt";
    std::unordered_set<int> dep{};
    SingleRPCEvent(uint32_t cli_id, uint32_t coo_id): Event(),
                                                      cli_id_(cli_id),
                                                      coo_id_(coo_id){
    }
    void add_dep(int tgtId){
      auto index = dep.find(tgtId);
      if(index == dep.end()) dep.insert(tgtId);
      Log_info("size of dependencies: %d", dep.size());
    }
    void log(){
      std::ofstream of(log_file, std::fstream::app);
      //of << "hello\n";
      of << "{ " << cli_id_ << ": ";
      for(auto it = dep.begin(); it != dep.end(); it++){
        of << *it << " ";
      }
      of << "}\n";
      of.close();
    }
    void Wait() override{
      log();
    }
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

  void log() {
    for(int i = 0; i < events_.size(); i++){
      events_[i]->log();
    }
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
        return true;
      }
      Log_info("FALSE");
      return false;
    }

};


class SingleRPCEvent: public Event{
  public:
    uint32_t cli_id_;
    uint32_t coo_id_;
    int32_t& res_;
    std::string log_file = "logs.txt";
    std::unordered_set<int> dep{};
    SingleRPCEvent(uint32_t cli_id, int32_t res): Event(),
                                                   cli_id_(cli_id),
                                                   res_(res){
    }
    void add_dep(int tgtId){
      auto index = dep.find(tgtId);
      if(index == dep.end()) dep.insert(tgtId);
      //Log_info("size of dependencies: %d", dep.size());
    }
    void log(){
      std::ofstream of(log_file, std::fstream::app);
      //of << "hello\n";
      of << "{ " << cli_id_ << ": ";
      for(auto it = dep.begin(); it != dep.end(); it++){
        of << *it << " ";
      }
      of << "}\n";
      of.close();
    }
    bool IsReady() override{
      //Log_info("READY");
      return res_ == SUCCESS || res_ == REJECT;
    }
    return false;
    
    //return std::all_of(events_.begin(), events_.end(), [](shared_ptr<Event> e){return e->IsReady();});
  }
};

}
