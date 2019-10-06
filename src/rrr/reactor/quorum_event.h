#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <fstream>
#include <iostream>
#include "event.h"

using rrr::Event;
using std::vector;

namespace janus {

class QuorumEvent : public Event {
 private:
  int32_t n_voted_yes_{0};
  int32_t n_voted_no_{0};
 public:
  int32_t n_total_ = -1;
  int32_t quorum_ = -1;
  bool timeouted_ = false;
  // fast vote result.
  vector<uint64_t> vec_timestamp_{};
  vector<int> sites_{}; // not sure if SiteInfo or int
  std::unordered_map<int, std::unordered_set<int>> deps{}; //not sure if SiteInfo or int
  std::string log_file = "logs.txt";

  QuorumEvent() = delete;

  //add the third parameter here
  //i don't want to mess things up
  QuorumEvent(int n_total,
              int quorum) : Event(),
                            n_total_(n_total),
                            quorum_(quorum) {
  }

  bool Yes() {
    return n_voted_yes_ >= quorum_;
  }

  bool No() {
    verify(n_total_ >= quorum_);
    return n_voted_no_ > (n_total_ - quorum_);
  }

  void VoteYes() {
    n_voted_yes_++;
    Test();
  }

  void VoteNo() {
    n_voted_no_++;
    Test();
  }

  void add_dep(int srcId, int tgtId){
    auto srcIndex = deps.find(srcId);
    if(srcIndex == deps.end()){
      std::unordered_set<int> temp = {};
      deps[srcId] = temp;
    }
    std::ofstream of(log_file, std::fstream::app);
    of << srcId << ", " << tgtId << "\n";
    of.close();
    auto tgtIndex = deps[srcId].find(tgtId);
    if(tgtIndex == deps[srcId].end() && srcId != tgtId){
      std::ofstream of(log_file, std::fstream::app);
      of << "hello\n";
      of.close();
      deps[srcId].insert(tgtId);
    }
  }

  void remove_dep(int srcId, int tgtId){
    auto srcIndex = deps.find(srcId);
    if(srcIndex != deps.end()){
      auto tgtIndex = deps[srcId].find(tgtId);
      if(tgtIndex != deps[srcId].end()){
        deps[srcId].erase(tgtId);
      }
    }
  }

  void log(){
    std::ofstream of(log_file, std::fstream::app);
    //of << "hello\n";
    for(auto it = deps.begin(); it != deps.end(); it++){
      of << "{ " << it->first << ": ";
      for(auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
        of << *it2 << " ";
      }
      of << "}\n";
    }
    of.close();
  }

  void Wait() override{
    log();
    Event::Wait();
  }

  bool IsReady() override {
    if (timeouted_) {
      // TODO add time out support
      return true;
    }
    if (Yes()) {
//      Log_debug("voted: %d is equal or greater than quorum: %d",
//                (int)n_voted_yes_, (int) quorum_);
      return true;
    } else if (No()) {
      return true;
    }
//    Log_debug("voted: %d is smaller than quorum: %d",
//              (int)n_voted_, (int) quorum_);
    return false;
  }

};

} // namespace janus
