#pragma once

#include <vector>
#include <unordered_map>
//#include <unordered_set>
//#include <fstream>
#include <iostream>
#include "event.h"

using rrr::Event;
using std::vector;

namespace janus {

class QuorumEvent : public Event {
 public:
  int32_t n_total_ = -1;
  int32_t quorum_ = -1;
  int32_t n_voted_{0};
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
                            quorum_(quorum){
  }

  void set_sites(vector<int> sites){
    sites_ = sites;
  }

  //seems pretty useless, but we can maybe keep it??
  void update_deps(int srcId){
    std::unordered_set<int> tgtIds = {};
    for(auto site: sites_){
      auto index = deps.find(site);
      if(index == deps.end() && site != srcId){
        tgtIds.insert(site);
      }
      auto index2 = deps[site].find(srcId);
      if(index2 != deps[site].end()) deps[site].erase(index2);
    }
    deps[srcId] = tgtIds;
  }


  void add_dep(int srcId, int tgtId){
    auto srcIndex = deps.find(srcId);
    if(srcIndex == deps.end()){
      std::unordered_set<int> temp = {};
      deps[srcId] = temp;
    }
    // commented part is for testing
    /*
    std::ofstream of(log_file, std::fstream::app);
    of << srcId << ", " << tgtId << "\n";
    of.close();
    */
    auto tgtIndex = deps[srcId].find(tgtId);
    if(tgtIndex == deps[srcId].end() && srcId != tgtId){
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
    if (n_voted_ >= quorum_) {
      Log_debug("voted: %d is equal or greater than quorum: %d",
                (int)n_voted_, (int) quorum_);
      return true;
    }
    Log_debug("voted: %d is smaller than quorum: %d",
              (int)n_voted_, (int) quorum_);
    return false;
  }

};

} // namespace janus
