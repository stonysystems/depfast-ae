#pragma once

#include <vector>
#include <unordered_map>
#include <unordered_set>
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


  //update_deps separated into two parts for finer control
  void add_dep(int srcId){
    std::unordered_set<int> tgtIds = {};
    for(auto site: sites_){
      auto index = deps.find(site);
      if(index == deps.end() && site != srcId){
        tgtIds.insert(site);
      }
    }
    deps[srcId] = tgtIds;
  }

  void erase_dep(int srcId){
    for(auto site: sites_){
      auto index = deps[site].find(srcId);
      if(index != deps[site].end()) deps[site].erase(index);
    }
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
