#pragma once

#include <vector>
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
  unordered_map<int, unordered_set<int>> deps{}; //not sure if SiteInfo or int

  QuorumEvent() = delete;

  //add the third parameter here
  //i don't want to mess things up
  QuorumEvent(int n_total,
              int quorum,
              vector<SiteInfo> sites) : Event(),
                                        quorum_(quorum){
  }

  void update_deps(int srcId){
    unordered_set<int> tgtIds{};
    for(auto site: sites_){
      if(!deps.contains(site) && site != srcId){
        tgtIds.insert(site);
      }
      unordered_set<int>::const_iterator index = deps[site].find(source);
      if(index != deps[site].end()) deps[site].erase(source);
    }
    deps[srcId] = tgtIds;
  }


  //update_deps separated into two parts for finer control
  void add_dep(int srcId){
    unordered_set<int> tgtIds();
    for(auto site: sites_){
      if(!deps.contains(site) && site != srcId){
        tgtIds.insert(site);
      }
    }
    deps[srcId] = tgtIds;
  }

  void erase_dep(int srcId){
    unordered_set<int>::const_iterator index = deps[site].find(source);
    if(index != deps[site].end()) deps[site].erase(source);
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
