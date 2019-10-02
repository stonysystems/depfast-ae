#pragma once

#include <vector>
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
  unordered_map<int, unordered_set<int>> deps{}; //not sure if SiteInfo or int

  QuorumEvent() = delete;

  //add the third parameter here
  //i don't want to mess things up
  QuorumEvent(int n_total,
              int quorum) : Event(),
                            n_total_(n_total),
                            quorum_(quorum) {
  }

  void update_deps(int source){
    int srcId = source.get_site_id();
    unordered_set<int> tgtIds{};
    for(auto site: sites_){
      if(!deps.(contains(site.get_site_id())){
        tgtIds.insert(site.get_site_id());
      }
      else{
        unordered_set<SiteInfo>::const_iterator index = deps[site].find(source);
        if(index != deps[site].end()) deps[site].erase(source);
      }
    }
    deps[source] = targets;
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
