#pragma once

#include <vector>
#include <unordered_map>
#include <boost/container_hash/hash.hpp>
//#include <unordered_set>
//#include <fstream>
#include <iostream>
#include "event.h"
#include <chrono>

template <typename Container> // we can make this generic for any container [1]
struct container_hash {
    std::size_t operator()(Container const& c) const {
        return boost::hash_range(c.begin(), c.end());
    }
};

using rrr::Event;
using rrr::IntEvent;
using std::vector;
using std::unordered_map;
using std::unordered_set;

typedef std::unordered_map<int, unordered_map<int, unordered_map<int, unordered_set<int>>>> dependencies_t;
typedef std::unordered_map<unordered_set<std::string>, unordered_map<std::string, int>, container_hash<unordered_set<std::string>>> history_t;
typedef std::unordered_map<unordered_set<std::string>, int, container_hash<unordered_set<std::string>>> count_t;
typedef std::unordered_map<unordered_set<std::string>, unordered_map<std::string, std::vector<long>>, container_hash<std::unordered_set<std::string>>> latency_t;

#define logging 0

namespace janus {

class QuorumEvent : public Event {
 public:
	static uint64_t count;
  int32_t n_voted_yes_{0};
  int32_t n_voted_no_{0};
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
  vector<int> sites_{};
	unordered_set<std::string> ips_{};
	unordered_set<std::string> changing_ips_{};
	//std::vector<rrr::Client> clients_{};
  dependencies_t deps{};
	struct timespec begin;
	static history_t history;
	static count_t counts;
	static latency_t latencies;
	enum TimeoutFlag {FLAG_FREE};
  std::string log_file = "logs.txt";

	std::shared_ptr<IntEvent> finalize_event;

  QuorumEvent() = delete;

  QuorumEvent(int n_total,
              int quorum,
							int dep_id = -1) : Event(),
																 n_total_(n_total),
																 quorum_(quorum){
		if (quorum_ != n_total_) {
			needs_finalize_ = true;
		}
		finalize_event = std::make_shared<IntEvent>();
		finalize_event->__debug_creator = 1;

		finalize_event->target_ = n_total_;
  }

	void recordHistory(unordered_set<std::string> ip_addrs);
	void updateDataStructs(std::string ip_addrs);
	void verifyTransient(std::string ip_addr);
	void updateHistory(std::string ip_addr);
	void Finalize(int timeout, int flag);
  void set_sites(vector<int> sites){
    sites_ = sites;
  }


  void add_dep(int srcId, int srcCoro, int tgtId, int tgtCoro){
    auto srcIndex = deps.find(srcId);
    if(srcIndex == deps.end()){
      unordered_map<int, unordered_map<int, unordered_set<int>>> temp = {};
      deps[srcId] = temp;
    }

    auto srcCoroIndex = deps[srcId].find(srcCoro);
    if(srcCoroIndex == deps[srcId].end()){
      unordered_map<int, unordered_set<int>> temp = {};
      deps[srcId][srcCoro] = temp; 
    }
    // commented part is for testing
    /*
    std::ofstream of(log_file, std::fstream::app);
    of << srcId << ", " << tgtId << "\n";
    of.close();
    */
    auto tgtIndex = deps[srcId][srcCoro].find(tgtId);
    if(tgtIndex == deps[srcId][srcCoro].end()){
      unordered_set<int> temp = {};
      deps[srcId][srcCoro][tgtId] = temp;
    }

    auto tgtCoroIndex = deps[srcId][srcCoro][tgtId].find(tgtCoro);
    if(tgtCoroIndex == deps[srcId][srcCoro][tgtId].end()){
      deps[srcId][srcCoro][tgtId].insert(tgtCoro);
    }
  }

  /*void remove_dep(int srcId, int tgtId){
    auto srcIndex = deps.find(srcId);
    if(srcIndex != deps.end()){
      auto tgtIndex = deps[srcId].find(tgtId);
      if(tgtIndex != deps[srcId].end()){
        deps[srcId].erase(tgtId);
      }
    }
  }*/

  void log() override {
    if(logging){
      std::ofstream of(log_file, std::fstream::app);
      //of << "hello\n";
      //if(coro_id_ == -1) return;
    
      // Maybe this part can be more efficient
      for(auto it = deps.begin(); it != deps.end(); it++){
        for(auto it2 = it->second.begin(); it2 != it->second.end(); it2++){
          for(auto it3 = it2->second.begin(); it3 != it2->second.end(); it3++){
            for(auto it4 = it3->second.begin(); it4 != it3->second.end(); it4++){
              of << "{ " << it->first << ", " << it2->first << ", " << it3->first << ", " << *it4 << ", " << id_ << ": " << quorum_ << "/" << n_total_ << " }\n";
            }
          }
        }
      }
      of.close();
    }
  }

  virtual bool Yes() {
    return n_voted_yes_ >= quorum_;
  }

  virtual bool No() {
    verify(n_total_ >= quorum_);
    return n_voted_no_ > (n_total_ - quorum_);
  }

  void VoteYes(std::string ip_addr = "") {
		updateHistory(ip_addr);
    n_voted_yes_++;
    Test();
		if (finalize_event->status_ != TIMEOUT) {
			auto it = changing_ips_.find(ip_addr);
			changing_ips_.erase(it);
			finalize_event->Set(n_voted_yes_ + n_voted_no_);
		}
  }

  void VoteNo(std::string ip_addr = "") {
    n_voted_no_++;
    Test();
		if (finalize_event->status_ != TIMEOUT) {
			auto it = changing_ips_.find(ip_addr);
			changing_ips_.erase(it);
			finalize_event->Set(n_voted_yes_ + n_voted_no_);
		}
  }

  int64_t Term() {
    return highest_term_ ;
  }

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

};

} // namespace janus
