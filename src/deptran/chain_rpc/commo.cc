
#include "commo.h"
#include "../rcc/graph.h"
#include "../rcc/graph_marshaler.h"
#include "../command.h"
#include "../procedure.h"
#include "../command_marshaler.h"
#include "../rcc_rpc.h"
#include <vector>
#include <algorithm>
#include <deque>
#include "utils.h"

namespace janus {

static int volatile xx =
    MarshallDeputy::RegInitializer(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC,
                                   []() -> Marshallable* {
                                     return new ControlUnit;
                                   });

// One instance per process
ChainRPCCommo::ChainRPCCommo(PollMgr* poll) : Communicator(poll) {
  _preAllocatePathsWithWeights();
  Log_info("Initialize ChainRPCCommo");
}

shared_ptr<ChainRPCForwardQuorumEvent> ChainRPCCommo::SendForward(parid_t par_id, 
                                            parid_t self_id, shared_ptr<Marshallable> cmd)
{
    int n = Config::GetConfig()->GetPartitionSize(par_id);
    auto e = Reactor::CreateSpEvent<ChainRPCForwardQuorumEvent>(1,1);
    parid_t fid = (self_id + 1 ) % n ;
    if (fid != self_id + 1 )
    {
      // sleep for 2 seconds cos no leader
      int32_t timeout = 2*1000*1000 ;
      auto sp_e = Reactor::CreateSpEvent<TimeoutEvent>(timeout);
      sp_e->Wait();    
    }
    auto proxies = rpc_par_proxies_[par_id];
    WAN_WAIT;
    auto proxy = (ChainRPCProxy*) proxies[fid].second ;
    FutureAttr fuattr;
    fuattr.callback = [e](Future* fu) {
      uint64_t cmt_idx = 0;
      fu->get_reply() >> cmt_idx;
      e->FeedResponse(cmt_idx);
    };    
    MarshallDeputy md(cmd);
    auto f = proxy->async_Forward(md, fuattr);
    Future::safe_release(f);
    return e;
}

void ChainRPCCommo::BroadcastHeartbeat(parid_t par_id,
																			 uint64_t logIndex) {
	//Log_info("heartbeat for log index: %d", logIndex);
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    if (p.first == this->loc_id_)
        continue;
		auto follower_id = p.first;
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    
		fuattr.callback = [this, follower_id, logIndex] (Future* fu) {
      uint64_t index = 0;
			
      fu->get_reply() >> index;
			this->matchedIndex[follower_id] = index;
			
			//Log_info("follower_index for %d: %d and leader_index: %d", follower_id, index, logIndex);
			
    };

		DepId di;
		di.str = "hb";
		di.id = -1;
    auto f = proxy->async_Heartbeat(logIndex, di, fuattr);
    Future::safe_release(f);
  }
}

void ChainRPCCommo::SendHeartbeat(parid_t par_id,
																	siteid_t site_id,
																  uint64_t logIndex) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
	WAN_WAIT;
  for (auto& p : proxies) {
    if (p.first != site_id)
        continue;
		auto follower_id = p.first;
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};
    
		DepId di;
		di.str = "dep";
		di.id = -1;
		
		//Log_info("heartbeat2 for log index: %d", logIndex);
    auto f = proxy->async_Heartbeat(logIndex, di, fuattr);
    Future::safe_release(f);
  }
}

void ChainRPCCommo::SendAppendEntriesAgain(siteid_t site_id,
																					 parid_t par_id,
																					 slotid_t slot_id,
																					 ballot_t ballot,
																					 bool isLeader,
																					 uint64_t currentTerm,
																					 uint64_t prevLogIndex,
																					 uint64_t prevLogTerm,
																					 uint64_t commitIndex,
																					 shared_ptr<Marshallable> cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
	WAN_WAIT;
  for (auto& p : proxies) {
    if (p.first != site_id)
        continue;
		auto follower_id = p.first;
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};

		MarshallDeputy md(cmd);
		verify(md.sp_data_ != nullptr);

		DepId di;
		di.str = "dep";
		di.id = -1;

		Log_info("heartbeat2 for log index: %d", prevLogIndex);
    auto f = proxy->async_AppendEntries(slot_id,
                                        ballot,
                                        currentTerm,
                                        prevLogIndex,
                                        prevLogTerm,
                                        commitIndex,
																				di,
                                        md, 
                                        fuattr);
    Future::safe_release(f);
  }

}

#ifdef CHAIN_RPC_ENABLED
shared_ptr<ChainRPCAppendQuorumEvent>
ChainRPCCommo::BroadcastAppendEntries(parid_t par_id,
                                      siteid_t leader_site_id,
                                      slotid_t slot_id,
                                      i64 dep_id,
                                      ballot_t ballot,
                                      bool isLeader,
                                      uint64_t currentTerm,
                                      uint64_t prevLogIndex,
                                      uint64_t prevLogTerm,
                                      uint64_t commitIndex,
                                      shared_ptr<Marshallable> cmd) {
  verify(pathsW[par_id].size() > 0);
  int pathIdx = getNextAvailablePath(par_id);
  vector<int> path = std::get<0>(pathsW[par_id][pathIdx]);

  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<ChainRPCAppendQuorumEvent>(n, n/2 + 1);
  auto proxies = rpc_par_proxies_[par_id];

  WAN_WAIT;
  uniq_id_++;

  // Forward the request to the next hop in the path.
  {
    auto cu = make_shared<ControlUnit>();
    cu->SetUniqueID(uniq_id_);
    cu->total_partitions_ = n;
    cu->acc_ack_ = 1; // The first ack is from the leader
    cu->SetPath(pathIdx, path);
    cu->AppendResponseForAppendEntries(0, 1, currentTerm, prevLogIndex + 1);

    auto cu_m = dynamic_pointer_cast<Marshallable>(cu);
    int nextHop = cu->Increment2NextHop();
    Log_track("slot_id:%d, ControlUnit: %s", slot_id, cu->toString().c_str());
    auto &p = proxies[nextHop];
    auto follower_id = p.first;
    auto proxy = (ChainRPCProxy*) p.second;
    auto cli_it = rpc_clients_.find(follower_id);
    std::string ip = "";
    if (cli_it != rpc_clients_.end()) {
      ip = cli_it->second->host();
    }

    FutureAttr fuattr;
    struct timespec begin;
    clock_gettime(CLOCK_MONOTONIC, &begin);

    fuattr.callback = [this, e, isLeader, currentTerm, follower_id, n, ip, begin] (Future* fu) {
      uint64_t accept = 0;
      uint64_t term = 0;
      uint64_t index = 0;
			
			fu->get_reply() >> accept;
      fu->get_reply() >> term;
      fu->get_reply() >> index;
			
			struct timespec end;
			//clock_gettime(CLOCK_MONOTONIC, &begin);
			this->outbound--;
			//Log_info("reply from server: %s and is_ready: %d", ip.c_str(), e->IsReady());
			clock_gettime(CLOCK_MONOTONIC, &end);
			//Log_info("time of reply on server %d: %ld", follower_id, (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec);
      bool y = ((accept == 1) && (isLeader) && (currentTerm == term));
      //e->FeedResponse(y, index, ip);
    };
    MarshallDeputy md(cmd);
		verify(md.sp_data_ != nullptr);
		outbound++;
		DepId di;
		di.str = "dep";
		di.id = dep_id;

    MarshallDeputy cu_cmd(cu_m);

    auto f = proxy->async_AppendEntriesChain(slot_id,
                                        ballot,
                                        currentTerm,
                                        prevLogIndex,
                                        prevLogTerm,
                                        commitIndex,
																				di,
                                        md,
                                        cu_cmd,
                                        fuattr);
    Future::safe_release(f);
    event_append_map_[cu->uniq_id_] = e;
  
    verify(!e->IsReady());

    e->ongoingPickedPath = pathIdx;
    e->uniq_id_ = cu->uniq_id_;
  }
  return e;
}
#else
shared_ptr<ChainRPCAppendQuorumEvent>
ChainRPCCommo::BroadcastAppendEntries(parid_t par_id,
                                      siteid_t leader_site_id,
                                      slotid_t slot_id,
                                      i64 dep_id,
                                      ballot_t ballot,
                                      bool isLeader,
                                      uint64_t currentTerm,
                                      uint64_t prevLogIndex,
                                      uint64_t prevLogTerm,
                                      uint64_t commitIndex,
                                      shared_ptr<Marshallable> cmd) {
  Log_info("BroadcastAppendEntries\n");
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<ChainRPCAppendQuorumEvent>(n, n/2 + 1);
  auto proxies = rpc_par_proxies_[par_id];

  WAN_WAIT;

  for (auto& p : proxies) {
    auto follower_id = p.first;
    auto proxy = (ChainRPCProxy*) p.second;
    auto cli_it = rpc_clients_.find(follower_id);
    std::string ip = "";
    if (cli_it != rpc_clients_.end()) {
      ip = cli_it->second->host();
    }
	  if (p.first == leader_site_id) {
        // fix the 1c1s1p bug
        // Log_info("leader_site_id %d", leader_site_id);
        e->FeedResponse(true, prevLogIndex + 1, ip);
        continue;
    }
    FutureAttr fuattr;
    struct timespec begin;
    clock_gettime(CLOCK_MONOTONIC, &begin);

    fuattr.callback = [this, e, isLeader, currentTerm, follower_id, n, ip, begin] (Future* fu) {
      uint64_t accept = 0;
      uint64_t term = 0;
      uint64_t index = 0;
			
			fu->get_reply() >> accept;
      fu->get_reply() >> term;
      fu->get_reply() >> index;
			
			struct timespec end;
			//clock_gettime(CLOCK_MONOTONIC, &begin);
			this->outbound--;
			//Log_info("reply from server: %s and is_ready: %d", ip.c_str(), e->IsReady());
			clock_gettime(CLOCK_MONOTONIC, &end);
			//Log_info("time of reply on server %d: %ld", follower_id, (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec);
			
      bool y = ((accept == 1) && (isLeader) && (currentTerm == term));
      e->FeedResponse(y, index, ip);
    };
    MarshallDeputy md(cmd);
		verify(md.sp_data_ != nullptr);
		outbound++;
		DepId di;
		di.str = "dep";
		di.id = dep_id;
    auto f = proxy->async_AppendEntries(slot_id,
                                        ballot,
                                        currentTerm,
                                        prevLogIndex,
                                        prevLogTerm,
                                        commitIndex,
																				di,
                                        md, 
                                        fuattr);
    Future::safe_release(f);
  } // END of for loop
  verify(!e->IsReady());
  return e;
}
#endif


void ChainRPCCommo::BroadcastAppendEntries(parid_t par_id,
                                           slotid_t slot_id,
																					 i64 dep_id,
                                           ballot_t ballot,
                                           uint64_t currentTerm,
                                           uint64_t prevLogIndex,
                                           uint64_t prevLogTerm,
                                           uint64_t commitIndex,
                                           shared_ptr<Marshallable> cmd,
                                           const function<void(Future*)>& cb) {
  verify(0); // deprecated function
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    MarshallDeputy md(cmd);
		DepId di;
		di.str = "dep";
		di.id = dep_id;
    auto f = proxy->async_AppendEntries(slot_id, 
                                        ballot, 
                                        currentTerm,
                                        prevLogIndex,
                                        prevLogTerm,
                                        commitIndex,
																				di,
                                        md, 
                                        fuattr);
    Future::safe_release(f);
  }
//  verify(0);
}

void ChainRPCCommo::BroadcastDecide(const parid_t par_id,
                                      const slotid_t slot_id,
																			const i64 dep_id,
                                      const ballot_t ballot,
                                      const shared_ptr<Marshallable> cmd) {
  auto proxies = rpc_par_proxies_[par_id];
  vector<Future*> fus;
  for (auto& p : proxies) {
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [](Future* fu) {};
    MarshallDeputy md(cmd);
		DepId di;
		di.str = "dep";
		di.id = dep_id;
    auto f = proxy->async_Decide(slot_id, ballot, di, md, fuattr);
    Future::safe_release(f);
  }
}

void ChainRPCCommo::BroadcastVote(parid_t par_id,
                                        slotid_t lst_log_idx,
                                        ballot_t lst_log_term,
                                        parid_t self_id,
                                        ballot_t cur_term,
                                       const function<void(Future*)>& cb) {
  verify(0); // deprecated function
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Vote(lst_log_idx, lst_log_term, self_id,cur_term, fuattr));
  }
}

shared_ptr<ChainRPCVoteQuorumEvent>
ChainRPCCommo::BroadcastVote(parid_t par_id,
                                    slotid_t lst_log_idx,
                                    ballot_t lst_log_term,
                                    parid_t self_id,
                                    ballot_t cur_term ) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<ChainRPCVoteQuorumEvent>(n, n/2);
  auto proxies = rpc_par_proxies_[par_id];
  WAN_WAIT;
  for (auto& p : proxies) {
    if (p.first == this->loc_id_)
        continue;
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [e](Future* fu) {
      ballot_t term = 0;
      bool_t vote = false ;
      fu->get_reply() >> term;
      fu->get_reply() >> vote ;
      e->FeedResponse(vote, term);
      // TODO add max accepted value.
    };
    Future::safe_release(proxy->async_Vote(lst_log_idx, lst_log_term, self_id, cur_term, fuattr));
  }
  return e;
}

void ChainRPCCommo::BroadcastVote2FPGA(parid_t par_id,
                                        slotid_t lst_log_idx,
                                        ballot_t lst_log_term,
                                        parid_t self_id,
                                        ballot_t cur_term,
                                       const function<void(Future*)>& cb) {
  verify(0); // deprecated function
  auto proxies = rpc_par_proxies_[par_id];
  for (auto& p : proxies) {
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = cb;
    Future::safe_release(proxy->async_Vote(lst_log_idx, lst_log_term, self_id,cur_term, fuattr));
  }
}

shared_ptr<ChainRPCVote2FPGAQuorumEvent>
ChainRPCCommo::BroadcastVote2FPGA(parid_t par_id,
                                    slotid_t lst_log_idx,
                                    ballot_t lst_log_term,
                                    parid_t self_id,
                                    ballot_t cur_term ) {
  int n = Config::GetConfig()->GetPartitionSize(par_id);
  auto e = Reactor::CreateSpEvent<ChainRPCVote2FPGAQuorumEvent>(n, n/2);
  auto proxies = rpc_par_proxies_[par_id];
  WAN_WAIT;
  for (auto& p : proxies) {
    if (p.first == this->loc_id_)
        continue;
    auto proxy = (ChainRPCProxy*) p.second;
    FutureAttr fuattr;
    fuattr.callback = [e](Future* fu) {
      ballot_t term = 0;
      bool_t vote = false ;
      fu->get_reply() >> term;
      fu->get_reply() >> vote ;
      e->FeedResponse(vote, term);
    };
    Future::safe_release(proxy->async_Vote(lst_log_idx, lst_log_term, self_id, cur_term, fuattr));
  }
  return e;
}

// Exponential to # of replicas, it's ok for 3/5 replicas (2 and 16 paths). 
std::vector<std::vector<int>> _generatePermutations(int n) {
    std::vector<std::vector<int>> permutations;
    std::vector<int> nums(n);
    for(int i = 0; i < n; ++i) nums[i] = i + 1;
    do {
        permutations.push_back(nums);
    } while(std::next_permutation(nums.begin(), nums.end()));
    return permutations;
}

template <typename E>
std::string _arrayToString(const std::vector<E>& data) {
    std::ostringstream oss;
    oss << "[";
    for (size_t i = 0; i < data.size(); ++i) {
        oss << data[i];
        if (i != data.size() - 1) {
            oss << ", "; // Separate with a comma, but not after the last element
        }
    }
    oss << "]";
    return oss.str();
}

// Allocate the paths with equal weights
// site_id is a global id for both servers and clients.
// locale_id is a id within a replica group, we use locale_id to identify the path.
void ChainRPCCommo::_preAllocatePathsWithWeights() {
  auto config = Config::GetConfig();
  vector<parid_t> partitions = config->GetAllPartitionIds();
	Log_info("size of partitions: %d", partitions.size());
  for (auto& par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    int n = site_infos.size();

    auto permutations = _generatePermutations(n-1);
    double intial_w = 1.0 / permutations.size();

    for(const auto& perm : permutations) {
        vector<int> path;
        // The first or last node is always leader, and start from or end with the leader.
        path.push_back(0);
        for(int num : perm) {
            path.push_back(num);
        }
        path.push_back(0);
        pathsW[par_id].push_back(std::make_tuple(path, intial_w));
    }
  }
}

// Pickup one path based on weights.
// Return: the index of the paths.
int ChainRPCCommo::getNextAvailablePath(int par_id) {
  auto paths = pathsW[par_id];

  vector<double> weights;
  for (auto& p : paths) {
    weights.push_back(std::get<1>(p));
  }

  std::vector<double> cumulative;
  cumulative.reserve(weights.size());
  std::partial_sum(weights.begin(), weights.end(), std::back_inserter(cumulative));

  // Generate a random number between 0 and 1
  std::random_device rd;  // Seed for the random number engine
  std::mt19937 gen(rd()); // Standard mersenne_twister_engine seeded with rd()
  std::uniform_real_distribution<> dis(0.0, 1.0);
  double rand_num = dis(gen);

  // Find the index corresponding to the random number
  auto it = std::lower_bound(cumulative.begin(), cumulative.end(), rand_num);
  return std::distance(cumulative.begin(), it);
}

// Update the weight of cur_i-th path in the par_id partition based on response_time. 
void ChainRPCCommo::updatePathWeights(int par_id, int cur_i, double cur_response_time) {
  std::deque<double> response_times = pathResponeTime_[par_id];
  double tol = cur_response_time, cnt = 1;
  for (int i=0; i<response_times.size(); i++) {
    tol += response_times[i];
    cnt++;
  }
  double avg = tol / cnt;
  auto weights = vector<double>();
  for (auto& p : pathsW[par_id]) {
    weights.push_back(std::get<1>(p));
  }

  // Update the i-th probability based on the latency comparison
  double prevW = weights[cur_i];

  double changeRatio = avg/cur_response_time;
  double max_change = 0.05; // max change is 5%, we don't want to update weights significantly.
  weights[cur_i] *= max(1-max_change, 
                        min(1+max_change, changeRatio)); 
  weights[cur_i] = max(weights[cur_i], 0.01); // min weight is 1%
  weights[cur_i] = min(weights[cur_i], 0.9); // max weight is 90%
  Log_track("Update weight from %f to %f, avg: %f, cur_response_time: %f, Weights: %s (not normalized), path_id: %d, len of pathResponeTime_: %d", prevW, weights[cur_i], avg, cur_response_time, _arrayToString(weights).c_str(), cur_i, pathResponeTime_[par_id].size());
  // Normalize the probability array to ensure the sum is 1
  double total = std::accumulate(weights.begin(), weights.end(), 0.0);
  for (double& p : weights) {
    p /= total;
  }

  // Write back the updated weights
  for (int i=0; i<weights.size(); i++) {
    std::get<1>(pathsW[par_id][i]) = weights[i];
  }
}

// Keep latest 200 data to update the response time of all Paths.
// In this implementation, I eliminate outliers.
void ChainRPCCommo::updateResponseTime(int par_id, double latency) {
    int N = 200;
    auto& responseTimes = pathResponeTime_[par_id];

    // Copy response times to a vector for sorting
    if (responseTimes.size() >= N) {
        // Copy response times to a vector for sorting
        std::vector<double> times(responseTimes.begin(), responseTimes.end());
        auto min_it = std::min_element(times.begin(), times.end());
        auto max_it = std::max_element(times.begin(), times.end());

        // Dereference the iterators to get the values
        double min_value = *min_it;
        double max_value = *max_it;

        // Define the acceptable range
        double lowerBound = 0.5 * min_value;
        double upperBound = 1.5 * max_value ;

        // If the latency is an outlier, do not add it
        if (latency < lowerBound || latency > upperBound) {
            Log_info("Outlier detected: latency = %f, min = %f, max = %f, lowerBound = %f, upperBound = %f", latency, min_value, max_value, lowerBound, upperBound);
            return; // Ignore the outlier
        } else {
            responseTimes.push_back(latency);
        }
    } else {
      responseTimes.push_back(latency);
    }

    // Keep only the last N response times
    if (responseTimes.size() > N) {
        responseTimes.pop_front();
    }
}

} // namespace janus
