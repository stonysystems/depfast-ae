
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
#include <chrono>

namespace janus {

static int volatile xx =
    MarshallDeputy::RegInitializer(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC,
                                   []() -> Marshallable* {
                                     return new ControlUnit;
                                   });

// One instance per process
ChainRPCCommo::ChainRPCCommo(PollMgr* poll) : Communicator(poll) {
  _preAllocatePathsWithWeights();
  _initializePathResponseTime();
  Log_info("Initialize ChainRPCCommo");
  initializtion_time = std::chrono::high_resolution_clock::now();
  availablePath = 0;
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

void ChainRPCCommo::Statistics() const {
  Log_info("Retransmit RPC counts %d, received quorum_ok: %d, received quorum_fail: %d, ok+fail:%d", 
                retry_rpc_cnt.load(), 
                received_quorum_ok_cnt.load(), 
                received_quorum_fail_cnt.load(), 
                received_quorum_ok_cnt.load() + received_quorum_fail_cnt.load());
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
  verify(pathsWeights[par_id].size() > 0);
  int pathIdx = getNextAvailablePath(par_id);
  vector<int> path = std::get<0>(pathsWeights[par_id][pathIdx]);

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
    Log_track("Leader sends a request, slot_id:%d, ControlUnit: %s", slot_id, cu->toString().c_str());
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
    data_append_map_[cu->uniq_id_] = std::make_tuple(slot_id,
                                        ballot,
                                        currentTerm,
                                        prevLogIndex,
                                        prevLogTerm,
                                        commitIndex,
																				di,
                                        md);
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
  //Log_info("BroadcastAppendEntries\n");
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

// Exponential to # of replicas, it's ok for 3/5 replicas (2 and 24 paths). 
// First half is the normal path, the second half is the reverse path.
// permutations[i+halfSize] is the reverse of permutations[i].
// In path allocations, we always have a forward and backward path in pair to tolerate one-node failure.
std::vector<std::vector<int>> _generatePermutations(int n) {
    std::vector<std::vector<int>> permutations;
    std::vector<int> nums(n);
    
    // Initialize nums as [1, 2, ..., n]
    for (int i = 0; i < n; ++i) {
        nums[i] = i + 1;
    }

    // Generate first half of permutations
    int halfSize = 1;
    for (int i = 2; i <= n; ++i) {
        halfSize *= i;  // Calculate the factorial to get half the total permutations
    }
    halfSize /= 2;  // The first half size of the permutations

    // Use next_permutation to fill the first half
    int count = 0;
    do {
        permutations.push_back(nums);
        count++;
    } while (std::next_permutation(nums.begin(), nums.end()) && count < halfSize);

    // Fill the second half with the reverse of the first half
    for (int i = 0; i < halfSize; ++i) {
        std::vector<int> reversed = permutations[i];
        std::reverse(reversed.begin(), reversed.end());
        permutations.push_back(reversed);
    }

#ifdef SINGLE_PATH_ENABLED
    permutations.resize(1);
#endif

    for (int i = 0; i < permutations.size(); ++i) {
        Log_info("Path %d: %s", i, _arrayToString(permutations[i]).c_str());
    }

    return permutations;
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
        pathsWeights[par_id].push_back(std::make_tuple(path, intial_w));
    }
  }
}

void ChainRPCCommo::_initializePathResponseTime() {
  auto config = Config::GetConfig();
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto& par_id : partitions) {
    for (int i = 0; i < pathsWeights[par_id].size(); i++) {
      pathResponeTime_[par_id].push_back(std::deque<uint64_t>());
    }
  }
}

// Pickup one path based on weights.
// Return: the index of the paths.
int ChainRPCCommo::getNextAvailablePath(int par_id) {
  return availablePath;
}

void ChainRPCCommo::updatePathWeights(int par_id, uint64_t slot_id, int cur_i, uint64_t cur_response_time) {
  if (slot_id % 100 != 0) {
    return;
  }

  vector<double> avg_lat;
  vector<std::deque<uint64_t>> response_times_paths = pathResponeTime_[par_id];
  for (int i=0; i<response_times_paths.size(); i++) {
    double tol = 0, cnt = 0;
    for (int j=0; j<response_times_paths[i].size(); j++) {
      tol += response_times_paths[i][j];
      cnt++;
    }
    if (cnt==0) {
      avg_lat.push_back(1000 * 1000); // If there are no request to this path, go for this path
    }else{
      avg_lat.push_back(tol/cnt);
    }
  }

  std::vector<double> weights(avg_lat.size());
    
  // Step 1: Compute the inverse of each latency (since lower latencies should get higher weights)
  for (size_t i = 0; i < avg_lat.size(); ++i) {
    weights[i] = 1000 * 1000 * 10.0 / avg_lat[i];
  }

  double sum = std::accumulate(weights.begin(), weights.end(), 0.0); // Compute the sum of the inverses

  for (double& weight : weights) { // Normalize
    weight /= sum;
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
  availablePath = std::distance(cumulative.begin(), it);

  Log_track("Weights: %s, avg_lat: %s, availablePath: %d", _arrayToString(weights).c_str(), _arrayToString(avg_lat).c_str(), availablePath);
  /*
  // Update the weight of cur_i-th path in the par_id partition based on response_time. 
  vector<std::deque<uint64_t>> response_times_paths = pathResponeTime_[par_id];
  double tol = 0, cnt = 0;
  for (int i=0; i<response_times_paths.size(); i++) {
    if (i == cur_i) { // We don't want to consider the current path, compare to other paths's response time.
      continue;
    }
    for (int j=0; j<response_times_paths[i].size(); j++) {
      tol += response_times_paths[i][j];
      cnt++;
    }
  }
  if (cnt == 0) {
    return;
  }
  double avg = tol / cnt;
  auto weights = vector<double>();
  for (auto& p : pathsWeights[par_id]) {
    weights.push_back(std::get<1>(p));
  }

  // Update the i-th probability based on the latency comparison
  if (cur_response_time >= avg * 0.7 && cur_response_time <= avg * 1.3) {
    return;
  }

  // max change is 5%, we don't want to update weights significantly.
  double changeRatio = 0.3 * avg/cur_response_time;
  double max_change = 0.05;
  weights[cur_i] *= max(1-max_change, 
                        min(1+max_change, changeRatio)); 
  weights[cur_i] = max(weights[cur_i], 0.01); // min weight is 1%
  weights[cur_i] = min(weights[cur_i], 0.9); // max weight is 90%
  Log_track("Weights: %s", _arrayToString(weights).c_str());
  // Normalize the probability array to ensure the sum is 1
  double total = std::accumulate(weights.begin(), weights.end(), 0.0);
  for (double& p : weights) {
    p /= total;
  }*/

  // Write back the updated weights
  for (int i=0; i<weights.size(); i++) {
    std::get<1>(pathsWeights[par_id][i]) = weights[i];
  }
}

// Keep latest 200 data for each path.
// latency is in ns.
void ChainRPCCommo::appendResponseTime(int par_id, int cur_i, uint64_t latency) {
    if (latency == 0) return;

    int N = 200;
    auto& responseTimesPath = pathResponeTime_[par_id][cur_i];

    // For outliers, it's better not to eliminate it. Otherwise, it's hard to detect slowness.
    /*
    if (responseTimesPath.size() >= N) {
        // Copy response times to a vector for sorting
        std::vector<double> times(responseTimesPath.begin(), responseTimesPath.end());
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
            Log_track("Outlier detected: latency = %f, min = %f, max = %f, lowerBound = %f, upperBound = %f, path: %d", latency, min_value, max_value, lowerBound, upperBound, cur_i);
            return; // Ignore the outlier
        } else {
            responseTimesPath.push_back(latency);
        }
    } else {
      responseTimesPath.push_back(latency);
    } */

    responseTimesPath.push_back(latency);

    // Keep only the last N response times
    if (responseTimesPath.size() > N) {
        responseTimesPath.pop_front();
    }
}

} // namespace janus
