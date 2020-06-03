
#include "communicator.h"
#include "coordinator.h"
#include "classic/coordinator.h"
#include "rcc/graph.h"
#include "rcc/graph_marshaler.h"
#include "command.h"
#include "command_marshaler.h"
#include "procedure.h"
#include "rcc_rpc.h"
#include <typeinfo>

namespace janus {

uint64_t Communicator::global_id = 0;

Communicator::Communicator(PollMgr* poll_mgr) {
  vector<string> addrs;
  if (poll_mgr == nullptr)
    rpc_poll_ = new PollMgr(1);
  else
    rpc_poll_ = poll_mgr;
  auto config = Config::GetConfig();
  vector<parid_t> partitions = config->GetAllPartitionIds();
  for (auto& par_id : partitions) {
    auto site_infos = config->SitesByPartitionId(par_id);
    vector<std::pair<siteid_t, ClassicProxy*>> proxies;
    for (auto& si : site_infos) {
      auto result = ConnectToSite(si, std::chrono::milliseconds
          (CONNECT_TIMEOUT_MS));
      verify(result.first == SUCCESS);
      proxies.push_back(std::make_pair(si.id, result.second));
    }
    rpc_par_proxies_.insert(std::make_pair(par_id, proxies));
  }
  client_leaders_connected_.store(false);
  if (config->forwarding_enabled_) {
    threads.push_back(std::thread(&Communicator::ConnectClientLeaders, this));
  } else {
    client_leaders_connected_.store(true);
  }
}

void Communicator::ConnectClientLeaders() {
  auto config = Config::GetConfig();
  if (config->forwarding_enabled_) {
    Log_info("%s: connect to client sites", __FUNCTION__);
    auto client_leaders = config->SitesByLocaleId(0, Config::CLIENT);
    for (Config::SiteInfo leader_site_info : client_leaders) {
      verify(leader_site_info.locale_id == 0);
      Log_info("client @ leader %d", leader_site_info.id);
      auto result = ConnectToClientSite(leader_site_info,
                                        std::chrono::milliseconds
                                            (CONNECT_TIMEOUT_MS));
      verify(result.first == SUCCESS);
      verify(result.second != nullptr);
      Log_info("connected to client leader site: %d, %d, %p",
               leader_site_info.id,
               leader_site_info.locale_id,
               result.second);
      client_leaders_.push_back(std::make_pair(leader_site_info.id,
                                               result.second));
    }
  }
  client_leaders_connected_.store(true);
}

void Communicator::WaitConnectClientLeaders() {
  bool connected;
  do {
    connected = client_leaders_connected_.load();
  } while (!connected);
  Log_info("Done waiting to connect to client leaders.");
}

void Communicator::ResetProfiles(){
	index = 0;
	total = 0;
	for(int i = 0; i < 100; i++){
		window[i] = 0;
	}
	window_time = 0;
	total_time = 0;
	window_avg = 0;
	total_avg = 0;
}
Communicator::~Communicator() {
  verify(rpc_clients_.size() > 0);
  for (auto& pair : rpc_clients_) {
    auto rpc_cli = pair.second;
    rpc_cli->close_and_release();
  }
  rpc_clients_.clear();
}

std::pair<siteid_t, ClassicProxy*>
Communicator::RandomProxyForPartition(parid_t par_id) const {
  auto it = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& par_proxies = it->second;
  int index = rrr::RandomGenerator::rand(0, par_proxies.size() - 1);
  return par_proxies[index];
}

std::pair<siteid_t, ClassicProxy*>
Communicator::LeaderProxyForPartition(parid_t par_id) const {
  auto leader_cache =
      const_cast<map<parid_t, SiteProxyPair>&>(this->leader_cache_);
  auto leader_it = leader_cache.find(par_id);
  if (leader_it != leader_cache.end()) {
    return leader_it->second;
  } else {
    auto it = rpc_par_proxies_.find(par_id);
    verify(it != rpc_par_proxies_.end());
    auto& partition_proxies = it->second;
    auto config = Config::GetConfig();
    auto proxy_it = std::find_if(
        partition_proxies.begin(),
        partition_proxies.end(),
        [config](const std::pair<siteid_t, ClassicProxy*>& p) {
          verify(p.second != nullptr);
          auto& site = config->SiteById(p.first);
          return site.locale_id == 0;
        });
    if (proxy_it == partition_proxies.end()) {
      Log_fatal("could not find leader for partition %d", par_id);
    } else {
      leader_cache[par_id] = *proxy_it;
      Log_debug("leader site for parition %d is %d", par_id, proxy_it->first);
    }
    verify(proxy_it->second != nullptr);
    return *proxy_it;
  }
}

ClientSiteProxyPair
Communicator::ConnectToClientSite(Config::SiteInfo& site,
                                  std::chrono::milliseconds timeout) {
  auto config = Config::GetConfig();
  char addr[1024];
  snprintf(addr, sizeof(addr), "%s:%d", site.host.c_str(), site.port);

  auto start = std::chrono::steady_clock::now();
  rrr::Client* rpc_cli = new rrr::Client(rpc_poll_);
	rpc_cli->client_ = true;
  double elapsed;
  int attempt = 0;
  do {
    Log_info("connect to client site: %s (attempt %d)", addr, attempt++);
    auto connect_result = rpc_cli->connect(addr, true);
    if (connect_result == SUCCESS) {
      ClientControlProxy* rpc_proxy = new ClientControlProxy(rpc_cli);
      rpc_clients_.insert(std::make_pair(site.id, rpc_cli));
      Log_debug("connect to client site: %s success!", addr);
      return std::make_pair(SUCCESS, rpc_proxy);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP_MS));
    }
    auto end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
  } while (elapsed < timeout.count());
  Log_info("timeout connecting to client %s", addr);
  rpc_cli->close_and_release();
  return std::make_pair(FAILURE, nullptr);
}

std::pair<int, ClassicProxy*>
Communicator::ConnectToSite(Config::SiteInfo& site,
                            std::chrono::milliseconds timeout) {
  string addr = site.GetHostAddr();
  auto start = std::chrono::steady_clock::now();
  auto rpc_cli = std::make_shared<rrr::Client>(rpc_poll_);
  double elapsed;
  int attempt = 0;
  do {
    Log_debug("connect to site: %s (attempt %d)", addr.c_str(), attempt++);
    auto connect_result = rpc_cli->connect(addr.c_str());
    if (connect_result == SUCCESS) {
      ClassicProxy* rpc_proxy = new ClassicProxy(rpc_cli.get());
      rpc_clients_.insert(std::make_pair(site.id, rpc_cli));
      rpc_proxies_.insert(std::make_pair(site.id, rpc_proxy));
      Log_debug("connect to site: %s success!", addr.c_str());
      return std::make_pair(SUCCESS, rpc_proxy);
    } else {
      std::this_thread::sleep_for(std::chrono::milliseconds(CONNECT_SLEEP_MS));
    }
    auto end = std::chrono::steady_clock::now();
    elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(end - start)
        .count();
  } while (elapsed < timeout.count());
  Log_info("timeout connecting to %s", addr.c_str());
  rpc_cli->close_and_release();
  return std::make_pair(FAILURE, nullptr);
}

std::pair<siteid_t, ClassicProxy*>
Communicator::NearestProxyForPartition(parid_t par_id) const {
  // TODO Fix me.
  auto it = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  verify(partition_proxies.size() > loc_id_);
  int index = loc_id_;
  return partition_proxies[index];
};

std::shared_ptr<QuorumEvent> Communicator::SendReelect(){
	int total = rpc_par_proxies_[0].size() - 1;
  std::shared_ptr<QuorumEvent> e = Reactor::CreateSpEvent<QuorumEvent>(total, 1);

	for(auto& pair: rpc_par_proxies_[0]){
		rrr::FutureAttr fuattr;
		int id = pair.first;
		if(id == 0) continue;
		fuattr.callback = 
			[e, this, id] (Future* fu) {
				bool_t success = false;
				fu->get_reply() >> success;
				
				if(success){
					for(int i = 0; i < 100; i++) Log_info("success success");
					e->VoteYes();
					this->SetNewLeaderProxy(0, id);
					e->Test();
				}
			};
		auto f = pair.second->async_ReElect(fuattr);
		Future::safe_release(f);
	}
	return e;

}
void Communicator::BroadcastDispatch(
    shared_ptr<vector<shared_ptr<TxPieceData>>> sp_vec_piece,
    Coordinator* coo,
    const function<void(int, TxnOutput&)> & callback) {
  cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
  verify(!sp_vec_piece->empty());
  auto par_id = sp_vec_piece->at(0)->PartitionId();
  rrr::FutureAttr fuattr;
  fuattr.callback =
      [coo, this, callback](Future* fu) {
        int32_t ret;
        TxnOutput outputs;
        fu->get_reply() >> ret >> outputs;
        callback(ret, outputs);
      };
  auto pair_leader_proxy = LeaderProxyForPartition(par_id);
  SetLeaderCache(par_id, pair_leader_proxy) ;
  Log_debug("send dispatch to site %ld",
            pair_leader_proxy.first);
  auto proxy = pair_leader_proxy.second;
  shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
  sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
  MarshallDeputy md(sp_vpd); // ????

  auto future = proxy->async_Dispatch(cmd_id, md, fuattr);
  Future::safe_release(future);
  if (!broadcasting_to_leaders_only_) {
    for (auto& pair : rpc_par_proxies_[par_id]) {
      if (pair.first != pair_leader_proxy.first) {
        rrr::FutureAttr fu2;
        fu2.callback =
            [coo, this, callback](Future* fu) {
              int32_t ret;
              TxnOutput outputs;
              fu->get_reply() >> ret >> outputs;
              // do nothing
            };
        Future::safe_release(pair.second->async_Dispatch(cmd_id, md, fu2));
      }
    }
  }
}

//need to change this code to solve the quorum info in the graphs
//either create another event here or inside the coordinator.
std::shared_ptr<QuorumEvent> Communicator::BroadcastDispatch(
    ReadyPiecesData cmds_by_par,
    Coordinator* coo,
    TxData* txn) {
  int total = cmds_by_par.size();
  //std::shared_ptr<AndEvent> e = Reactor::CreateSpEvent<AndEvent>();
  std::shared_ptr<QuorumEvent> e = Reactor::CreateSpEvent<QuorumEvent>(total, total);
  std::unordered_set<int> leaders{};
  auto src_coroid = e->GetCoroId();
  coo->coro_id_ = src_coroid;
  //Log_info("The size of cmds_by_par is %d", cmds_by_par.size());

  for(auto& pair: cmds_by_par){
    bool first = false;
    auto& cmds = pair.second;
    auto sp_vec_piece = std::make_shared<vector<shared_ptr<TxPieceData>>>();
    for(auto c: cmds){
      c->id_ = coo->next_pie_id();
      coo->dispatch_acks_[c->inn_id_] = false;
      sp_vec_piece->push_back(c);
    }
    cmdid_t cmd_id = sp_vec_piece->at(0)->root_id_;
    verify(sp_vec_piece->size() > 0);
    auto par_id = sp_vec_piece->at(0)->PartitionId();
    auto pair_leader_proxy = LeaderProxyForPartition(par_id);
    auto leader_id = pair_leader_proxy.first;

    phase_t phase = coo->phase_;
    rrr::FutureAttr fuattr;
    fuattr.callback =
        [e, coo, this, phase, txn, src_coroid, leader_id](Future* fu) {
          int32_t ret;
          TxnOutput outputs;
          uint64_t coro_id = 0;
	  			double cpu = 0.0;
	  			double net = 0.0;
          fu->get_reply() >> ret >> outputs >> coro_id;

          e->n_voted_yes_++;
          if(phase != coo->phase_){
	    			e->Test();
	  			}
          else{
            if(ret == REJECT){
              coo->aborted_ = true;
              txn->commit_.store(false);
            }
            coo->n_dispatch_ack_ += outputs.size();
            for(auto& pair: outputs){
              const uint32_t& inn_id = pair.first;
              coo->dispatch_acks_[inn_id] = true;
              txn->Merge(pair.first, pair.second);
            }
	  
	    			CoordinatorClassic* classic_coo = (CoordinatorClassic*)coo;
	    			//classic_coo->debug_cnt--;
            if(txn->HasMoreUnsentPiece()){
              classic_coo->DispatchAsync(false);
            }
              //e->add_dep(coo->cli_id_, src_coroid, leader_id, coro_id);
            coo->ids_.push_back(leader_id);
            e->Test();
	  			}
      };
    
		Log_info("the parid is: %d", par_id);
    Log_debug("send dispatch to site %ld",
              pair_leader_proxy.first);
    auto proxy = pair_leader_proxy.second;
    shared_ptr<VecPieceData> sp_vpd(new VecPieceData);
    sp_vpd->sp_vec_piece_data_ = sp_vec_piece;
    MarshallDeputy md(sp_vpd); // ????
    CoordinatorClassic* classic_coo = (CoordinatorClassic*) coo;
    //classic_coo->debug_cnt++;
    //Log_info("debug_cnt: %d", classic_coo->debug_cnt);

    struct timespec start_;
    clock_gettime(CLOCK_REALTIME, &start_);

    outbound_[src_coroid] = make_pair((rrr::i64)start_.tv_sec, (rrr::i64)start_.tv_nsec);

    auto future = proxy->async_Dispatch(cmd_id, md, fuattr);
    Future::safe_release(future);
    if(!broadcasting_to_leaders_only_){
      for (auto& pair : rpc_par_proxies_[par_id]) {
        if (pair.first != pair_leader_proxy.first) {
          //if(first) curr->n_total_++;
          auto follower_id = pair.first;
          rrr::FutureAttr fuattr;
          fuattr.callback =
              [e, coo, this, src_coroid, follower_id](Future* fu) {
                int32_t ret;
                TxnOutput outputs;
                uint64_t coro_id = 0;
                fu->get_reply() >> ret >> outputs >> coro_id;
                //e->add_dep(coo->cli_id_, src_coroid, follower_id, coro_id);
                //coo->ids_.push_back(follower_id);
                // do nothing
              };
          Future::safe_release(pair.second->async_Dispatch(cmd_id, md, fuattr));
        }
      }
    }
  }
  //probably should modify the data structure here.
  return e;
}


void Communicator::SendStart(SimpleCommand& cmd,
                             int32_t output_size,
                             std::function<void(Future* fu)>& callback) {
  verify(0);
}

shared_ptr<AndEvent>
Communicator::SendPrepare(Coordinator* coo,
                          txnid_t tid,
                          std::vector<int32_t>& sids){
  int32_t res_ = 10;
  TxData* cmd = (TxData*) coo->cmd_;
  auto n = cmd->partition_ids_.size();
  auto e = Reactor::CreateSpEvent<AndEvent>();
  auto phase = coo->phase_;
  int n_total = 1;
  int quorum_id = 0;
  for(auto& partition_id : cmd->partition_ids_){
    auto leader_id = LeaderProxyForPartition(partition_id).first;
    auto site_id = leader_id;
    auto proxies = rpc_par_proxies_[partition_id];
    if(follower_forwarding) n_total = 3;
    auto qe = Reactor::CreateSpEvent<QuorumEvent>(n_total, 1);
    e->AddEvent(qe);
    auto src_coroid = qe->GetCoroId();
      
    qe->id_ = Communicator::global_id;
    qe->par_id_ = quorum_id++;
    FutureAttr fuattr;
    fuattr.callback = [e, qe, src_coroid, site_id, coo, phase, cmd](Future* fu) {
      int32_t res;
      uint64_t coro_id = 0;
      fu->get_reply() >> res >> coro_id;

      qe->add_dep(coo->cli_id_, src_coroid, site_id, coro_id); 
      
      if(phase != coo->phase_){
        return;
      }

      if(res == REJECT){
        cmd->commit_.store(false);
        coo->aborted_ = true;
      }
      qe->n_voted_yes_++;
      e->Test();
    };
    
    ClassicProxy* proxy = LeaderProxyForPartition(partition_id).second;
    Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n",
              sids.size(),
              partition_id,
              tid);
    Future::safe_release(proxy->async_Prepare(tid, sids, Communicator::global_id++, fuattr));
    if(follower_forwarding){
      for(auto& pair : rpc_par_proxies_[partition_id]){
        if(pair.first != leader_id){
          site_id = pair.first;
          proxy = pair.second;
          Future::safe_release(proxy->async_Prepare(tid, sids, Communicator::global_id++, fuattr));  
        }
      }
    }
  }
  return e;
}

/*void Communicator::SendPrepare(groupid_t gid,
                               txnid_t tid,
                               std::vector<int32_t>& sids,
                               const function<void(int)>& callback) {
  FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [this, callback](Future* fu) {
        int res;
        fu->get_reply() >> res;
        callback(res);
      };
  fuattr.callback = cb;
  ClassicProxy* proxy = LeaderProxyForPartition(gid).second;
  Log_debug("SendPrepare to %ld sites gid:%ld, tid:%ld\n",
            sids.size(),
            gid,
            tid);
  Future::safe_release(proxy->async_Prepare(tid, sids, fuattr));
}*/

void Communicator::___LogSent(parid_t pid, txnid_t tid) {
  auto value = std::make_pair(pid, tid);
  auto it = phase_three_sent_.find(value);
  if (it != phase_three_sent_.end()) {
    Log_fatal("phase 3 sent exists: %d %x", it->first, it->second);
  } else {
    phase_three_sent_.insert(value);
    Log_debug("phase 3 sent: pid: %d; tid: %x", value.first, value.second);
  }
}

shared_ptr<AndEvent>
Communicator::SendCommit(Coordinator* coo,
                              txnid_t tid) {
#ifdef LOG_LEVEL_AS_DEBUG
//  ___LogSent(pid, tid);
#endif
  TxData* cmd = (TxData*) coo->cmd_;
  int n_total = 1;
  auto n = cmd->GetPartitionIds().size();
  auto e = Reactor::CreateSpEvent<AndEvent>();
  
  for(auto& rp : cmd->partition_ids_){
    auto leader_id = LeaderProxyForPartition(rp).first;
    auto site_id = leader_id;
    auto proxies = rpc_par_proxies_[rp];
    if(follower_forwarding) n_total = 3;
    auto qe = Reactor::CreateSpEvent<QuorumEvent>(n_total, 1);
    qe->id_ = Communicator::global_id;
    auto src_coroid = qe->GetCoroId();

    e->AddEvent(qe);

    coo->n_finish_req_++;
    FutureAttr fuattr;
    auto phase = coo->phase_;
    fuattr.callback = [this, e, qe, src_coroid, site_id, coo, phase, cmd](Future* fu) {
      int32_t res;
      uint64_t coro_id = 0;
			Profiling profile;
      fu->get_reply() >> res >> coro_id >> profile;

	  	if(profile.cpu_util != -1.0){
				Log_info("cpu: %f and network: %f", profile.cpu_util, profile.tx_util);
				this->cpu = profile.cpu_util;
				this->tx = profile.tx_util;
			}

      struct timespec end_;
	  	clock_gettime(CLOCK_REALTIME,&end_);

	  	rrr::i64 start_sec = this->outbound_[src_coroid].first;
	  	rrr::i64 start_nsec = this->outbound_[src_coroid].second;

	  	rrr::i64 curr = ((rrr::i64)end_.tv_sec - start_sec)*1000000000 + ((rrr::i64)end_.tv_nsec - start_nsec);
	  	curr /= 1000;
	  	this->total_time += curr;
	  	this->total++;
      if(this->index < 100){
	    	this->window[this->index];
	    	this->index++;
	    	this->window_time = this->total_time;
	  	}
      else{
	    	this->window_time = 0;
	    	for(int i = 0; i < 99; i++){
	      	this->window[i] = this->window[i+1];
	      	this->window_time += this->window[i];
	    	}
	    	this->window[99] = curr;
	    	this->window_time += curr;
	  	}
			this->window_avg = this->window_time/this->index;
			this->total_avg = this->total_time/this->total;
			Log_info("this time is: %d", curr);
	  	//Log_info("average time of RPC is: %d", this->total_avg);
	 		//Log_info("window time of RPC is: %d", this->window_avg);

      qe->add_dep(coo->cli_id_, src_coroid, site_id, coro_id);

      if(coo->phase_ != phase) return;
      qe->n_voted_yes_++;
      e->Test();
    };

    ClassicProxy* proxy = LeaderProxyForPartition(rp).second;
    Log_debug("SendCommit to %ld tid:%ld\n", rp, tid);
    Future::safe_release(proxy->async_Commit(tid, Communicator::global_id++, fuattr));
    
    if(follower_forwarding){
      for(auto& pair : rpc_par_proxies_[rp]){
        if(pair.first != leader_id){
          site_id = pair.first;
          proxy = pair.second;
          Future::safe_release(proxy->async_Commit(tid, Communicator::global_id++, fuattr));  
        }
      }
    }

    coo->site_commit_[rp]++;

  }
  return e;
}

/*void Communicator::SendCommit(parid_t pid,
                              txnid_t tid,
                              const function<void()>& callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback](Future*) { callback(); };
  auto proxy_pair = LeaderProxyForPartition(pid);
  ClassicProxy* proxy = proxy_pair.second;
  SetLeaderCache(pid, proxy_pair) ;
  Log_debug("SendCommit to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Commit(tid, 0, fuattr));
}*/
shared_ptr<AndEvent>
Communicator::SendAbort(Coordinator* coo,
                              txnid_t tid) {
#ifdef LOG_LEVEL_AS_DEBUG
//  ___LogSent(pid, tid);
#endif
  TxData* cmd = (TxData*) coo->cmd_;
  int n_total = 1;
  auto n = cmd->GetPartitionIds().size();
  auto e = Reactor::CreateSpEvent<AndEvent>();
  for(auto& rp : cmd->partition_ids_){
    auto proxies = rpc_par_proxies_[rp];
    auto leader_id = LeaderProxyForPartition(rp).first;
    auto site_id = leader_id;
    if(follower_forwarding) n_total = 3;
    auto qe = Reactor::CreateSpEvent<QuorumEvent>(n_total, 1);
    qe->id_ = Communicator::global_id;
    auto src_coroid = qe->GetCoroId();

    e->AddEvent(qe);

    coo->n_finish_req_++;
    FutureAttr fuattr;
    auto phase = coo->phase_;
    fuattr.callback = [this, e, qe, coo, src_coroid, site_id, phase, cmd](Future* fu) {
      int32_t res;
      uint64_t coro_id = 0;
			Profiling profile;
      fu->get_reply() >> res >> coro_id >> profile;

	  	if(profile.cpu_util != -1.0){
				Log_info("cpu: %f and network: %f", profile.cpu_util, profile.tx_util);
				this->cpu = profile.cpu_util;
				this->tx = profile.tx_util;
			}

      struct timespec end_;
	  	clock_gettime(CLOCK_REALTIME,&end_);

	  	rrr::i64 start_sec = this->outbound_[src_coroid].first;
	  	rrr::i64 start_nsec = this->outbound_[src_coroid].second;

	  	rrr::i64 curr = ((rrr::i64)end_.tv_sec - start_sec)*1000000000 + ((rrr::i64)end_.tv_nsec - start_nsec);
	  	curr /= 1000;
	  	this->total_time += curr;
	  	this->total++;
      if(this->index < 100){
	    	this->window[this->index];
	    	this->index++;
	    	this->window_time = this->total_time;
	  	}
      else{
	    	this->window_time = 0;
	    	for(int i = 0; i < 99; i++){
	      	this->window[i] = this->window[i+1];
	      	this->window_time += this->window[i];
	    	}
	    	this->window[99] = curr;
	    	this->window_time += curr;
	  	}
			Log_info("this time is: %d", curr);
	  	Log_info("average time of RPC is: %d", this->total_time/this->total);
	 		Log_info("window time of RPC is: %d", this->window_time/this->index);

      qe->add_dep(coo->cli_id_, src_coroid, site_id, coro_id); 

      if(coo->phase_ != phase) return;
      qe->n_voted_yes_++;
      e->Test();
    };

    ClassicProxy* proxy = LeaderProxyForPartition(rp).second;
    Log_debug("SendAbort to %ld tid:%ld\n", rp, tid);
    Future::safe_release(proxy->async_Abort(tid, Communicator::global_id++, fuattr));

    if(follower_forwarding){
      for(auto& pair : rpc_par_proxies_[rp]){
        if(pair.first != leader_id){
          site_id = pair.first;
          proxy = pair.second;
          Future::safe_release(proxy->async_Abort(tid, Communicator::global_id++, fuattr));  
        }
      }

    }
    coo->site_abort_[rp]++;
  }
  return e;
}

/*void Communicator::SendAbort(parid_t pid, txnid_t tid,
                             const function<void()>& callback) {
#ifdef LOG_LEVEL_AS_DEBUG
  ___LogSent(pid, tid);
#endif
  FutureAttr fuattr;
  fuattr.callback = [callback](Future*) { callback(); };
  auto proxy_pair = LeaderProxyForPartition(pid);
  ClassicProxy* proxy = proxy_pair.second;
  SetLeaderCache(pid, proxy_pair) ;
  Log_debug("SendAbort to %ld tid:%ld\n", pid, tid);
  Future::safe_release(proxy->async_Abort(tid, 0, fuattr));
}*/

void Communicator::SendUpgradeEpoch(epoch_t curr_epoch,
                                    const function<void(parid_t,
                                                        siteid_t,
                                                        int32_t&)>& callback) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      auto& site_id = pair.first;
      function<void(Future*)> cb = [callback, par_id, site_id](Future* fu) {
        int32_t res;
        fu->get_reply() >> res;
        callback(par_id, site_id, res);
      };
      fuattr.callback = cb;
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_UpgradeEpoch(curr_epoch, fuattr));
    }
  }
}

void Communicator::SendTruncateEpoch(epoch_t old_epoch) {
  for (auto& pair: rpc_par_proxies_) {
    auto& par_id = pair.first;
    auto& proxies = pair.second;
    for (auto& pair: proxies) {
      FutureAttr fuattr;
      fuattr.callback = [](Future*) {};
      auto proxy = (ClassicProxy*) pair.second;
      Future::safe_release(proxy->async_TruncateEpoch(old_epoch));
    }
  }
}

void Communicator::SendForwardTxnRequest(
    TxRequest& req,
    Coordinator* coo,
    std::function<void(const TxReply&)> callback) {
  Log_info("%s: %d, %d", __FUNCTION__, coo->coo_id_, coo->par_id_);
  verify(client_leaders_.size() > 0);
  auto idx = rrr::RandomGenerator::rand(0, client_leaders_.size() - 1);
  auto p = client_leaders_[idx];
  auto leader_site_id = p.first;
  auto leader_proxy = p.second;
  Log_debug("%s: send to client site %d", __FUNCTION__, leader_site_id);
  TxDispatchRequest dispatch_request;
  dispatch_request.id = coo->coo_id_;
  for (size_t i = 0; i < req.input_.size(); i++) {
    dispatch_request.input.push_back(req.input_[i]);
  }
  dispatch_request.tx_type = req.tx_type_;

  FutureAttr future;
  future.callback = [callback](Future* f) {
    TxReply reply;
    f->get_reply() >> reply;
    callback(reply);
  };
  Future::safe_release(leader_proxy->async_DispatchTxn(dispatch_request,
                                                       future));
}

vector<shared_ptr<MessageEvent>>
Communicator::BroadcastMessage(shardid_t shard_id,
                               svrid_t svr_id,
                               string& msg) {
  verify(0);
  // TODO
  vector<shared_ptr<MessageEvent>> events;

  for (auto& p : rpc_par_proxies_[shard_id]) {
    auto site_id = p.first;
    auto proxy = (p.second);
    verify(proxy != nullptr);
    FutureAttr fuattr;
    auto msg_ev = std::make_shared<MessageEvent>(shard_id, site_id);
    events.push_back(msg_ev);
    fuattr.callback = [msg_ev] (Future* fu) {
      auto& marshal = fu->get_reply();
      marshal >> msg_ev->msg_;
      msg_ev->Set(1);
    };
    Future* f = nullptr;
    Future::safe_release(f);
    return events;
  }
}

shared_ptr<MessageEvent>
Communicator::SendMessage(siteid_t site_id,
                          string& msg) {
  verify(0);
  // TODO
  auto ev = std::make_shared<MessageEvent>(site_id);
  return ev;
}


void Communicator::AddMessageHandler(
    function<bool(const MarshallDeputy&, MarshallDeputy&)> f) {
   msg_marshall_handlers_.push_back(f);
}

shared_ptr<GetLeaderQuorumEvent>
  Communicator::BroadcastGetLeader(parid_t par_id, locid_t cur_pause ) 
{  
  int n = Config::GetConfig()->GetPartitionSize(par_id);  
  auto e = Reactor::CreateSpEvent<GetLeaderQuorumEvent>(n-1, 1);  
  auto proxies = rpc_par_proxies_[par_id];
  int count = 0 ;
  WAN_WAIT;  
  for (auto& p : proxies) {
    count++ ;
    if (p.first == cur_pause)        
      continue;    
    auto proxy = p.second;    
    FutureAttr fuattr;    
    fuattr.callback = [e, p](Future* fu) {
      bool_t is_leader  = false ;      
      fu->get_reply() >> is_leader ;      
      e->FeedResponse(is_leader, p.first); 
    };    
    Future::safe_release(proxy->async_IsFPGALeader(cur_pause, fuattr));  
  }  
  verify(count>=3) ; // TODO del it
  return e;
}

shared_ptr<QuorumEvent> Communicator::SendFailOverTrig(parid_t par_id, 
                                          locid_t loc_id, bool pause ) 
{  
  int n = Config::GetConfig()->GetPartitionSize(par_id);  
  auto e = Reactor::CreateSpEvent<QuorumEvent>(1,1);
  auto proxies = rpc_par_proxies_[par_id];  
  WAN_WAIT;  
  for (auto& p : proxies) {    
    if (p.first != loc_id)        
      continue;    
    auto proxy = p.second;    
    FutureAttr fuattr;
    fuattr.callback = [e](Future* fu) {
          int res;
          fu->get_reply() >> res;
          if (res == 0) e->VoteYes() ;
          else e->VoteNo() ;
        };
    Future::safe_release(proxy->async_FailOverTrig(pause, fuattr));  
  }  
  return e ;
}


void Communicator::SetNewLeaderProxy(parid_t par_id, locid_t loc_id)
{
  bool found = false ;
  auto proxies = rpc_par_proxies_[par_id];  
  for (auto& p : proxies) {    
    if (p.first == loc_id) {
       leader_cache_[par_id] = p;
       found = true ;
       break ;
    }
  }  

  verify(found) ;

/*  auto it = rpc_par_proxies_.find(par_id);
  verify(it != rpc_par_proxies_.end());
  auto& partition_proxies = it->second;
  auto config = Config::GetConfig();
  auto proxy_it = std::find_if(
      partition_proxies.begin(),
      partition_proxies.end(),
      [config, loc_id](const std::pair<siteid_t, ClassicProxy*>& p) {
        verify(p.second != nullptr);
        auto& site = config->SiteById(p.first);
        return site.locale_id == loc_id ;
      });
   verify (proxy_it != partition_proxies.end()) ;
   leader_cache_[par_id] = *proxy_it;*/
   Log_debug("set leader porxy for parition %d is %d", par_id, loc_id);
}

void Communicator::SendSimpleCmd(groupid_t gid,
                               SimpleCommand & cmd,
                               std::vector<int32_t>& sids,
                               const function<void(int)>& callback)
{
  FutureAttr fuattr;
  std::function<void(Future*)> cb =
      [this, callback](Future* fu) {
        int res;
        fu->get_reply() >> res;
        callback(res);
      };
  fuattr.callback = cb;
  ClassicProxy* proxy = LeaderProxyForPartition(gid).second;
  Log_debug("SendEmptyCmd to %ld sites gid:%ld\n",
            sids.size(),
            gid);
  Future::safe_release(proxy->async_SimpleCmd(cmd, fuattr));
}


} // namespace janus
