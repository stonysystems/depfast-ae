#pragma once

#include "__dep__.h"
#include "constants.h"
#include "msg.h"
#include "config.h"
#include "command_marshaler.h"
#include "deptran/rcc/dep_graph.h"
#include "rcc_rpc.h"
#include <unordered_map>

namespace janus {

static void _wan_wait() {
  Reactor::CreateSpEvent<NeverEvent>()->Wait(100*1000);
}

#ifdef SIMULATE_WAN

#define WAN_WAIT _wan_wait();

#else

#define WAN_WAIT ;

#endif


class Coordinator;
class ClassicProxy;
class ClientControlProxy;

typedef std::pair<siteid_t, ClassicProxy*> SiteProxyPair;
typedef std::pair<siteid_t, ClientControlProxy*> ClientSiteProxyPair;

class MessageEvent : public IntEvent {
 public:
  shardid_t shard_id_;
  svrid_t svr_id_;
  string msg_;
  MessageEvent(svrid_t svr_id) : IntEvent(), svr_id_(svr_id) {

  }

  MessageEvent(shardid_t shard_id, svrid_t svr_id)
      : IntEvent(), shard_id_(shard_id), svr_id_(svr_id) {

  }
};

class GetLeaderQuorumEvent : public QuorumEvent {
 public:
  using QuorumEvent::QuorumEvent;
  void FeedResponse(bool y, locid_t leader_id) {
    if (y) {
      leader_id_ = leader_id;
      VoteYes();
    } else {
      VoteNo();
    }
  }

  bool No() override { return n_voted_no_ == n_total_; }

  bool IsReady() override {
    if (Yes()) {
      return true;
    } else if (No()) {
      return true;
    }

    return false;
  }
};

class Communicator {
 public:
  static uint64_t global_id;
  const int CONNECT_TIMEOUT_MS = 120*1000;
  const int CONNECT_SLEEP_MS = 1000;
  rrr::PollMgr *rpc_poll_ = nullptr;
  locid_t loc_id_ = -1;
	cliid_t cli_id_ = -1;
  map<siteid_t, shared_ptr<rrr::Client>> rpc_clients_{};
  map<siteid_t, ClassicProxy *> rpc_proxies_{};
  map<parid_t, vector<SiteProxyPair>> rpc_par_proxies_{};
  map<parid_t, SiteProxyPair> leader_cache_ = {};
  unordered_map<uint64_t, pair<rrr::i64, rrr::i64>> outbound_{};
	map<uint64_t, double> lat_util_{};
  locid_t leader_ = 0;
	int outbound = 0;
	int outbounds[100];
	int ob_index = 0;
	int begin_index = 0;
	bool paused = false;
	bool slow = false;
	int index;
	int cpu_index;
	int low_util;
  int total;
	int total_;
	shared_ptr<QuorumEvent> qe;
  rrr::i64 window[200];
  rrr::i64 window_time;
  rrr::i64 total_time;
	rrr::i64 window_avg;
	rrr::i64 total_avg;
	double cpu_stor[10];
	double cpu_total;
	double cpu = 1.0;
	double last_cpu = 1.0;
	double tx;
  vector<ClientSiteProxyPair> client_leaders_;
  std::atomic_bool client_leaders_connected_;
  std::vector<std::thread> threads;
  bool broadcasting_to_leaders_only_{true};
  bool follower_forwarding{false};
  std::recursive_mutex mtx_{};
	std::mutex lock_;
	std::mutex count_lock_;
	std::condition_variable cv_;
	bool waiting = false;

  Communicator(PollMgr* poll_mgr = nullptr);
  virtual ~Communicator();

  SiteProxyPair RandomProxyForPartition(parid_t partition_id) const;
  SiteProxyPair LeaderProxyForPartition(parid_t) const;
  SiteProxyPair NearestProxyForPartition(parid_t) const;
  void SetLeaderCache(parid_t par_id, SiteProxyPair& proxy) {
    leader_cache_[par_id] = proxy;
  }
  virtual SiteProxyPair DispatchProxyForPartition(parid_t par_id) const {
    return LeaderProxyForPartition(par_id);
  };
  locid_t GenerateNewLeaderId(parid_t par_id) {
    return leader_cache_[par_id].first = leader_cache_[par_id].first + 1;
  };
  std::pair<int, ClassicProxy*> ConnectToSite(Config::SiteInfo &site,
                                              std::chrono::milliseconds timeout_ms);
  ClientSiteProxyPair ConnectToClientSite(Config::SiteInfo &site,
                                          std::chrono::milliseconds timeout);
  void ConnectClientLeaders();
  void WaitConnectClientLeaders();

  vector<function<bool(const string& arg, string& ret)> >
      msg_string_handlers_{};
  vector<function<bool(const MarshallDeputy& arg,
                       MarshallDeputy& ret)> > msg_marshall_handlers_{};

	void ResetProfiles();
  void SendStart(SimpleCommand& cmd,
                 int32_t output_size,
                 std::function<void(Future *fu)> &callback);
  void BroadcastDispatch(shared_ptr<vector<shared_ptr<SimpleCommand>>> vec_piece_data,
                         Coordinator *coo,
                         const std::function<void(int res, TxnOutput &)> &) ;

	shared_ptr<QuorumEvent> SendReelect();

  shared_ptr<IntEvent> BroadcastDispatch(ReadyPiecesData cmds_by_par,
                        Coordinator* coo,
                        TxData* txn);

  shared_ptr<AndEvent> SendPrepare(Coordinator* coo,
                                         txnid_t tid,
                                         std::vector<int32_t>& sids);
  shared_ptr<AndEvent> SendCommit(Coordinator* coo,
                                     txnid_t tid);
  shared_ptr<AndEvent> SendAbort(Coordinator* coo,
                                    txnid_t tid);
  /*void SendPrepare(parid_t gid,
                   txnid_t tid,
                   std::vector<int32_t> &sids,
                   const std::function<void(int)> &callback) ;*/
  /*void SendCommit(parid_t pid,
                  txnid_t tid,
                  const std::function<void()> &callback) ;
  void SendAbort(parid_t pid,
                 txnid_t tid,
                 const std::function<void()> &callback) ;*/
  void SendEarlyAbort(parid_t pid,
                      txnid_t tid) ;

  // for debug
  std::set<std::pair<parid_t, txnid_t>> phase_three_sent_;

  void ___LogSent(parid_t pid, txnid_t tid);

  void SendUpgradeEpoch(epoch_t curr_epoch,
                        const function<void(parid_t,
                                            siteid_t,
                                            int32_t& graph)>& callback);

  void SendTruncateEpoch(epoch_t old_epoch);
  void SendForwardTxnRequest(TxRequest& req, Coordinator* coo, std::function<void(const TxReply&)> callback);

  /**
   *
   * @param shard_id 0 means broadcast to all shards.
   * @param svr_id 0 means broadcast to all replicas in that shard.
   * @param msg
   */
  vector<shared_ptr<MessageEvent>> BroadcastMessage(shardid_t shard_id,
                                                    svrid_t svr_id,
                                                    string& msg);
  std::shared_ptr<MessageEvent> SendMessage(svrid_t svr_id, string& msg);

  void AddMessageHandler(std::function<bool(const string&, string&)>);
  void AddMessageHandler(std::function<bool(const MarshallDeputy&,
                                            MarshallDeputy&)>);
  shared_ptr<GetLeaderQuorumEvent> BroadcastGetLeader(parid_t par_id, locid_t cur_pause);
  shared_ptr<QuorumEvent> SendFailOverTrig(parid_t par_id, locid_t loc_id, bool pause);
  void SetNewLeaderProxy(parid_t par_id, locid_t loc_id);
  void SendSimpleCmd(groupid_t gid, SimpleCommand& cmd, std::vector<int32_t>& sids,
      const function<void(int)>& callback);
};

} // namespace janus
