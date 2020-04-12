
#include "service.h"
#include "server.h"

namespace janus {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(TxLogServer *sched)
    : sched_((PaxosServer*)sched) {

}

void MultiPaxosServiceImpl::Forward(const MarshallDeputy& md_cmd,
                                    const uint64_t& dep_id,
                                    uint64_t* coro_id,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto coro = Coroutine::CreateRun([&] () {
    sched_->OnForward(const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                      dep_id,
                      coro_id,
                      std::bind(&rrr::DeferredReply::reply, defer));
  });
}

void MultiPaxosServiceImpl::Prepare(const uint64_t& slot,
                                    const ballot_t& ballot,
                                    ballot_t* max_ballot,
                                    uint64_t* coro_id,
                                    rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  sched_->OnPrepare(slot,
                    ballot,
                    max_ballot,
                    coro_id,
                    std::bind(&rrr::DeferredReply::reply, defer));
}

void MultiPaxosServiceImpl::Accept(const uint64_t& slot,
		                   const uint64_t& time,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   ballot_t* max_ballot,
                                   uint64_t* coro_id,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto start = chrono::system_clock::now();

  time_t tstart = chrono::system_clock::to_time_t(start);
  tm * date = localtime(&tstart);
  date->tm_hour = 0;
  date->tm_min = 0;
  date->tm_sec = 0;
  auto midn = chrono::system_clock::from_time_t(std::mktime(date));

  auto hours = chrono::duration_cast<chrono::hours>(start-midn);
  auto minutes = chrono::duration_cast<chrono::minutes>(start-midn);
  auto seconds = chrono::duration_cast<chrono::seconds>(start-midn);

  auto start_ = chrono::duration_cast<chrono::microseconds>(start-midn-hours-minutes).count();
  Log_info("Duration of RPC is: %d", start_-time);

  auto coro = Coroutine::CreateRun([&] () {
    sched_->OnAccept(slot,
		     time,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                     max_ballot,
                     coro_id,
                     std::bind(&rrr::DeferredReply::reply, defer));

  });

  auto end = chrono::system_clock::now();
  auto duration = chrono::duration_cast<chrono::microseconds>(end-start);
  Log_info("Duration of Accept() at Follower's side is: %d", duration.count());
  //Log_info("coro id on service side: %d", coro->id);
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto x = md_cmd.sp_data_;
  sched_->OnCommit(slot, ballot,x);
  defer->reply();
}


} // namespace janus;
