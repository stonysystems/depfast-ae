
#include "service.h"
#include "scheduler.h"

namespace janus {

MultiPaxosServiceImpl::MultiPaxosServiceImpl(Scheduler *sched)
    : sched_((SchedulerMultiPaxos*)sched) {

}

void MultiPaxosServiceImpl::Forward(const MarshallDeputy& cmd,
                                    rrr::DeferredReply* defer) {
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
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   ballot_t* max_ballot,
                                   uint64_t* coro_id,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  auto coro = Coroutine::CreateRun([&] () {
    sched_->OnAccept(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_,
                     max_ballot,
                     coro_id,
                     std::bind(&rrr::DeferredReply::reply, defer));

  });
  Log_info("coro id on service side: %d", coro->id);
}

void MultiPaxosServiceImpl::Decide(const uint64_t& slot,
                                   const ballot_t& ballot,
                                   const MarshallDeputy& md_cmd,
                                   rrr::DeferredReply* defer) {
  verify(sched_ != nullptr);
  Coroutine::CreateRun([&] () {
    sched_->OnCommit(slot,
                     ballot,
                     const_cast<MarshallDeputy&>(md_cmd).sp_data_);
    defer->reply();
  });
}


} // namespace janus;
