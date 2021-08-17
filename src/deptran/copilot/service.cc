#include "service.h"
#include "server.h"

namespace janus {

CopilotServiceImpl::CopilotServiceImpl(TxLogServer *sched)
    : sched_((CopilotServer *)sched) {
}

void CopilotServiceImpl::Forward(const MarshallDeputy& cmd,
                                 rrr::DeferredReply* defer) {
  verify(sched_);
  auto coro = Coroutine::CreateRun([&]() {
    sched_->OnForward(const_cast<MarshallDeputy&>(cmd).sp_data_,
                      std::bind(&rrr::DeferredReply::reply, defer));
  });
}

void CopilotServiceImpl::Prepare(const uint8_t& is_pilot,
                                 const uint64_t& slot,
                                 const ballot_t& ballot,
                                 MarshallDeputy* ret_cmd,
                                 ballot_t* max_ballot,
                                 uint64_t* dep,
                                 status_t* status,
                                 rrr::DeferredReply* defer) {
  verify(sched_);
  sched_->OnPrepare(is_pilot, slot,
                    ballot,
                    ret_cmd->sp_data_,
                    max_ballot,
                    dep,
                    status,
                    bind(&rrr::DeferredReply::reply, defer));
}

void CopilotServiceImpl::FastAccept(const uint8_t& is_pilot,
                                    const uint64_t& slot,
                                    const ballot_t& ballot,
                                    const uint64_t& dep,
                                    const MarshallDeputy& cmd,
                                    ballot_t* max_ballot,
                                    uint64_t* ret_dep,
                                    rrr::DeferredReply* defer) {
  verify(sched_);

  // auto coro = Coroutine::CreateRun([&]() {
    sched_->OnFastAccept(is_pilot, slot,
                         ballot,
                         dep,
                         const_cast<MarshallDeputy&>(cmd).sp_data_,
                         max_ballot,
                         ret_dep,
                         bind(&rrr::DeferredReply::reply, defer));
  // });
}

void CopilotServiceImpl::Accept(const uint8_t& is_pilot,
                                const uint64_t& slot,
                                const ballot_t& ballot,
                                const uint64_t& dep,
                                const MarshallDeputy& cmd,
                                ballot_t* max_ballot,
                                rrr::DeferredReply* defer) {
  verify(sched_);

  // auto coro = Coroutine::CreateRun([&]() {
    sched_->OnAccept(is_pilot, slot,
                     ballot,
                     dep,
                     const_cast<MarshallDeputy&>(cmd).sp_data_,
                     max_ballot,
                     bind(&rrr::DeferredReply::reply, defer));
  // });
}

void CopilotServiceImpl::Commit(const uint8_t& is_pilot,
                                const uint64_t& slot,
                                const uint64_t& dep,
                                const MarshallDeputy& cmd,
                                rrr::DeferredReply* defer) {
  verify(sched_);
  // Coroutine::CreateRun([&]() {
    sched_->OnCommit(is_pilot, slot, dep,
                     const_cast<MarshallDeputy&>(cmd).sp_data_);
    defer->reply();
  // });
}

} // namespace janus
