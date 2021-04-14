#include <utility>

#include <functional>
#include <iostream>
#include <boost/coroutine2/protected_fixedsize_stack.hpp>
#include "../base/all.hpp"
#include "coroutine.h"
#include "reactor.h"


namespace rrr {
uint64_t Coroutine::global_id = 0;

Coroutine::Coroutine(std::function<void()> func) : func_(func), status_(INIT), id(Coroutine::global_id++) {
	begin_time = Time::now();
}

Coroutine::~Coroutine() {
  verify(up_boost_coro_task_);
  up_boost_coro_task_.reset();
//  verify(0);
}

void Coroutine::DoFinalize() {
	for (int i = 0; i < quorum_events_.size(); i++) {
		auto qe = std::dynamic_pointer_cast<janus::QuorumEvent>(quorum_events_[i]);
		verify(qe);
		if (!qe->needs_finalize_) {
			qe->finalize_event->Wait(1*1000*1000);
		}
	}
	quorum_events_.clear();
	status_ = FINISHED;
}

void Coroutine::BoostRunWrapper(boost_coro_yield_t& yield) {
  boost_coro_yield_ = yield;
  verify(func_);
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  while (true) {
    auto sz = reactor->coros_.size();
    verify(sz > 0);
    verify(func_);
    func_();
//    func_ = nullptr; // Can be swapped out here?
		status_ = FINISHED;
		if (needs_finalize_) {
			print_warning = true;
		}
		int elapsed_time_us = rrr::Time::now() - begin_time;

		if (elapsed_time_us >= PRINT_INTERVAL) {
			if (print_warning) {
				Log_info("Warning: We did not deal with backlog issues");
			}
			print_warning = false;
			begin_time = rrr::Time::now();
		}

		//if (!quorum_events_.empty()) Log_info("use_count6: %d", quorum_events_[0].use_count());
		quorum_events_.clear();
    Reactor::GetReactor()->n_active_coroutines_--;
    yield();
  }
}

void Coroutine::Run() {
  verify(!up_boost_coro_task_);
  verify(status_ == INIT);
  status_ = STARTED;
  auto reactor = Reactor::GetReactor();
//  reactor->coros_;
  auto sz = reactor->coros_.size();
  verify(sz > 0);
//  up_boost_coro_task_ = make_shared<boost_coro_task_t>(

  const auto x = new boost_coro_task_t(
#ifdef USE_PROTECTED_STACK
      boost::coroutines2::protected_fixedsize_stack(),
#endif
      std::bind(&Coroutine::BoostRunWrapper, this, std::placeholders::_1)
//    [this] (boost_coro_yield_t& yield) {
//      this->BoostRunWrapper(yield);
//    }
      );
  verify(up_boost_coro_task_ == nullptr);
  up_boost_coro_task_.reset(x);
#ifdef USE_BOOST_COROUTINE1
  (*up_boost_coro_task_)();
#endif
}

void Coroutine::Yield() {
  verify(boost_coro_yield_);
  verify(status_ == STARTED || status_ == RESUMED || status_ == FINALIZING);
  status_ = PAUSED;
  Reactor::GetReactor()->n_active_coroutines_--;
  boost_coro_yield_.value()();
}

void Coroutine::Continue() {
  verify(status_ == PAUSED || status_ == RECYCLED);
  verify(up_boost_coro_task_);
  status_ = RESUMED;
  auto& r = *up_boost_coro_task_;
  verify(r);
  r();
  // some events might have been triggered from last coroutine,
  // but you have to manually call the scheduler to loop.
}

bool Coroutine::Finished() {
  return status_ == FINISHED;
}

} // namespace rrr
