
#include "quorum_event.h"
#include "coroutine.h"

namespace janus {

using rrr::Coroutine;

void QuorumEvent::Finalize(uint64_t timeout, function<bool()> finalize_func) {
	Coroutine::CreateRun([this, timeout, finalize_func]() {
		bool ret = false;
		finalize_event_->Wait(timeout);

		if (finalize_event_->status_ == Event::TIMEOUT)
			ret = finalize_func();
	});
}

void QuorumEvent::AddXid(uint16_t site, rrr::i64 xid) {
	xids_[site] = xid;
}

void QuorumEvent::RemoveXid(uint16_t site) {
  auto it = xids_.find(site);
  if (it != xids_.end())
		xids_.erase(it);
}

} // namespace janus
