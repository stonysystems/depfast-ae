#include "../config.h"
#include "../multi_value.h"
#include "../procedure.h"
#include "../txn_reg.h"
#include "../rcc/dep_graph.h"
#include "../rcc/graph_marshaler.h"
#include "scheduler.h"

namespace janus {
bool SchedulerNone::Dispatch(cmdid_t cmd_id, shared_ptr<Marshallable> cmd,
                             TxnOutput& ret_output) {
	auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(cmd_id));
	DepId di = { "dep", 0 };
	SchedulerClassic::Dispatch(cmd_id, di, cmd, ret_output);
	sp_tx->fully_dispatched_->Wait();
	// auto begin = Time::now(true);
	std::vector<shared_ptr<QuorumEvent>> quorum_events;
	OnCommit(cmd_id, di, SUCCESS, quorum_events);  // it waits for the command to be executed

	for (int i = 0; i < quorum_events.size(); i++) {
		quorum_events[i]->Finalize(1*1000*1000, 0);
		//Log_info("use_count5: %d", quorum_events[i].use_count());
	}
	// cout << Time::now(true) - begin << endl;
	return true;
}
} // namespace janus
