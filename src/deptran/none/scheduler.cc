#include "../config.h"
#include "../multi_value.h"
#include "../procedure.h"
#include "../txn_reg.h"
#include "scheduler.h"
#include "../rcc_rpc.h"

namespace janus {
bool SchedulerNone::Dispatch(cmdid_t cmd_id, shared_ptr<Marshallable> cmd,
                             TxnOutput& ret_output) {
	auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(cmd_id));
	DepId di;
	di.str = "dep";
	di.id = 0;
	SchedulerClassic::Dispatch(cmd_id, di, cmd, ret_output);
	sp_tx->fully_dispatched_->Wait();
	OnCommit(cmd_id, di, SUCCESS);  // it waits for the command to be executed

	return true;
}
} // namespace janus
