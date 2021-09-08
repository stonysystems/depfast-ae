#include "../config.h"
#include "../multi_value.h"
#include "../procedure.h"
#include "../txn_reg.h"
#include "scheduler.h"

namespace janus {
bool SchedulerNone::Dispatch(cmdid_t cmd_id, shared_ptr<Marshallable> cmd,
                             TxnOutput& ret_output) {
  auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(cmd_id));
	SchedulerClassic::Dispatch(cmd_id, cmd, ret_output);
	sp_tx->fully_dispatched_->Wait();
	OnCommit(cmd_id, SUCCESS);
	return true;
}
} // namespace janus
