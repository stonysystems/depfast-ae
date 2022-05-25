#include "../constants.h"
#include "../tx.h"
#include "../procedure.h"
#include "../coordinator.h"
#include "../2pl/tx.h"
#include "../tx.h"
#include "../classic/tpc_command.h"
#include "scheduler.h"

namespace janus
{

int SchedulerNoneCopilot::OnCommit(cmdid_t tx_id,
								   struct DepId dep_id,
								   int commit_or_abort) {
	std::lock_guard<std::recursive_mutex> lock(mtx_);
	Log_debug("%s: at site %d, tx: %" PRIx64,
            __FUNCTION__, this->site_id_, tx_id);
	auto sp_tx = dynamic_pointer_cast<TxClassic>(GetOrCreateTx(tx_id));
	// TODO maybe change inuse to an event?
	//  verify(!sp_tx->inuse);
	//  sp_tx->inuse = true;
	//
	//always true
	if (Config::GetConfig()->IsReplicated()) {
		auto cmd = std::make_shared<TpcCommitCommand>();
		cmd->tx_id_ = tx_id;
		cmd->ret_ = commit_or_abort;
		cmd->cmd_ = sp_tx->cmd_;
		sp_tx->is_leader_hint_ = true;

		auto copilot_server = static_cast<CopilotServer*>(rep_sched_);
		batch_buffer_.push_back(cmd);
		if (!in_waiting_) {
			
			if (copilot_server->WillWait())
				in_waiting_ = true;
			copilot_server->WaitForPingPong();
			in_waiting_ = false;
			
			auto batch_cmd = std::make_shared<TpcBatchCommand>();
			batch_cmd->AddCmds(batch_buffer_);
			batch_buffer_.clear();

			submit_++;
			total_ += batch_cmd->Size();
			if (submit_ % 1000 == 0)
				Log_info("avg batch size %f", (double)total_/submit_);
			shared_ptr<Coordinator> coo(CreateRepCoord(dep_id.id));
			auto sp_m = dynamic_pointer_cast<Marshallable>(batch_cmd);
			coo->Submit(sp_m);
		}
		sp_tx->commit_result->Wait();
	} else {
		if (commit_or_abort == SUCCESS) {
			DoCommit(*sp_tx);
		} else if (commit_or_abort == REJECT) {
		//      exec->AbortLaunch(res, callback);
			DoAbort(*sp_tx);
		} else {
			verify(0);
		}
	}
	return 0;
}

} // namespace janus
