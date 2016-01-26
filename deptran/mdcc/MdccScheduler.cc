#include "MdccScheduler.h"
#include "deptran/frame.h"
#include "deptran/txn_chopper.h"
#include "executor.h"

namespace mdcc {
 using rococo::TxnRequest;
 using rococo::TxnReply;
 using rococo::Frame;

  bool MdccScheduler::LaunchNextPiece(txnid_t txn_id, rococo::TxnChopper *chopper) {
    std::lock_guard<std::mutex> lock(mtx_);
    if (chopper->HasMoreSubCmdReadyNotOut()) {
      auto cmd = static_cast<rococo::SimpleCommand*>(chopper->GetNextSubCmd());
      cmd->id_ = txn_id;
      Log_info("Start sub-command: command site_id is %d %d %d", cmd->GetSiteId(), cmd->type_, cmd->inn_id_);
      std::function<void(StartPieceResponse&)> callback;
      callback = std::function<void(StartPieceResponse&)>([this, txn_id, chopper, callback, cmd] (StartPieceResponse& response) {
        if (response.result!=SUCCESS) {
          Log_info("piece %d failed.", cmd->inn_id());
        } else {
          Log_info("piece %d success.", cmd->inn_id());
          this->LaunchNextPiece(txn_id, chopper);
        }
      });
      GetOrCreateCommunicator()->SendStartPiece(*cmd, callback);
      return true;
    } else {
      Log_debug("no more subcommands or no sub-commands ready.");
      return false;
    }
  }

  void MdccScheduler::StartTransaction(txnid_t txn_id,
                                       txntype_t txn_type,
                                       const map<int32_t, Value> &inputs,
                                       i8* result,
                                       rrr::DeferredReply *defer) {
    TxnRequest req;
    req.txn_type_ = txn_type;
    req.input_ = inputs;
    req.n_try_ = 0;
    req.callback_ = [] (TxnReply& reply) {
      Log_info("TxnReq callback!!!!!! %d", reply.n_try_);
    };

    auto chopper = Frame().CreateChopper(req, txn_reg_);
    Log_debug("chopper num pieces %d", chopper->GetNPieceAll());
    do {} while(LaunchNextPiece(txn_id, chopper));
    Log_debug("exit %s", __FUNCTION__);
  }

  void MdccScheduler::init(Config *config, uint32_t site_id) {
    this->config_ = config;
    this->site_id_ = site_id;
  }

  void MdccScheduler::StartPiece(const rococo::SimpleCommand& cmd, int32_t* result, DeferredReply *defer) {
    auto executor = static_cast<MdccExecutor*>(this->GetOrCreateExecutor(cmd.id_));
    executor->StartPiece(cmd, result);
    defer->reply();
  }
}