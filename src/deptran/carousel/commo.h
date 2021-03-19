#pragma once

#include "../__dep__.h"
#include "../communicator.h"

namespace janus {

class Simplecommand;
class CarouselCommo : public Communicator {
 public:
  CarouselCommo(PollMgr* poll_mgr = nullptr);
  virtual ~CarouselCommo() {}
  bool using_basic_;

  void BroadcastReadAndPrepare(
      shared_ptr<vector<shared_ptr<TxPieceData>>> sp_vec_piece,
      Coordinator* coo,
      const function<void(bool, int, TxnOutput&)> & callback) ;

  void BroadcastDecide(parid_t,
                       cmdid_t cmd_id,
                       int32_t decision);
};

} // namespace janus
