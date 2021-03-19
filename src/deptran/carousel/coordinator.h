#pragma once

#include "../none/coordinator.h"

namespace janus {

class CarouselCommo;
class CoordinatorCarousel : public CoordinatorClassic {
 public:
   CoordinatorCarousel(uint32_t coo_id,
                      int benchmark,
                      ClientControlServiceImpl* ccsi,
                      uint32_t thread_id);
  virtual ~CoordinatorCarousel() {}
  
  enum Phase {INIT_END=0, READPREPARE=1, DECIDE=2};
  enum Decision { UNKNOWN = 0, COMMIT = 1, ABORT = 2};
  map<innid_t, int> n_prepare_super_maj_ = {};
  map<innid_t, int> n_prepare_no_maj_ = {};  
  map<innid_t, int> n_prepare_oks_ = {};
  map<innid_t, int> n_prepare_rejects_ = {};
  map<innid_t, bool> n_leader_reply_ = {};
  const bool using_basic_;
  
//  using CoordinatorClassic::CoordinatorClassic;

  void Reset() override;
  CarouselCommo* commo();

  void ReadAndPrepare();
  void ReadAndPrepareAck(phase_t, cmdid_t, parid_t, bool leader, int32_t res, TxnOutput& output);

  bool AllDispatchAcked();
  bool SuperMajorPrepareOK();
  bool NoMajorPrepareOK();  

  void Decide();

  void Restart() override;
  void GotoNextPhase() override;
};

} // namespace janus
