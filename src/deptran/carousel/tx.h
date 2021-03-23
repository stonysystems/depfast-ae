#pragma once

#include "../__dep__.h"
#include "../occ/tx.h"
#include "../classic/tx.h"

namespace janus {
/*
class TxCarousel : public TxOcc {
  using TxOcc::TxOcc;
};*/

class TxCarousel : public TxClassic {
 public:
  set<VersionedRow *> locked_rows_{};
  unordered_map<Row *, unordered_map<colid_t, mdb::version_t>> read_vers_{};
  unordered_map<Row *, unordered_map<colid_t, Value>> write_bufs_ = {};
  unordered_map<VersionedRow *, unordered_map<colid_t, uint64_t>> prepared_rvers_{};
  unordered_map<VersionedRow *, unordered_map<colid_t, uint64_t>> prepared_wvers_{};

  using TxClassic::TxClassic;

  mdb::Txn *mdb_txn();

  Row *CreateRow(const mdb::Schema *schema,
                 const std::vector<mdb::Value> &values) override {
    return mdb::VersionedRow::create(schema, values);
  }

  virtual bool ReadColumn(Row *row,
                          mdb::colid_t col_id,
                          Value *value,
                          int hint_flag = TXN_SAFE) override;

  virtual bool WriteColumn(Row *row,
                           colid_t col_id,
                           const Value &value,
                           int hint_flag = TXN_SAFE) override;

  virtual ~TxCarousel();
};

class TpcPrepareCarouselCommand : public Marshallable {
 public:
  TpcPrepareCarouselCommand() : Marshallable(MarshallDeputy::CMD_TPC_PREPARE_CAROUSEL) {
  }
  txnid_t tx_id_ = 0;
  int32_t ret_ = -1;
  // TODO: yidawu try to use pointer
  unordered_map<uint64_t, int> pending_write_row_map_;
  unordered_map<uint64_t, int> pending_read_row_map_;
  shared_ptr<Marshallable> cmd_{nullptr};
  Marshal& ToMarshal(Marshal&) const override;
  Marshal& FromMarshal(Marshal&) override;
};

} // namespace janus
