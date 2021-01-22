#include "../__dep__.h"
#include "tx.h"
#include "frame.h"
#include "scheduler.h"

namespace janus {

using mdb::Value;

TxCarousel::~TxCarousel() {
}

bool TxCarousel::ReadColumn(mdb::Row *row,
                           mdb::colid_t col_id,
                           Value *value,
                           int hint_flag) {
  // Ignore hint flags.
  auto r = dynamic_cast<mdb::VersionedRow*>(row);
  verify(r->rtti() == symbol_t::ROW_VERSIONED);
  auto c = r->get_column(col_id);
  auto ver_id = r->get_column_ver(col_id);
  row->ref_copy();
  read_vers_[row][col_id] = ver_id;
  *value = c;
  return true;
}

bool TxCarousel::WriteColumn(Row *row,
                            colid_t col_id,
                            const Value &value,
                            int hint_flag) {
  row->ref_copy(); // TODO
  write_bufs_[row][col_id] = value;
  return true;
}

Marshal& TpcPrepareCarouselCommand::ToMarshal(Marshal& m) const {
  m << tx_id_;
  m << ret_;
  m << pending_write_row_map_;
  m << pending_read_row_map_;
  MarshallDeputy md(cmd_);
  m << md;
  return m;
}

Marshal& TpcPrepareCarouselCommand::FromMarshal(Marshal& m) {
  m >> tx_id_;
  m >> ret_;
  m >> pending_write_row_map_;
  m >> pending_read_row_map_;
  MarshallDeputy md;
  m >> md;
  if (!cmd_)
    cmd_ = md.sp_data_;
  else
    verify(0);
  return m;
}



} // namespace janus
