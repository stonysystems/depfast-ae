#include "tpc_command.h"
#include "../command.h"
#include "../command_marshaler.h"

using namespace janus;

static int volatile x1 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_PREPARE,
                                     [] () -> Marshallable* {
                                       return new TpcPrepareCommand;
                                     });

static int volatile x2 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_COMMIT,
                                     [] () -> Marshallable* {
                                       return new TpcCommitCommand;
                                     });

static int volatile x3 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_EMPTY,
                                     [] () -> Marshallable* {
                                       return new TpcEmptyCommand;
                                     });

static int volatile x4 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_NOOP,
                                     [] () -> Marshallable* {
                                       return new TpcNoopCommand;
                                     });

static int volatile x5 =
    MarshallDeputy::RegInitializer(MarshallDeputy::CMD_TPC_BATCH,
                                      [] () -> Marshallable* {
                                       return new TpcBatchCommand;
                                     });


Marshal& TpcPrepareCommand::ToMarshal(Marshal& m) const {
  m << tx_id_;
  m << ret_;
//  m << (int32_t) cmd_.size();
//  for (auto o : cmd_) {
//    m << *o;
//  }
  MarshallDeputy md(cmd_);
  m << md;
  return m;
}

Marshal& TpcPrepareCommand::FromMarshal(Marshal& m) {
  m >> tx_id_;
  m >> ret_;
//  int32_t sz;
//  m >> sz;
//  verify(cmd_.empty());
//  for (int i = 0; i < sz; i++) {
//    auto o = make_shared<SimpleCommand>();
//    m >> *o;
//    cmd_.push_back(o);
//  }
  MarshallDeputy md;
  m >> md;
  verify(md.sp_data_!=NULL) ;
  if (!cmd_)
    cmd_ = md.sp_data_;
  else
    verify(0);
  return m;
}

Marshal& TpcCommitCommand::ToMarshal(Marshal& m) const {
  m << tx_id_;
  m << ret_;
  MarshallDeputy md(cmd_);
  m << md;
  return m;
}

Marshal& TpcCommitCommand::FromMarshal(Marshal& m) {
  m >> tx_id_;
  m >> ret_;
  MarshallDeputy md;
  m >> md;
  if (!cmd_)
    cmd_ = md.sp_data_;
  else
    verify(0);
  return m;
}

Marshal& TpcEmptyCommand::ToMarshal(Marshal& m) const {
  return m;
}

Marshal& TpcEmptyCommand::FromMarshal(Marshal& m) {
  return m;
}

Marshal& TpcNoopCommand::ToMarshal(Marshal& m) const {
  return m;
}

Marshal& TpcNoopCommand::FromMarshal(Marshal& m) {
  return m;
}

Marshal& TpcBatchCommand::ToMarshal(Marshal& m) const {
  verify(size_ == cmds_.size());
  m << size_;
  for (auto it = cmds_.begin(); it != cmds_.end(); ++it) {
    (*it)->ToMarshal(m);
  }
  return m;
}

Marshal& TpcBatchCommand::FromMarshal(Marshal& m) {
  m >> size_;
  for (uint32_t i = 0; i < size_; i++) {
    cmds_.emplace_back(std::make_shared<TpcCommitCommand>());
    cmds_[i]->FromMarshal(m);
  }
  return m;
}

void TpcBatchCommand::AddCmd(shared_ptr<TpcCommitCommand> cmd) {
  size_++;
  cmds_.push_back(cmd);
}

void TpcBatchCommand::AddCmds(vector<shared_ptr<TpcCommitCommand> >& cmds) {
  cmds_ = cmds;
  size_ = cmds_.size();
}

void TpcBatchCommand::ClearCmd() {
  cmds_.clear();
  size_ = 0;
}
