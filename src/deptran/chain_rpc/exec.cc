#include "exec.h"

namespace janus {


ballot_t ChainRPCExecutor::Prepare(const ballot_t ballot) {
  verify(0);
  return 0;
}

ballot_t ChainRPCExecutor::Accept(const ballot_t ballot,
                                    shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t ChainRPCExecutor::AppendEntries(const ballot_t ballot,
                                         shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t ChainRPCExecutor::Decide(ballot_t ballot, CmdData& cmd) {
  verify(0);
  return 0;
}

} // namespace janus
