#include "exec.h"

namespace janus {


ballot_t FpgaRaftExecutor::Prepare(const ballot_t ballot) {
  verify(0);
  return 0;
}

ballot_t FpgaRaftExecutor::Accept(const ballot_t ballot,
                                    shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t FpgaRaftExecutor::AppendEntries(const ballot_t ballot,
                                         shared_ptr<Marshallable> cmd) {
  verify(0);
  return 0;
}

ballot_t FpgaRaftExecutor::Decide(ballot_t ballot, CmdData& cmd) {
  verify(0);
  return 0;
}

} // namespace janus
