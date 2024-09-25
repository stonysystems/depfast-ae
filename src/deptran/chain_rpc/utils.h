#ifndef CHAINRPC_UTILS_H
#define CHAINRPC_UTILS_H

#include "__dep__.h"
#include "marshallable.h"

namespace janus {
    // In our implementation, we use TxData::ToMarshal to encode/decode the command data.
    // To support ChainRPC, we should have an additional data structure to carry the control unit.
    class ControlUnit : public Marshallable {
    public:
        // Necessary information to decode and encode in the control Unit.
        int total_partitions_;
        int acc_ack_;
        int acc_rej_;
        // Path for the current request.
        std::vector<int> path_; // int is the loc_id
        // The current path index.
        int path_index_;

        ControlUnit() : Marshallable(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC) {
            total_partitions_ = 0;
            acc_ack_ = 0;
            acc_rej_ = 0;
        }
        virtual ~ControlUnit() { }
        Marshal& ToMarshal(Marshal&m) const {
            m << total_partitions_;
            return m;
        };

        Marshal& FromMarshal(Marshal&m) {
            m >> total_partitions_;
            return m;
        };
        void SetTotalPartitions(int n) {
            total_partitions_ = n;
        }

        bool RegisterEarlyTerminate() {
            return acc_ack_ > 0.5 * total_partitions_ || acc_rej_ > 0.5 * total_partitions_;
        }
    };
}

#endif
