#pragma once

#include "__dep__.h"
#include "marshallable.h"

namespace janus {

    // TODO: this a placeholder for the control unit, we will carry this data over the networking.
    
    // In our implementation, we use TxData to encode/decode the command data.
    // To support ChainRPC, we should have an additional data structure to carry the control unit.
    class ControlUnit : public Marshallable {
    private:
        // Necessary information to decode and encode in the control Unit.
        int total_partitions_;
        int acc_ack_;
        int acc_rej_;
        // Path for the current request.
        std::vector<int> path_; // int is the loc_id
        // The current path index.
        int path_index_;

        ControlUnit() : Marshallable(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC) {}
        virtual ~ControlUnit() { }
        virtual Marshal& ToMarshal(Marshal&) const override;
        virtual Marshal& FromMarshal(Marshal&) override;
    public:
        void SetTotalPartitions(int n) {
            total_partitions_ = n;
        }

        bool RegisterEarlyTerminate() {
            return acc_ack_ > 0.5 * total_partitions_ || acc_rej_ > 0.5 * total_partitions_;
        }
    };

    ControlUnit::ControlUnit() : Marshallable(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC) {
        total_partitions_ = 0;
        acc_ack_ = 0;
        acc_rej_ = 0;
    };

    Marshal& ControlUnit::ToMarshal(Marshal& m) const {
        m << total_partitions_;
        return m;
    };

    Marshal& ControlUnit::FromMarshal(Marshal& m) {
        m >> total_partitions_;
        return m;
    };
}