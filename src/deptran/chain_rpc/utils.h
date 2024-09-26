#ifndef CHAINRPC_UTILS_H
#define CHAINRPC_UTILS_H

#include "__dep__.h"
#include "marshallable.h"

namespace janus {
    // In our implementation, we use TxData::ToMarshal to encode/decode the command data.
    // To support ChainRPC, we should have an additional data structure to carry the control unit.
    class ControlUnit : public Marshallable {
    private:
        // The index node in the curret path.
        int toIndex_;
    public:
        // Necessary information to decode and encode in the control Unit.
        int total_partitions_;
        int acc_ack_;
        int acc_rej_;
        // Path for the current request.
        // If total_partitions_ is 3, then path_ can be [0, 1, 2, 0] or [0, 2, 1, 0]
        std::vector<int> path_; // int is the loc_id

        ControlUnit() : Marshallable(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC) {
            total_partitions_ = 0;
            acc_ack_ = 0;
            acc_rej_ = 0;
            toIndex_ = 0;
        }
        virtual ~ControlUnit() { }
        Marshal& ToMarshal(Marshal&m) const {
            m << total_partitions_;
            m << acc_ack_;
            m << acc_rej_;
            m << (int32_t) path_.size();
            for (auto p : path_) {
                m << p;
            }
            return m;
        };

        Marshal& FromMarshal(Marshal&m) {
            m >> total_partitions_;
            m >> acc_ack_;
            m >> acc_rej_;
            int32_t sz;
            m >> sz;
            for (int i = 0; i < sz; i++) {
                int p;
                m >> p;
                path_.push_back(p);
            }
            return m;
        };

        void SetTotalPartitions(int n) {
            total_partitions_ = n;
        }

        bool RegisterEarlyTerminate() {
            return acc_ack_ > 0.5 * total_partitions_ 
                    || acc_rej_ > 0.5 * total_partitions_
                    || toIndex_ == path_.size() - 1;
        }

        // If we can terminate earlier, return back to the leader immediately, otherwise forward to the next server.
        int GetNextHopWithUpdate() {
            toIndex_++;
            return RegisterEarlyTerminate()? 0 : path_[toIndex_];
        }
    };
}

#endif
