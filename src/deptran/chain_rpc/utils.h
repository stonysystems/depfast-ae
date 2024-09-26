#ifndef CHAINRPC_UTILS_H
#define CHAINRPC_UTILS_H

#include "__dep__.h"
#include "marshallable.h"
#include <random>
#include <sstream>
#include <iomanip>

namespace janus {
    static std::string generateUUID4() {
        std::random_device rd;
        std::mt19937 generator(rd());
        std::uniform_int_distribution<uint32_t> dist(0, 0xFFFFFFFF);

        std::stringstream uuid;

        // 8-4-4-4-12 format for UUID4
        uuid << std::hex << std::setfill('0');

        // 8 hex digits
        uuid << std::setw(8) << dist(generator) << "-";

        // 4 hex digits (first digit is a version identifier, UUID4 uses '4')
        uuid << std::setw(4) << ((dist(generator) & 0x0FFF) | 0x4000) << "-";

        // 4 hex digits (first two bits are for the variant, UUID4 uses '8', '9', 'A', or 'B')
        uuid << std::setw(4) << ((dist(generator) & 0x3FFF) | 0x8000) << "-";

        // 4 hex digits
        uuid << std::setw(4) << dist(generator) << "-";

        // 12 hex digits
        uuid << std::setw(12) << dist(generator) << dist(generator);

        return uuid.str();
    }

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
        // int is the loc_id.
        std::vector<int> path_; 
        // Debugging purpose, a random string to identify the control unit.
        std::string uuid_;

        ControlUnit() : Marshallable(MarshallDeputy::CONTROL_UNIT_CHAIN_RPC) {
            total_partitions_ = 0;
            acc_ack_ = 0;
            acc_rej_ = 0;
            toIndex_ = 0;
            uuid_ = generateUUID4();
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
            m << uuid_.c_str();
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
            m >> uuid_;
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
