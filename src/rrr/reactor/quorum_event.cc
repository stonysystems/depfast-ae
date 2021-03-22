
#include "quorum_event.h"


namespace janus {

history_t QuorumEvent::history{};
count_t QuorumEvent::counts{};
	
	void QuorumEvent::recordHistory(unordered_set<std::string> ip_addrs) {
		auto it = QuorumEvent::history.find(ip_addrs);

		ips_ = ip_addrs;
		if (it == QuorumEvent::history.end()) {
			unordered_map<std::string, int> counts = {};
			for (std::string ip_addr : ip_addrs) {
				counts.insert(std::make_pair(ip_addr, 0));
			}

			QuorumEvent::history.insert(std::make_pair(ip_addrs, counts));
			QuorumEvent::counts.insert(std::make_pair(ip_addrs, 0));
		}
	}

	void QuorumEvent::updateHistory(std::string ip_addr) {
		if (ip_addr != "") {
			auto map = QuorumEvent::history.find(ips_);
			auto count_ = QuorumEvent::counts.find(ips_);

			if (map != QuorumEvent::history.end()) {
				auto ip_count = QuorumEvent::history[ips_].find(ip_addr);
				
				if (ip_count != map->second.end()) {
					
					if (!IsReady()) {
						QuorumEvent::history[ips_][ip_addr]++;
						QuorumEvent::counts[ips_]++;
						
						int slow_nodes = 0;
						int nodes = 0;
						if (QuorumEvent::counts[ips_] == 50000) {
							int threshold = (int)(QuorumEvent::counts[ips_]/(QuorumEvent::history[ips_].size()));
							for (auto it = QuorumEvent::history[ips_].begin(); it != QuorumEvent::history[ips_].end(); it++) {
								if (it->second < (int)(threshold/5)) {
									slow_nodes++;
									//Log_info("Warning: the follower with address %s is slower than usual", it->first.c_str());
									//Log_info("Warning: information is %d and %d", n_total_, quorum_);
								}
								nodes++;
								it->second = 0;
							}
							if (slow_nodes >= (nodes-quorum_)) {
									Log_info("Warning: the replicated system is susceptible to transient performance");
							}

							QuorumEvent::counts[ips_] = 0;
						}
					}

				}
			}
		}
	}
} // namespace janus
