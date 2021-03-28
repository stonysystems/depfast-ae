
#include "quorum_event.h"


namespace janus {

history_t QuorumEvent::history{};
count_t QuorumEvent::counts{};
latency_t QuorumEvent::latencies{};
	
	void QuorumEvent::recordHistory(unordered_set<std::string> ip_addrs) {
		auto it = QuorumEvent::history.find(ip_addrs);
		clock_gettime(CLOCK_MONOTONIC, &begin);

		ips_ = ip_addrs;
		if (it == QuorumEvent::history.end()) {
			unordered_map<std::string, int> counts = {};
			unordered_map<std::string, vector<long>> empties = {};
			
			for (std::string ip_addr : ip_addrs) {
				counts.insert(std::make_pair(ip_addr, 0));
				std::vector<long> empty{};
				empties.insert(std::make_pair(ip_addr, empty));
			}

			QuorumEvent::history.insert(std::make_pair(ip_addrs, counts));
			QuorumEvent::counts.insert(std::make_pair(ip_addrs, 0));
			QuorumEvent::latencies.insert(std::make_pair(ip_addrs, empties));
		}
	}

	void QuorumEvent::updateDataStructs(std::string ip_addr) {
		QuorumEvent::history[ips_][ip_addr]++;
		QuorumEvent::counts[ips_]++;

		struct timespec end;
		clock_gettime(CLOCK_MONOTONIC, &end);
		long elapsed_time = (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec;
		QuorumEvent::latencies[ips_][ip_addr].push_back(elapsed_time);
	}
	
	void QuorumEvent::verifyTransient(std::string ip_addr) {
		int slow_nodes = 0;
		int nodes = 0;
		
		if (QuorumEvent::counts[ips_] == 100000) {
			std::vector<long> medians{};
			std::vector<long> p99s{};
			std::vector<std::string> ips{};
							
			int threshold = (int)(QuorumEvent::counts[ips_]/(QuorumEvent::history[ips_].size()));
			for (auto it = QuorumEvent::history[ips_].begin(); it != QuorumEvent::history[ips_].end(); it++) {
				if (it->second < (int)(threshold/100)) {
									
					slow_nodes++;
					//Log_info("Warning: the follower with address %s is slower than usual", it->first.c_str());
					//Log_info("Warning: information is %d and %d", it->first, it->second);
				} else {
					std::vector<long> lats = QuorumEvent::latencies[ips_][it->first];
					std::sort(lats.begin(), lats.end());
					medians.push_back(lats[lats.size()/2.0]);
					p99s.push_back(lats[lats.size()*99/100.0]);
					ips.push_back(it->first);
				}
				it->second = 0;
				QuorumEvent::latencies[ips_][it->first].clear();

				nodes++;
			}
			if (slow_nodes >= (nodes-quorum_)) {
				Log_info("Warning: the replicated system is susceptible to transient performance");
				for (int i = 0; i < medians.size(); i++) {
					Log_info("Warning: transient performance info for %s; avg: %ld and p99: %ld", ips[i].c_str(), medians[i], p99s[i]);
				}
			}

			QuorumEvent::counts[ips_] = 0;
		}
	}

	void QuorumEvent::updateHistory(std::string ip_addr) {
		if (ip_addr != "") {
			auto map = QuorumEvent::history.find(ips_);
			auto count_ = QuorumEvent::counts.find(ips_);

			if (map != QuorumEvent::history.end()) {
				Log_info("ip_addr here: %s", ip_addr.c_str());
				auto ip_count = QuorumEvent::history[ips_].find(ip_addr);
				
				if (ip_count != map->second.end()) {
					if (!IsReady()) {
						updateDataStructs(ip_addr);
						verifyTransient(ip_addr);

					}
				}
			}
		}
	}
} // namespace janus
