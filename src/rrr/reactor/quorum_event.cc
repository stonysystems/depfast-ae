
#include "quorum_event.h"


namespace janus {

history_t QuorumEvent::history{};
times_t QuorumEvent::times{};
count_t QuorumEvent::counts{};
count_t QuorumEvent::counts_two{};
latency_t QuorumEvent::latencies{};
uint64_t QuorumEvent::count = 0;
	
	void QuorumEvent::recordHistory(unordered_set<std::string> ip_addrs) {
		auto it = QuorumEvent::history.find(ip_addrs);
		begin_time = Time::now(true);

		ips_ = ip_addrs;
		if (it == QuorumEvent::history.end()) {
			unordered_map<std::string, int> counts = {};
			unordered_map<std::string, vector<int>> empties = {};
			
			for (std::string ip_addr : ip_addrs) {
				counts.insert(std::make_pair(ip_addr, 0));
				std::vector<int> empty{};
				empties.insert(std::make_pair(ip_addr, empty));
			}

			QuorumEvent::history.insert(std::make_pair(ip_addrs, counts));
			QuorumEvent::counts.insert(std::make_pair(ip_addrs, 0));
			QuorumEvent::times.insert(std::make_pair(ip_addrs, Time::now()));
			QuorumEvent::latencies.insert(std::make_pair(ip_addrs, empties));
		}
	}

	void QuorumEvent::updateDataStructs(std::string ip_addr) {
		QuorumEvent::history[ips_][ip_addr]++;
		QuorumEvent::counts[ips_]++;

		//time in us
		int elapsed_time = Time::now(true) - begin_time;
		QuorumEvent::latencies[ips_][ip_addr].push_back(elapsed_time);
	}
	
	void QuorumEvent::verifyTransient(std::string ip_addr) {
		int slow_nodes = 0;
		int nodes = 0;

		int curr_time = Time::now();
		int elapsed_time_us = curr_time - times[ips_];

		if (elapsed_time_us >= PRINT_INTERVAL_US) {
			std::vector<long> medians{};
			std::vector<long> p99s{};
			std::vector<long> p99_9s{};
			std::vector<std::string> ips{};
							
			int threshold = (int)(QuorumEvent::counts[ips_]/(QuorumEvent::history[ips_].size()));
			for (auto it = QuorumEvent::history[ips_].begin(); it != QuorumEvent::history[ips_].end(); it++) {
				if (it->second < (int)(threshold/100)) {
									
					slow_nodes++;
					//Log_info("Warning: the follower with address %s is slower than usual", it->first.c_str());
					//Log_info("Warning: information is %d and %d", it->second, QuorumEvent::counts[ips_]);
				} else {
					std::vector<int> lats = QuorumEvent::latencies[ips_][it->first];
					std::sort(lats.begin(), lats.end());
					medians.push_back(lats[lats.size()/2.0]);
					p99s.push_back(lats[lats.size()*99/100.0]);
					p99_9s.push_back(lats[lats.size()*99.9/100.0]);
					ips.push_back(it->first);
				}
				it->second = 0;
				QuorumEvent::latencies[ips_][it->first].clear();

				nodes++;
			}
			if (slow_nodes >= (nodes-quorum_)) {
				Log_info("Warning: the replicated system is susceptible to transient performance");
				for (int i = 0; i < medians.size(); i++) {
					Log_info("Warning: transient performance info for %s; avg: %d, p99: %d, and p99.9: %d", ips[i].c_str(), medians[i], p99s[i], p99_9s[i]);
				}
			}

			curr_time = Time::now();
			QuorumEvent::counts[ips_] = 0;
			QuorumEvent::times[ips_] = curr_time;
		}
	}

	void QuorumEvent::updateHistory(std::string ip_addr) {
		if (ip_addr != "") {
			auto map = QuorumEvent::history.find(ips_);

			if (map != QuorumEvent::history.end()) {
				//Log_info("ip_addr here: %s", ip_addr.c_str());
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
	long QuorumEvent::MemoryUtil() {
		long page_size = sysconf(_SC_PAGE_SIZE) / 1024;
		long rss;
		std::string ignore;

		pid_t pid_ = ::getpid();
		std::string pid = std::to_string(pid_);
		std::ifstream stat_file("/proc/"+pid+"/stat", std::ios_base::in);

		stat_file >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
							>> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
							>> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> rss;

		return rss * page_size;
	}

	void QuorumEvent::Finalize(int timeout, int flag) {
		CalledFinalize();

		if (QuorumEvent::counts_two[ips_] == FINALIZE_THRESHOLD) {
			QuorumEvent::counts_two[ips_] = 0;
			finalize_event->Wait(timeout);
		} else {
			QuorumEvent::counts_two[ips_]++;
		}

		if (finalize_event->status_ == TIMEOUT) {
			if (flag == TimeoutFlag::FLAG_FREE) {
				for (auto it = changing_ips_.begin(); it != changing_ips_.end(); it++) {
					//long mem_before = MemoryUtil();
					FreeDangling(*it);
					//long mem_after = MemoryUtil();
					Log_info("finalizing timeout");
				}			
			}
		}

		changing_ips_.clear();
	}

} // namespace janus
