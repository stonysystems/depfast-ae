#pragma once

#include "reactor/event.h"
#include "reactor/reactor.h"
#include <map>

namespace rrr {
class IO {
	public:
		IO() = delete;

		static std::shared_ptr<DiskEvent> write(std::string file, void* ptr, size_t size){
			std::vector<std::map<int, i32>> key_values{};
			std::map<int, i32> curr_map{};
			curr_map[-1] = -1;
			key_values.push_back(curr_map);
			int value = -1;
			auto de = Reactor::CreateSpEvent<DiskEvent>(file, ptr, size, DiskEvent::WRITE_SPEC);
			de->AddToList();
			return de;
		}
	//static rrr::DiskEvent* read(int fd){ return nullptr; }
	//static rrr::DiskEvent* fsync(int fd){ return nullptr; }
};
}

