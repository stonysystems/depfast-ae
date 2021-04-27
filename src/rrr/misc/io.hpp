#pragma once

#include "reactor/event.h"
#include "reactor/reactor.h"
#include <map>

namespace rrr {
class IO {
	public:
		IO() = delete;

		static std::shared_ptr<DiskEvent> write(std::string file, void* ptr, size_t size, size_t count){
			auto de = Reactor::CreateSpEvent<DiskEvent>(file, ptr, size, count, DiskEvent::WRITE_SPEC | DiskEvent::FSYNC);
			de->AddToList();
			return de;
		}
		static std::shared_ptr<DiskEvent> read(std::string file, void* ptr, size_t size, size_t count){
			auto de = Reactor::CreateSpEvent<DiskEvent>(file, ptr, size, count, DiskEvent::READ);
			de->AddToList();
			return de;
		}
		static std::shared_ptr<DiskEvent> fsync(std::string file){
			auto de = Reactor::CreateSpEvent<DiskEvent>(file, nullptr, 0, 0, DiskEvent::FSYNC);
			de->AddToList();
			return de;
		}
		static std::shared_ptr<DiskEvent> write(std::function<void()> f){
			auto de = Reactor::CreateSpEvent<DiskEvent>(f);
			de->AddToList();
			return de;
		}

};
}

