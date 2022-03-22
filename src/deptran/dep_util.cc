#include "__dep__.h"
#include "marshallable.h"
#include "rcc/tx.h"
#include "dep_util.h"
#include "rcc_rpc.h"

namespace janus {
void depid_log(const struct DepId& dep_id, enum Direction inout, const std::string &loc,
			   int total, int require) {
	std::stringstream msg;
	msg << '(' << loc << ')'; 
	if (inout == IN)
		msg << "[IN]|";
	else if (inout == OUT)
		msg << "[OUT]|";
	else
		msg << "[]|";

	msg << dep_id.str << "|";
	msg << require << "/" << total << "|";
	msg << "0x" << std::hex << dep_id.id;
	msg << std::endl;

	std::cout << msg.str();
}

void depid_login(const struct DepId& dep_id, const std::string &loc) {
	depid_log(dep_id, Direction::IN, loc, 0, 0);
}

void depid_logout(const struct DepId& dep_id, const std::string &loc,
			   int total, int require) {
	depid_log(dep_id, Direction::OUT, loc, total, require);
}
}  // namespace janus
