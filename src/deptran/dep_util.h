#pragma once
#include <iostream>
#include <sstream>


namespace janus {

struct DepId;
enum Direction : bool { IN = true, OUT = false };

void depid_log(const struct DepId& dep_id, enum Direction inout, const std::string &loc,
			   int total, int require);

void depid_login(const struct DepId& dep_id, const std::string &loc="");

void depid_logout(const struct DepId& dep_id, const std::string &loc="",
				  int total=1, int require=1);
}  // namespace janus