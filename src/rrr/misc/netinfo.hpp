#pragma once

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include "sys/times.h"
//#include <fstream>

#ifndef __APPLE__
#include "sys/vtimes.h"
#endif

namespace rrr {

class NetInfo {
private:
    clock_t last_ticks_;
    unsigned long last_bytes_rxed, last_bytes_txed;
    //uint32_t num_processors_;
    NetInfo() {
        struct tms tms_buf;
        //char line[1024];

        last_ticks_ = times(&tms_buf);

	std::string line1;
	std::ifstream rxfile("/sys/class/net/ens4/statistics/rx_bytes");
        getline(rxfile, line1);
        unsigned long rxed = strtoul(line1.c_str(), NULL, 0);
        rxfile.close();

	std::string line2;
	std::ifstream txfile("/sys/class/net/ens4/statistics/tx_bytes");
	getline(txfile, line2);
        unsigned long txed = strtoul(line2.c_str(), NULL, 0);
	txfile.close();

        last_bytes_rxed = rxed;
        last_bytes_txed = txed;

        //proc_cpuinfo = fopen("/proc/cpuinfo", "r");
        //num_processors_ = 0;
        //while (fgets(line, 1024, proc_cpuinfo) != NULL)
        //    if (strncmp(line, "processor", 9) == 0)
        //        num_processors_++;
        //fclose(proc_cpuinfo);
    }

    /**
     * get current cpu utilization for the calling process
     * result is calculated as (sys_time + user_time) / total_time, since the
     * last recent call to this function
     *
     * return:  upon success, this function will return the utilization roughly
     *          between 0.0 to 1.0; otherwise, it will return a negative value
     */
    double get_net_stat() {
        struct tms tms_buf;
        clock_t ticks;
        double ret;

        ticks = times(&tms_buf);
        //Log_info("ticks: %d -> %d", last_ticks_, ticks);
        if (ticks <= last_ticks_ + 1000000/* || num_processors_ <= 0*/)
            return -1.0;

	std::string line1;
	std::ifstream rxfile("/sys/class/net/ens4/statistics/rx_bytes");
        getline(rxfile, line1);
        unsigned long rxed = strtoul(line1.c_str(), NULL, 0);
	Log_info("rxed: %d -> %d", last_bytes_rxed, rxed);
        rxfile.close();

	std::string line2;
	std::ifstream txfile("/sys/class/net/ens4/statistics/tx_bytes");
	getline(txfile, line2);
        unsigned long txed = strtoul(line2.c_str(), NULL, 0);
	txfile.close();

	ret += (txed - last_bytes_txed) + (rxed - last_bytes_rxed);
	Log_info("what is ret before div? %f", ret);
        ret /= (ticks - last_ticks_);
	Log_info("what is ret? %f", ret);
        //ret /= num_processors_;

        last_ticks_ = ticks;
        last_bytes_rxed = rxed;
        last_bytes_txed = txed;

        return ret;
    }

public:
    static double net_stat() {
        static NetInfo net_info;
        return net_info.get_net_stat();
    }
};

}
