#pragma once

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include <sstream>
#include <vector>
#include "sys/times.h"

#ifndef __APPLE__
#include "sys/vtimes.h"
#endif

namespace rrr {

class CPUInfo {
private:
    clock_t last_ticks_, last_user_ticks_, last_kernel_ticks_;
    //uint32_t num_processors_;
    CPUInfo() {
        //FILE* proc_cpuinfo;
        struct tms tms_buf;
        //char line[1024];

        last_ticks_ = times(&tms_buf);
        last_user_ticks_ = tms_buf.tms_utime;
        last_kernel_ticks_ = tms_buf.tms_stime;

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
    std::vector<double> get_cpu_stat() {
        struct tms tms_buf;
        clock_t ticks;
	std::vector<double> result;
        double cpu_total, tx_total, rx_total;

        ticks = times(&tms_buf);
        Log_info("ticks: %d -> %d", last_ticks_, ticks);
        if (ticks <= last_ticks_ + 1000/* || num_processors_ <= 0*/)
            return {-1.0, -1.0, -1.0};

        //ret = (tms_buf.tms_stime - last_kernel_ticks_) +
        //    (tms_buf.tms_utime - last_user_ticks_);
        //ret /= (ticks - last_ticks_);
        //ret /= num_processors_;

	popen("cpu.sh", "r");
	//Log_info("status: %d", status);
	std::ifstream cpufile("temp.txt");
	std::string value, pid, cpu, line, rx, tx;
	while(std::getline(cpufile, value, '\n')){
	    pid = value.substr(0, value.find(" "));
	    cpu = value.substr(value.find(" "));
	    Log_info("PID: %s and CPU: %s", pid, cpu);
	    cpu_total += strtod(cpu.c_str(), NULL);
	    
	    std::string command("/bin/cat /proc/"+pid+"/net/dev | /bin/grep ens4 | /usr/bin/awk '{print $2, $10}; > temp.txt");
	    std::system(command.c_str());
	    std::ifstream netfile("temp.txt");
	    
	    std::string line;
	    std::getline(netfile, line);
	    tx = value.substr(0, line.find(" "));
	    rx = value.substr(line.find(" "));
	    

	    netfile.close();
	    tx_total += strtod(tx.c_str(), NULL);
	    rx_total += strtod(rx.c_str(), NULL);
	}
	cpufile.close();

	result.push_back(cpu_total);
	result.push_back(tx_total);
	result.push_back(rx_total);
	
        last_ticks_ = ticks;
        //last_user_ticks_ = tms_buf.tms_utime;
        //last_kernel_ticks_ = tms_buf.tms_stime;
	
        return result;
    }

public:
    static std::vector<double> cpu_stat() {
        static CPUInfo cpu_info;
        return cpu_info.get_cpu_stat();
    }
};

}
