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
    unsigned long last_bytes_rxed, last_bytes_txed;
    clock_t last_ticks_, last_user_ticks_, last_kernel_ticks_;
    pid_t pid_;
    //uint32_t num_processors_;
    CPUInfo() {
        //FILE* proc_cpuinfo;
        struct tms tms_buf;
        //char line[1024];
	double txed, rxed;

	std::string line;
	std::string temp;
	std::ifstream netfile("/proc/"+std::to_string(pid_)+"/net/dev");
	for(int i = 0; i < 4; i++){
	  getline(netfile, line);
	}
	
	int i = 1;
	char* token = strtok(&line[0], " ");
	while(token != NULL){
	 token = strtok(NULL, " ");
	 Log_info("token here: %s", token);
	 if(i == 1){
	   txed = strtof(token, NULL);
	   //Log_info("TX total: %d", tx_total);
	 }
	 if(i == 9){
	   rxed = strtof(token, NULL);
	 }
	 i++;
	
	}

	last_bytes_rxed = rxed;
	last_bytes_txed = txed;
        last_ticks_ = times(&tms_buf);
        last_user_ticks_ = tms_buf.tms_utime;
        last_kernel_ticks_ = tms_buf.tms_stime;

	pid_ = ::getpid();
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
	unsigned long txed, rxed;
        double cpu_total, tx_total, rx_total;

        ticks = times(&tms_buf);
        Log_info("ticks: %d -> %d", last_ticks_, ticks);
        if (ticks <= last_ticks_ + 1000/* || num_processors_ <= 0*/)
            return {-1.0, -1.0, -1.0};

        cpu_total = (tms_buf.tms_stime - last_kernel_ticks_) +
            (tms_buf.tms_utime - last_user_ticks_);
        //ret /= (ticks - last_ticks_);
        //ret /= num_processors_;
	
	std::string line;
	std::string temp;
	std::ifstream netfile("/proc/"+std::to_string(pid_)+"/net/dev");
	for(int i = 0; i < 4; i++){
	  getline(netfile, line);
	}
	
	int i = 1;
	char* token = strtok(&line[0], " ");
	while(token != NULL){
	 token = strtok(NULL, " ");
	 Log_info("token here: %s", token);
	 if(i == 1){
	   txed = strtoul(token, NULL, 0);
	   //Log_info("TX total: %d", tx_total);
	 }
	 if(i == 9){
	   rxed = strtoul(token, NULL, 0);
	 }
	 i++;
	}

	std::ifstream iofile("/proc/"+std::to_string(pid_)+"/io");
	
	tx_total = (txed-last_bytes_txed)/(ticks - last_ticks_);
	rx_total = (rxed-last_bytes_rxed)/(ticks - last_ticks_);
	result.push_back(cpu_total);
	result.push_back(tx_total);
	result.push_back(rx_total);
	
        last_ticks_ = ticks;
        last_user_ticks_ = tms_buf.tms_utime;
        last_kernel_ticks_ = tms_buf.tms_stime;
	last_bytes_txed = txed;
	last_bytes_rxed = rxed;

        return result;
    }

public:
    static std::vector<double> cpu_stat() {
        static CPUInfo cpu_info;
        return cpu_info.get_cpu_stat();
    }
};

}
