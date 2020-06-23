#pragma once

#include "stdlib.h"
#include "stdio.h"
#include "string.h"
#include <mutex>
#include <sstream>
#include <vector>
#include "sys/times.h"
#include "sys/sysinfo.h"

#ifndef __APPLE__
#include "sys/vtimes.h"
#endif

namespace rrr {

class CPUInfo {
private:
    unsigned long last_bytes_rxed[10], last_bytes_txed[10], last_mem_usage[10];
    clock_t last_ticks_[10], last_user_ticks_[10], last_kernel_ticks_[10];
		double last_cpu, last_txed, last_rxed, last_mem;
		long long total_mem;
		long page_size;
		int index = 0;
    pid_t pid_;
		std::recursive_mutex mtx_;
    //uint32_t num_processors_;
    CPUInfo() {
			const std::lock_guard<std::recursive_mutex> lock (mtx_);
    	struct tms tms_buf;
			double txed, rxed;
			std::vector<double> result;
			struct sysinfo mem_info;

			sysinfo(&mem_info);
			total_mem = mem_info.totalram;
			total_mem *= mem_info.mem_unit;
			total_mem /= 1024;
			Log_info("total amount of ram is: %lld", total_mem);

			page_size = sysconf(_SC_PAGE_SIZE) / 1024;

			last_ticks_[index] = times(&tms_buf);
			last_kernel_ticks_[index] = tms_buf.tms_stime;
			last_user_ticks_[index] = tms_buf.tms_utime;
			
			pid_ = ::getpid();
			result = get_network(std::to_string(pid_), result, last_ticks_[index]);
			result = get_memory(std::to_string(pid_), result, last_ticks_[index]);

			index++;
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
			const std::lock_guard<std::recursive_mutex> lock (mtx_);

      struct tms tms_buf;
      clock_t ticks;
			std::vector<double> result;
			unsigned long txed, rxed;
      double cpu_total, tx_total, rx_total;
      clock_t last_ticks;
			
			ticks = times(&tms_buf);
			if(index < 10) last_ticks = last_ticks_[index-1];
			else last_ticks = last_ticks_[9];

      Log_info("ticks: %d -> %d", last_ticks, ticks);
      if (ticks <= last_ticks + 10/* || num_processors_ <= 0*/){
				if(index < 10){
					return {-1.0, -1.0, -1.0, -1.0};
				} else{
					return {last_cpu, last_txed, last_rxed, last_mem};
				}
			}
			
			if(index < 10){
				last_kernel_ticks_[index] = tms_buf.tms_stime;
				last_user_ticks_[index] = tms_buf.tms_utime;
				last_ticks_[index] = ticks;
				index++;
			} else{
				for(int i = 0; i < 9; i++){
					last_kernel_ticks_[i] = last_kernel_ticks_[i+1];
					last_user_ticks_[i] = last_user_ticks_[i+1];
					last_ticks_[i] = last_ticks_[i+1];
				}
				last_kernel_ticks_[9] = tms_buf.tms_stime;
				last_user_ticks_[9] = tms_buf.tms_utime;
				last_ticks_[9] = ticks;
			}

      cpu_total = (tms_buf.tms_stime - last_kernel_ticks_[0]) +
            (tms_buf.tms_utime - last_user_ticks_[0]);
      cpu_total /= (ticks - last_ticks_[0]);
			last_cpu = cpu_total;
        //ret /= num_processors_;
	
			result.push_back(cpu_total);
			result = get_network(std::to_string(pid_), result, ticks);
			result = get_memory(std::to_string(pid_), result, ticks);
    	return result;
    }

		std::vector<double> get_network(std::string pid, std::vector<double> result, clock_t ticks){
			double tx_total, rx_total;
			std::string line;
			std::string temp;
			unsigned long txed, rxed;
			std::ifstream netfile("/proc/"+pid+"/net/dev");

			for(int i = 0; i < 4; i++){
				getline(netfile, line);
			}
			
			int i = 1;
			char* token = strtok(&line[0], " ");
			while(token != NULL){
				token = strtok(NULL, " ");
				if(i == 1){
					txed = strtoul(token, NULL, 0);
				}
				if(i == 9){
					rxed = strtoul(token, NULL, 0);
					break;
				}
				i++;
			}
			
			if(index < 10) {
				last_bytes_txed[index] = txed;
				last_bytes_rxed[index] = rxed;
			} else{
				for(int i = 0; i < 9; i++){
					last_bytes_txed[i] = last_bytes_txed[i+1];
					last_bytes_rxed[i] = last_bytes_rxed[i+1];
				}
				last_bytes_txed[9] = txed;
				last_bytes_rxed[9] = rxed;
			}

			if(ticks != last_ticks_[0]){
				tx_total = (txed-last_bytes_txed[0])/(ticks - last_ticks_[0]);
				rx_total = (rxed-last_bytes_rxed[0])/(ticks - last_ticks_[0]);
			}
			
			result.push_back(tx_total);
			result.push_back(rx_total);

			last_txed = tx_total;
			last_rxed = rx_total;
			return result;
		}

		std::vector<double> get_memory(std::string pid, std::vector<double> result, clock_t ticks){
			long rss;
			double mem_usage, mem_total;
			std::string ignore;

			std::ifstream stat_file("/proc/"+pid+"/stat", std::ios_base::in);
			
			stat_file >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
							  >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore
								>> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> ignore >> rss;
			
			mem_usage = rss * page_size;
			
			if(index < 10) {
				last_mem_usage[index] = mem_usage;
			} else{
				for(int i = 0; i < 9; i++){
					last_mem_usage[i] = last_mem_usage[i+1];
				}
				last_mem_usage[9] = mem_usage;
			}

			if(ticks != last_ticks_[0]){
				mem_total = (mem_usage - last_mem_usage[0])/(ticks - last_ticks_[0]);
			}

			result.push_back(mem_total);

			last_mem = mem_total;
			return result;
		}

public:
    static std::vector<double> cpu_stat() {
        static CPUInfo cpu_info;
        return cpu_info.get_cpu_stat();
    }
};

}
