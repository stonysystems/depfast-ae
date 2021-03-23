#include "sys/times.h"
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <cstring>
#include "coroutine.h"
#include <sys/resource.h>

int main (int argc, char* argv[]) {
	struct tms tms_buf;

	std::cout << CLOCKS_PER_SEC << std::endl;
	int j = 0;
	for (int i = 0; i < 150000; i++) {
		struct timespec begin, end;
		struct timespec begin_cpu, end_cpu;
		long begin_scpu, end_scpu;
		std::shared_ptr<Coroutine> sp_coro = std::make_shared<Coroutine>([]() {
			int sum = 0;
			for (int i = 0; i < 1; i++) {
				int* arr = new int[10000];
				int* arr2 = new int[10000];
				int* arr3 = new int[10000];
				int* arr4 = new int[10000];
				memset(arr, 2, 10000*sizeof(int));
				memset(arr2, 3, 10000*sizeof(int));
				for (int j = 0; j < 10000; j++) {
					arr[j] += arr2[j];
				}
			}
		});
		
		if (i != -1) {
			clock_gettime(CLOCK_MONOTONIC_RAW, &begin);
			clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &begin_cpu);
			
			struct rusage ru1;
			getrusage(RUSAGE_SELF, &ru1);
			begin_scpu = (ru1.ru_stime.tv_sec)*1000000000 + (ru1.ru_stime.tv_usec)*1000;
			sp_coro->Run();
			//sp_coro->Continue();
			/*for (int i = 0; i < 1; i++) {
				int* arr = new int[10000];
				int* arr2 = new int[10000];
				int* arr3 = new int[10000];
				int* arr4 = new int[10000];
				int* arr5 = new int[10000];
				int* arr6 = new int[10000];
				int* arr7 = new int[10000];
				int* arr8 = new int[10000];
				int* arr9 = new int[10000];
				int* arr10 = new int[10000];
				memset(arr, 2, 10000*sizeof(int));
				memset(arr2, 3, 10000*sizeof(int));
				memset(arr3, 4, 10000*sizeof(int));
				memset(arr4, 5, 10000*sizeof(int));
				memset(arr5, 5, 10000*sizeof(int));
				memset(arr6, 5, 10000*sizeof(int));
				memset(arr7, 5, 10000*sizeof(int));
				memset(arr8, 5, 10000*sizeof(int));
				memset(arr9, 5, 10000*sizeof(int));
				memset(arr10, 5, 10000*sizeof(int));
				for (int j = 0; j < 10000; j++) {
					arr[j] += arr2[j];
					//std::string s = "sdfsdafasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfasdfadsfasdfsdfasdfasdf";
					//::write(fd, s.c_str(), s.length());
				}
			}*/

			struct rusage ru2;
			getrusage(RUSAGE_SELF, &ru2);
			end_scpu = (ru2.ru_stime.tv_sec)*1000000000 + (ru2.ru_stime.tv_usec)*1000;

			clock_gettime(CLOCK_MONOTONIC_RAW, &end);
			clock_gettime(CLOCK_PROCESS_CPUTIME_ID, &end_cpu);

			long cpu_time = (end_cpu.tv_sec - begin_cpu.tv_sec)*1000000000 + end_cpu.tv_nsec - begin_cpu.tv_nsec;
			cpu_time += end_scpu - begin_scpu;
			long total_time = (end.tv_sec - begin.tv_sec)*1000000000 + end.tv_nsec - begin.tv_nsec;
			//if ((double) cpu_time/total_time < 2.0) {
				std::cout << (double) cpu_time/total_time << std::endl;

			//}
		}
	}
}
