#include <iostream>
#include <assert.h>
#include<cstring>
#include <stdio.h>
#include <thread>
#include <vector>
#include <iostream>
#include <queue>
#include <sstream>
#include <iostream>
#include <sstream>
#include <fstream>
#include <string>
#include <iomanip>
#include <exception>
#include <fstream>
#include <thread>
#include <vector>
#include <iostream>
#include <thread>
#include <mutex>
#include <functional>
#include <stdlib.h>
#include <unistd.h>
#include <getopt.h>
#include <condition_variable>
#include <glob.h>
#include <sys/time.h>
#include "deptran/s_main.h"

int main(int argc, char **argv){
  vector<std::string> paxos_config{"config/1c1s1p.yml", "config/occ_paxos.yml"};
  int k = 0, nthreads = 1, logs_to_commit = 1500000;

  char *argv_paxos[16];
  argv_paxos[0] = "third-party/paxos/build/microbench";
  argv_paxos[1] = "-b";
  argv_paxos[2] = "-d";
  argv_paxos[3] = "60";
  argv_paxos[4] = "-f";
  argv_paxos[5] = (char *) paxos_config[0].c_str();
  argv_paxos[6] = "-f";
  argv_paxos[7] = (char *) paxos_config[1].c_str();
  argv_paxos[8] = "-t";
  argv_paxos[9] = "30";
  argv_paxos[10] = "-T";
  argv_paxos[11] = "100000";
  argv_paxos[12] = "-n";
  argv_paxos[13] = "32";
  argv_paxos[14] = "-P";
  argv_paxos[15] = "localhost";
  int ret = setup(16, argv_paxos);
  if (ret != 0) {
    return ret;
  }
  int cnt = 0;
  register_for_leader([&cnt](const char* log, int len) {
    cnt++;
  }, 0);
  register_for_follower([](const char* log, int len) {
  }, 0);

  for(int i = 1; i <= logs_to_commit; i++){
    char *x = (char *)std::to_string(i).c_str();
    submit(x , strlen(x), 0);
  }

  vector<std::thread> wait_threads;
  for(int i = 0; i < 1; i++){
    wait_threads.push_back(std::thread([i](){
      std::cout << "Starting wait for " << i << std::endl;
      wait_for_submit(i);
      std::cout << "Ending wait for " << i << std::endl;
    }));
  }


  for (auto& th : wait_threads) {
    th.join();
  }

  pre_shutdown_step();
  ret = shutdown_paxos();
  std::cout << "Submitted " << logs_to_commit << std::endl;
  std::cout << "Committed " << cnt << std::endl;
  return 0;
}