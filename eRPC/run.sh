make clean
#cmake . -DTRANSPORT=dpdk -DAZURE=off -DPERF=ON -DLOG_LEVEL=none
cmake . -DTRANSPORT=infiniband -DROCE=on
make -j10
