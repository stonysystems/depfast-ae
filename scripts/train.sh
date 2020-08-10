#!/bin/bash

for i in {1..10..1}; do
  if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
    echo "concurrent_$i.yml exists"
  else
    echo -e "\nn_concurrent: $i\n" > janus-gray/config/concurrent_$i.yml
    if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
      echo "file successfully created"
    fi
  fi
	#./start-exp0.sh test_5_$i $i 150
	tar xvzf janus-gray/archive/test_5_$i-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py
	cd ../
done

for i in {20..100..10}; do
  if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
    echo "concurrent_$i.yml exists"
  else
    echo -e "\nn_concurrent: $i\n" > janus-gray/config/concurrent_$i.yml
    if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
      echo "file successfully created"
    fi
  fi
	#./start-exp0.sh test_5_$i $i 150
	tar xvzf janus-gray/archive/test_5_$i-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py
	cd ../
done

for i in {200..2000..100}; do
  if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
    echo "concurrent_$i.yml exists"
  else
    echo -e "\nn_concurrent: $i\n" > janus-gray/config/concurrent_$i.yml
    if [ -f "janus-gray/config/concurrent_$i.yml" ]; then
      echo "file successfully created"
    fi
  fi
	#./start-exp0.sh test_5_$i $i 150
	tar xvzf janus-gray/archive/test_5_$i-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py
	cd ../
done
