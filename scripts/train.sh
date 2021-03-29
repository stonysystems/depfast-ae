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
	trial="$1"_"$i"
	./start-exp.sh $trial $i 150 0 3 follower
	tar xvzf janus-gray/archive/$trial-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py $1
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
	trial="$1"_"$i"
	./start-exp.sh $trial $i 150 0 3 follower
	tar xvzf janus-gray/archive/$trial-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py $1
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
	trial="$1"_"$i"
	./start-exp.sh $trial $i 150 0 3 follower
	tar xvzf janus-gray/archive/$trial-tpca_2pl_ww-fpga_raft_1_1_-1.tgz
	cd janus-gray
	python3 avg_latency.py $1
	cd ../
done
