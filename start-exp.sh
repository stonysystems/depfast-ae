#!/bin/bash

# 1: name
# 2: concurrent
# 3: duration
# 4: exp
# 5: replica
# 6: follower/leader
# 7: thread
# 8: protocol
# 9: environment

name=$1
dur=$2
e=$3
rep=$4
typ=$5
nc=$6
conc=$7
ab=$8
env=$9
cc=none
workload=tpca

rm log/*
rm archive/*
rm tmp/*

if [[ $e == "0" ]]; then
	exp=""
else
	exp="_exp$e"
fi

cp scripts/${typ}_slow/run_all.py .
cp scripts/${typ}_slow/run$exp.py .
cp inf ~

if [[ $rep == "3" ]]; then
	./run_all.py -e ./run$exp.py -d $dur -hh config/hosts-$env.yml -s '1:2:1' -c $nc:$((nc+1)):1 -r '3' -cc config/${workload}.yml -cc config/client_closed.yml -cc config/${cc}_${ab}.yml -cc config/concurrent_$conc.yml -b ${workload} -m $cc:$ab $name
else
	./run_all.py -e ./run$exp.py -d $dur -hh config/hosts-$env-5.yml -s '1:2:1' -c $nc:$((nc+1)):1 -r '5' -cc config/${workload}.yml -cc config/client_closed.yml -cc config/${cc}_${ab}.yml -cc config/concurrent_$conc.yml -b ${workload} -m $cc:$ab $name
fi

rm run_all.py run$exp.py

echo $(pwd)
tar xzf archive/${name}-${workload}_${cc}-${ab}_${nc}_1_-1.tgz
log=log/${name}-${workload}_${cc}-${ab}_${nc}_1_-1.log
yml=log/${name}-${workload}_${cc}-${ab}_${nc}_1_-1.yml

tput=`yq e '.WRITE.tps' $yml`
avg=`yq e '.WRITE.all_latency["avg"]' $yml`
med=`yq e '.WRITE.all_latency[50]' $yml`
tail99=`yq e '.WRITE.all_latency[99]' $yml`
echo "$name, $tput, $avg, $med, $tail99" >> result$e_$rep.csv

mkdir -p results
cp $yml results/
