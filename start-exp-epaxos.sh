#!/bin/bash

# 1: name
# 2: concurrent
# 3: duration
# 4: exp
# 5: replica
# 6: follower/leader
# 7: thread

cc=none
nc=$7

cd depfast

rm log/*
rm archive/*
rm tmp/*

if [[ $4 == "0" ]]; then
	exp=""
else
	exp="_exp$4"
fi

cp scripts/$6_slow/run_all$exp.py .
cp scripts/$6_slow/run$exp.py .

if [[ $5 == "3" ]]; then
	./run_all$exp.py -d $3 -hh config/hosts-local.yml -s '1:2:1' -c $nc:$((nc+1)):1 -r '3' -cc config/tpca.yml -cc config/client_closed.yml -cc config/none_copilot.yml -cc config/concurrent_$2.yml -b tpca -m $cc:copilot $1
else
	./run_all$exp.py -d $3 -hh config/hosts-nonlocal-5.yml -s '1:2:1' -c '1:2:1' -r '5' -cc config/tpca.yml -cc config/client_closed.yml -cc config/tpl_ww_raft.yml -cc config/concurrent_$2.yml -b tpca -m $cc:fpga_raft $1
fi

rm run_all$exp.py run$exp.py

cd ../
echo $(pwd)
tar xzf depfast/archive/$1-tpca_${cc}-copilot_${nc}_1_-1.tgz
log=log/$1-tpca_$cc-copilot_${nc}_1_-1.log
line1=`grep -n "all_latency" $log | cut -f1 -d: | head -1`
# echo $line1
line2=$((line1+1))
tail99=`sed "${line1}q;d" $log | awk '{print $7}' | cut -f1 -d,`
line999=`sed "${line1}q;d" $log | awk '{print $8}' | cut -f1 -d,`
if [[ $line999 == "'99.9':" ]]; then
        tail999=`sed "${line1}q;d" $log | awk '{print $9}' | cut -f1 -d,`
        avg=`sed "${line2}q;d" $log | awk '{print $2}' | cut -f1 -d,`
else
        tail999=`sed "${line2}q;d" $log | awk '{print $2}' | cut -f1 -d,`
        avg=`sed "${line2}q;d" $log | awk '{print $4}' | cut -f1 -d,`
fi

tput=`grep "tps:" $log | awk '{print $2}'`
echo "$1, $tput, $avg, $tail99, $tail999" >> result$4.csv
