#!/bin/bash

# 1: name
# 2: concurrent
# 3: duration
# 4: exp
# 5: replica
# 6: follower/leader
# 7: thread
# 8: protocol

cc=none
ab=$8
nc=$7
workload=rw

# rm /db/data.txt
# sudo touch /db/data.txt
# sudo chmod o+w /db/data.txt


# rm log/*
rm archive/*
rm tmp/*

if [[ $4 == "0" ]]; then
	exp=""
else
	exp="_exp$4"
fi

cp scripts/$6_slow/run_all.py .
cp scripts/$6_slow/run$exp.py .

if [[ $5 == "3" ]]; then
	./run_all.py -e ./run$exp.py -d $3 -hh config/hosts-local.yml -s '1:2:1' -c $nc:$((nc+1)):1 -r '3' -cc config/${workload}.yml -cc config/client_closed.yml -cc config/${cc}_${ab}.yml -cc config/concurrent_$2.yml -b ${workload} -m $cc:$ab $1
else
	./run_all.py -e ./run$exp.py -d $3 -hh config/hosts-nonlocal-5.yml -s '1:2:1' -c $nc:$((nc+1)):1 -r '5' -cc config/${workload}.yml -cc config/client_closed.yml -cc config/${cc}_${ab}.yml -cc config/concurrent_$2.yml -b ${workload} -m $cc:$ab $1
fi

rm run_all.py run$exp.py

echo $(pwd)
tar xzf archive/$1-${workload}_${cc}-${ab}_${nc}_1_-1.tgz
log=log/$1-${workload}_$cc-${ab}_${nc}_1_-1.log
yml=log/$1-${workload}_$cc-${ab}_${nc}_1_-1.yml
# line1=`grep -n "all_latency" $log | cut -f1 -d: | head -1`
# # echo $line1
# line2=$((line1+1))
# med=`sed "${line1}q;d" $log | awk '{print $3}' | cut -f1 -d,`
# tail99=`sed "${line1}q;d" $log | awk '{print $7}' | cut -f1 -d,`
# line999=`sed "${line1}q;d" $log | awk '{print $8}' | cut -f1 -d,`
# if [[ $line999 == "'99.9':" ]]; then
#         tail999=`sed "${line1}q;d" $log | awk '{print $9}' | cut -f1 -d,`
#         avg=`sed "${line2}q;d" $log | awk '{print $2}' | cut -f1 -d,`
# else
#         tail999=`sed "${line2}q;d" $log | awk '{print $2}' | cut -f1 -d,`
#         avg=`sed "${line2}q;d" $log | awk '{print $4}' | cut -f1 -d,`
# fi

# tput=`grep "tps:" $log | awk '{print $2}'`
tput=`yq e '.WRITE.tps' $yml`
avg=`yq e '.WRITE.all_latency["avg"]' $yml`
med=`yq e '.WRITE.all_latency[50]' $yml`
tail99=`yq e '.WRITE.all_latency[99]' $yml`
echo "$1, $tput, $avg, $med, $tail99" >> result$4_$5.csv
