#!/bin/bash
set -ex

threads=(1)
concurrent=(2 4 6 8 10 12 14 16 18 20 25 30 40)
iter=3
exp=(0)
svr=5
time=60
role=follower


for c in ${concurrent[@]}
do
	for t in ${threads[@]}
	do
		for e in ${exp[@]}
		do
			for (( i=1; i<=$iter; i++ ))
				do
				name=copilot_"$role"_exp"$e"_t"$t"_c"$c"_s"$svr"_trail$i
				echo "Running experiment $name"
				./start-exp-epaxos.sh $name $c $time $e $svr $role $t
			done
		done
	done
done
