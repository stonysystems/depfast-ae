#!/bin/bash

threads=(1)
concurrent=(1600)
iter=3

for c in ${concurrent[@]}
do
	for t in ${threads[@]}
	do
		for (( i=1; i<=$iter; i++ ))
		do
			name=copilot_noslow_t"$t"_c"$c"_trail$i
			echo "Running experiment $name"
			./start-exp-epaxos.sh $name $c 60 0 3 follower $t
		done
	done
done	

