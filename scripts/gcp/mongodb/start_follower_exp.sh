#!/bin/bash

set -ex

if [ "$#" -ne 4 ]; then
    echo "Illegal number of parameters"
    echo "1st arg - number of iterations"
    echo "2nd arg - workload path"
    echo "3rd arg - seconds to run ycsb run"
    echo "4th arg - experiment to run"
    exit 1
fi

iterations=$1
workload=$2
ycsbruntime=$3
expno=$4

s1="10.128.0.28"
s2="10.128.0.14"
s3="10.128.0.15"

s1name="andrew-server1"
s2name="andrew-server2"
s3name="andrew-server3"
serverZone="us-central1-a"

echo "Running follower experiment $expno for mongodb"
# Start the servers if they are already not started
gcloud compute instances start "$s1name" "$s2name" "$s3name" --zone="$serverZone"

sleep 30

mkdir -p results

for (( i=1; i<=$iterations; i++ ))
do
	echo "Running experiment $expno - Trial $i"
	# 0. Cleanup first
	ssh -i ~/.ssh/id_rsa "$s1" "sh -c 'rm -rf /srv/mongodb/rs0-*/data ; rm -rf /srv/mongodb/rs0-*'"
	ssh -i ~/.ssh/id_rsa "$s2" "sh -c 'rm -rf /srv/mongodb/rs0-*/data ; rm -rf /srv/mongodb/rs0-*'"
	ssh -i ~/.ssh/id_rsa "$s3" "sh -c 'rm -rf /srv/mongodb/rs0-*/data ; rm -rf /srv/mongodb/rs0-*'"

	
	# 2. Create data directories
	ssh -i ~/.ssh/id_rsa "$s1" "sudo sh -c 'sudo mkdir -p /data ; sudo mkfs.xfs /dev/sdb -f ; sudo mount -t xfs /dev/sdb /data ; sudo mount -t xfs /dev/sdb /data -o remount,noatime ; sudo mkdir /data/db ; sudo chmod o+w /data/db'"
	ssh -i ~/.ssh/id_rsa "$s2" "sudo sh -c 'sudo mkdir -p /data ; sudo mkfs.xfs /dev/sdb -f ; sudo mount -t xfs /dev/sdb /data ; sudo mount -t xfs /dev/sdb /data -o remount,noatime ; sudo mkdir /data/db ; sudo chmod o+w /data/db'"
	ssh -i ~/.ssh/id_rsa "$s3" "sudo sh -c 'sudo mkdir -p /data ; sudo mkfs.xfs /dev/sdb -f ; sudo mount -t xfs /dev/sdb /data ; sudo mount -t xfs /dev/sdb /data -o remount,noatime ; sudo mkdir /data/db ; sudo chmod o+w /data/db'"

	# 1. SSH to all the machines and start db
	ssh  -i ~/.ssh/id_rsa "$s1" "sh -c 'numactl --interleave=all taskset -ac 0 mongod --replSet rs0 --bind_ip localhost,"$s1name" --fork --logpath /tmp/mongod.log'"
	ssh  -i ~/.ssh/id_rsa "$s2" "sh -c 'numactl --interleave=all taskset -ac 0 mongod --replSet rs0 --bind_ip localhost,"$s2name" --fork --logpath /tmp/mongod.log'"
	ssh  -i ~/.ssh/id_rsa "$s3" "sh -c 'numactl --interleave=all taskset -ac 0 mongod --replSet rs0 --bind_ip localhost,"$s3name" --fork --logpath /tmp/mongod.log'" 

	sleep 30

	# 2. Init
	mongo --host "$s1name" < init_script.js

	# Wait for startup
	sleep 60

	mongo --host "$s1name" < fetchprimary.js  | tail -n +5 | head -n -1  > result.json
	cat result.json

	primaryip=$(python parse.py | grep primary | cut -d" " -f2-)
	secondaryip=$(python parse.py | grep secondary | cut -d" " -f2-)

	primarypid=$(ssh -i ~/.ssh/id_rsa "$primaryip" "sh -c 'pgrep mongo'")
	echo $primarypid

	secondarypid=$(ssh -i ~/.ssh/id_rsa "$secondaryip" "sh -c 'pgrep mongo'")
	echo $secondarypid

	# Disable chaining allowed
	mongo --host $primaryip --eval "cfg = rs.config(); cfg.settings.chainingAllowed = false; rs.reconfig(cfg);"

	# 3. Run ycsb load
	./bin/ycsb load mongodb -s -P $workload -p mongodb.url=mongodb://$primaryip:27017/ycsb?w=majority&readConcernLevel=majority ; wait $!

	# 4. Run experiment
	# Slow down follower
	./experiment$expno.sh "$secondaryip" "$secondarypid"

	# 5. ycsb run
	./bin/ycsb run mongodb -s -P $workload  -p maxexecutiontime=$ycsbruntime -p mongodb.url="mongodb://$primaryip:27017/ycsb?w=majority&readConcernLevel=majority" > results/exp"$expno"_trial_"$i".txt ; wait $!

	# 6. cleanup
	mongo --host "$primaryip" < cleanup_script.js
	mongo --host "$primaryip" --eval "db.getCollectionNames().forEach(function(n){db[n].remove()});"
	# Cleanup secondary
	#ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo pkill -f 'dd if=/dev/zero' ; sudo cgdelete cpu:db cpu:cpulow cpu:cpuhigh blkio:db'" ; true
	ssh -i ~/.ssh/id_rsa "$s1" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; sudo rm -rf /data/ ; sudo cgdelete cpu:db cpu:cpulow cpu:cpuhigh blkio:db ; pkill mongod ; true'"
	ssh -i ~/.ssh/id_rsa "$s2" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; sudo rm -rf /data/ ; sudo cgdelete cpu:db cpu:cpulow cpu:cpuhigh blkio:db ; pkill mongod ; true'"
	ssh -i ~/.ssh/id_rsa "$s3" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; sudo rm -rf /data/ ; sudo cgdelete cpu:db cpu:cpulow cpu:cpuhigh blkio:db ; pkill mongod ; true'"
	rm result.json

	sleep 5
	
	# 8. Power off all the VMs
	#ssh -f -i ~/.ssh/id_rsa "$s1" "sudo sh -c 'sudo shutdown -h now'"
	gcloud compute instances stop "$s1name" "$s2name" "$s3name" --zone="$serverZone"

	# 9. Start all the vms again
	gcloud compute instances start "$s1name" "$s2name" "$s3name" --zone="$serverZone"

	# Sleep again for ssh to work
	sleep 1m
done

# sudo shutdown -h now
gcloud compute instances stop "$s1name" "$s2name" "$s3name" --zone="$serverZone"
