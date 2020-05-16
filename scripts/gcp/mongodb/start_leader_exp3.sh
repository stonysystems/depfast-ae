#!/bin/bash

set -ex

s1="10.128.0.28"
s2="10.128.0.14"
s3="10.128.0.15"

s1name="andrew-server1"
s2name="andrew-server2"
s3name="andrew-server3"
serverZone="us-central1-a"
iterations=100

echo "Running leader experiment 3 for mongodb"
# Start the servers if they are already not started
gcloud compute instances start "$s1name" "$s2name" "$s3name" --zone="$serverZone"

mkdir -p leader_results

for i in {1..$iterations}
do
    echo "Running experiment 3 - Trial $i"
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

	sleep 15

	# 2. Init
	mongo --host "$s1name" < init_script.js

	# Wait for startup
	sleep 60

	mongo --host "$s1name" < fetchprimary.js  | tail -n +5 | head -n -1  > result.json
	primaryip=$(python parse.py | grep primary | cut -d" " -f2-)
	secondaryip=$(python parse.py | grep secondary | cut -d" " -f2-)

	primarypid=$(ssh -i ~/.ssh/id_rsa "$primaryip" "sh -c 'pgrep mongo'")
	echo $primarypid

	secondarypid=$(ssh -i ~/.ssh/id_rsa "$secondaryip" "sh -c 'pgrep mongo'")
	echo $secondarypid

	# 3. Run ycsb load
	./bin/ycsb load mongodb -s -P workloads/workloada_more -p mongodb.url=mongodb://$s1name:27017/ycsb?w=majority&readConcernLevel=majority

	# 4. Run experiment
	# Slow down leader
	./experiment3.sh "$primaryip" "$primarypid"

	# 5. ycsb run
	./bin/ycsb run mongodb -s -P workloads/workloada_more  -p maxexecutiontime=900 -p mongodb.url=mongodb://$primaryip:27017/ycsb?w=majority&readConcernLevel=majority > leader_results/exp3_trial_$i.txt

	# 6. cleanup
	mongo --host "$primaryip" < cleanup_script.js
	mongo --host "$primaryip" --eval "db.getCollectionNames().forEach(function(n){db[n].remove()});"
	ssh -i ~/.ssh/id_rsa "$s1" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; rm -rf /data/ ;  sudo cgdelete cpu:mongodb  ; pkill mongo ; true'"
	ssh -i ~/.ssh/id_rsa "$s2" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; rm -rf /data/ ;  sudo cgdelete cpu:mongodb ; pkill mongo ; true'"
	ssh -i ~/.ssh/id_rsa "$s3" "sudo sh -c 'rm -rf /data/db ; sudo umount /dev/sdb ; rm -rf /data/ ; sudo cgdelete cpu:mongodb ; pkill mongo ; true'"

	sleep 5
	# 8. Power off all the VMs
	gcloud compute instances stop "$s1name" "$s2name" "$s3name" --zone="$serverZone"

	# 9. Start all the vms again
	gcloud compute instances start "$s1name" "$s2name" "$s3name" --zone="$serverZone"

	# Sleep again for ssh to work
	sleep 1m
done
