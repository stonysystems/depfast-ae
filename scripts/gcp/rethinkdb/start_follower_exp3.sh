#!/bin/bash

set -ex

s1="10.128.0.25"
s2="10.128.0.26"
s3="10.128.0.27"

echo "Running experiment 3."
gcloud compute instances start rethinkdb-1 rethinkdb-2 rethinkdb-3 --zone=us-central1-a

for i in {1..5}
do
    echo "Running follower experiment 3 - Trial $i"
	# 0. Cleanup first
	ssh -i ~/.ssh/id_rsa "$s1" "sh -c 'rm -rf /data1/*'"
	ssh -i ~/.ssh/id_rsa "$s2" "sh -c 'rm -rf /data1/*'"
	ssh -i ~/.ssh/id_rsa "$s3" "sh -c 'rm -rf /data1/*'"

	# 1. SSH to all the machines and start rethinkdb
	ssh  -i ~/.ssh/id_rsa "$s1" "sh -c 'nohup taskset -ac 0 rethinkdb --directory /data1/rethinkdb_data1 --bind all --server-name rethinkdb_first > /dev/null 2>&1 &'" 
	ssh  -i ~/.ssh/id_rsa "$s2" "sh -c 'nohup taskset -ac 0 rethinkdb --directory /data1/rethinkdb_data2 --join 10.128.0.25:29015 --bind all --server-name rethinkdb_second > /dev/null 2>&1 &'"
	ssh  -i ~/.ssh/id_rsa "$s3" "sh -c 'nohup taskset -ac 0 rethinkdb --directory /data1/rethinkdb_data3 --join 10.128.0.25:29015 --bind all --server-name rethinkdb_third > /dev/null 2>&1 &'"

	sleep 15

	# 2. Create tables
	source venv/bin/activate ;  python initr.py > tablesinfo ; deactivate

	# 3. Identify primary and follower
	primaryreplica=$(cat tablesinfo | grep -Eo 'primaryreplica=.{1,50}' | cut -d'=' -f2-)
	echo $primaryreplica

	secondaryreplica=$(cat tablesinfo | grep -Eo 'secondaryreplica=.{1,50}' | cut -d'=' -f2-)
	echo $secondaryreplica

	primarypid=$(cat tablesinfo | grep -Eo 'primarypid=.{1,10}' | cut -d'=' -f2-)
	echo $primarypid

	secondarypid=$(cat tablesinfo | grep -Eo 'secondarypid=.{1,10}' | cut -d'=' -f2-)
	echo $secondarypid

	primaryip=$(cat tablesinfo | grep -Eo 'primaryip=.{1,30}' | cut -d'=' -f2-)
	echo $primaryip

	secondaryip=$(cat tablesinfo | grep -Eo 'secondaryip=.{1,30}' | cut -d'=' -f2-)
	echo $secondaryip

	# 4. Run ycsb load
	./bin/ycsb load rethinkdb -s -P workloads/workloada_more -p rethinkdb.host=10.128.0.25 -p rethinkdb.port=28015

	# 5. Run experiment
	# 
	ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo mkdir /sys/fs/cgroup/blkio/cockroachdb'"
	ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'"
	lsblkcmd="8:16 524288"
	ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $lsblkcmd > /sys/fs/cgroup/blkio/cockroachdb/blkio.throttle.read_bps_device'"
	ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $lsblkcmd > /sys/fs/cgroup/blkio/cockroachdb/blkio.throttle.write_bps_device'"
	ssh -i ~/.ssh/id_rsa "$secondaryip" "sudo sh -c 'sudo echo $secondarypid > /sys/fs/cgroup/blkio/cockroachdb/cgroup.procs'"
	
	# 6. ycsb run
	./bin/ycsb run rethinkdb -s -P workloads/workloada_more -p maxexecutiontime=900 -p rethinkdb.host=$primaryip -p rethinkdb.port=28015 >> results/exp3_trial_$i.txt

	# 7. cleanup
	source venv/bin/activate ;  python cleanup.py > tablesinfo ; deactivate
	ssh -i ~/.ssh/id_rsa "$s1" "sh -c 'rm -rf /data1/*'"
	ssh -i ~/.ssh/id_rsa "$s2" "sh -c 'rm -rf /data1/*'"
	ssh -i ~/.ssh/id_rsa "$s3" "sh -c 'rm -rf /data1/*'"

	sleep 5

	# 8. Power off all the VMs
	#ssh -f -i ~/.ssh/id_rsa "$s1" "sudo sh -c 'sudo shutdown -h now'"
	gcloud compute instances stop rethinkdb-1 rethinkdb-2 rethinkdb-3 --zone=us-central1-a

	# 9. Start all the vms again
	gcloud compute instances start rethinkdb-1 rethinkdb-2 rethinkdb-3 --zone=us-central1-a

	# Sleep again for ssh to work
	sleep 1m
done
